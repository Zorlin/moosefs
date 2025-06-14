Below is a complete, self‑contained design for adding true high‑availability (HA) and optional metadata sharding to MooseFS Community Edition (MFS‑CE). It keeps MooseFS’ existing chunkserver layer untouched and focuses exclusively on the metadata tier.
1  Design goals & non‑goals
ID	Goal	Rationale
G1	Zero‑RPO / sub‑second RTO for metadata loss	Competitive with enterprise filesystems
G2	Scale metadata IOPS horizontally by sharding	Avoid the current “single master” bottleneck
G3	Survive WAN partitions & DC loss while staying available for both reads and writes	Target multi‑region deployments
G4	Keep operational footprint small (< 5 binaries) and codepath obvious	Must be community‑maintainable
NG1	No change to chunk placement or erasure‑coding logic	Out‑of‑scope
NG2	No POSIX‑breaking semantics	Applications cannot observe divergence
2  High‑level architecture

 +-------------------------+      +-------------------------+
 |  MDS‑A  (shard 0 leader)|<---->|  MDS‑B  (shard 0 follower / shard 1 leader)
 +-----------^-------------+      +-----------^-------------+
             |                                |
             |Circular delta stream           |Circular delta stream
             v                                v
 +-------------------------+      +-------------------------+
 |  MDS‑C  (shard 1 follower)|<-->|  MDS‑D  (shard 0 follower / shard 1 follower)
 +-------------------------+      +-------------------------+

    MDS‑X = Metadata server instance

    Each shard is an independent Raft group (“Raft Leader Lock”) that elects one leader.

    All MDSs form a logical ring. Every node forward‑streams deltas to its successor; the ring may be widened (k‑successors) for faster fan‑out in large clusters.

    CRDTs guarantee that when asynchronously replayed on any node the state converges, so any node may accept a write even if it is not the current Raft leader.

    A light‑weight membership gossip service maintains the ring and Raft‑group membership.

3  Data model – CRDT formulation
Filesystem concept	CRDT used	Merge rule
Inode attributes (mode, uid, gid, timestamps)	Last‑Writer‑Wins register keyed by (inode_id, attr_name) with Lamport timestamp ⟨wall clock, node ID⟩	Higher timestamp overrides
Link count	Grow‑Only counter (G‑Counter)	Element‑wise addition
Directory entries	Observed‑Remove Set (OR‑Set) of ⟨file name, inode_id⟩	OR‑Set merge semantics; rename = remove+add
Extended attributes	LWW registers like inode attributes	—
Quota usage	PN‑Counter	Positive inc / negative dec

All CRDTs are serialized as Raft log entries and as ring deltas.
4  Sharding strategy

    Shard key: shard_id = blake3(inode_id) % NUM_SHARDS

    Shard table lives in a tiny sidecar DB (badger / LMDB) on every MDS.

    Raft group per shard:

        N odd replica nodes (3 or 5 recommended).

        Leader writes are linearizable within that shard.

    Pathnames map to shards through the parent directory’s inode_id, so a lookup touches a single shard except during rename(2) (source + target shard).

5  Raft “Leader Lock” pattern

    A Leader Lock is simply “whoever is Raft leader for a shard holds the lock”; TTL == Raft election timeout.

    Write handling:

client -> nearest MDS
if local node == shard.leader:
    apply write locally, commit to Raft, then stream delta to ring
else:
    fast‑forward: issue CRDT op locally, stamp with Lamport ts, add to “async queue”
    continue serving client (optimistic completion)
    later: queue flushed to real leader via Raft AppendEntries

    Because every CRDT operation is commutative, a temporary dual‑writer scenario during partitions cannot corrupt state; Raft will ultimately serialize entries in one global order, but even if conflicting values land in different positions the CRDT merge converges.

6  Circular replication graph
6.1  Topology maintenance

on GossipTick():
    broadcast ⟨node_id, epoch, successors[1..k]⟩
    update membership table
    if ring break detected:
        re‑compute successors via consistent hash of node_id

    k (fan‑out width) defaults to ⌈log₂(N)⌉.

    Successor list serves as daisy‑chain for deltas; each delta is forwarded to all successors except the sender.

6.2  Delta propagation loop

on RaftCommit(entry):
    put entry into local CRDT store
    for succ in successors:
        send DeltaMsg⟨entry⟩ to succ

on DeltaMsg(entry) from pred:
    if entry.id already applied: drop
    else:
        apply to CRDT store
        forward to all successors except pred

    Guarantees O(N · k) messages per delta; no duplicate deliveries thanks to entry UUID hashes.

7  Conflict handling

The only conflict class that cannot be sanely expressed as a pure CRDT is rename across shards. Solution:

    Encapsulate rename(src_inode, dst_parent, new_name) as a composite CRDT update with two-phase intent:

        Phase 1 “prepare”: reserve an ephemeral rename UUID written to both shards.

        Phase 2 “commit” (idempotent): apply OR‑Set remove+add plus PN‑counter updates.

    If either shard leader crashes after prepare but before commit, the other side’s background sweeper detects an orphaned prepare record (age > T) and rolls it back.

8  Client read & write paths
8.1  Fast read

    Client hashes path, discovers shard_id.

    Asks any MDS for CRDT state.

    MDS serves directly from its local CRDT store (no leader needed). Reads are monotonic‑but‑stale; staleness ≤ round‑trip of ring.

8.2  Fast write (optimistic)

As shown in § 5: local speculative apply → ack → eventual serialization via Raft.
8.3  Strict‑serial read (optional)

Mount option strict_meta=1: client will contact current leader (supplied by shard gossip) and wait for confirmation that its vector clock ≤ leader clock.
9  Failure scenarios
Scenario	Outcome	Mechanism
Single MDS crash	No data loss; leader election triggers if it was leader	Raft
Network partition splits ring	Each partition continues serving reads/writes independently	CRDT convergence upon heal
Partition isolates Raft majority from minority	Minority continues in speculative mode only; once connection restores its un‑replicated ops are appended to Raft log (still safe)	Leader lock semantics
Data‑centre loss	Survived so long as any shard retains a majority elsewhere	Multi‑region Raft placement policy
10  Metadata snapshotting & log compaction

    Every MDS periodically (default 1 h or 1 GiB log growth) checkpoints its CRDT store as a flat protobuf snapshot.

    Snapshot entry is inserted into Raft log and ring.

    Nodes lagging by > SNAP_DISTANCE bytes pull latest snapshot via gRPC bulk sync instead of replaying the full log.

11  Shard migration & rebalancing

    Calculate new NUM_SHARDS’ via target_inode_per_shard.

    Freeze window; leaders cut snapshots.

    Deterministically map old‑shard → new‑shard set.

    Spawn new Raft groups, stream snapshots, then unfreeze.

(This is essentially a metadata‑only version of HDFS’ Federation rebalancer.)
12  Security & authn/z

    Re‑use MooseFS’ existing TLS mutual auth between master <‑> chunkserver.

    Add per‑shard Raft group secret rotated on leader change; ring deltas are AES‑GCM encrypted with cluster‑wide key.

13  Operational workflow
Task	Command‑line outline
Bootstrap 3‑node RAFT cluster	mfsmds --init --shards=2 --peers="A,B,C"
Add new MDS	mfsmds join --seed=A (gossip handles ring)
Force leader fail‑over	mfsmds raft transfer-leadership --shard=7 --to=B
Observe convergence lag	mfsmds ring stats --histogram
Trigger shard rebalance	mfsmds shard rebalance --target-shards=8
14  Performance budget (rule‑of‑thumb)
Metadata op	50th % latency	99th % latency	Notes
Local write, same DC	1–3 ms	10 ms	Raft roundtrip
Remote write, other DC	8–25 ms	60 ms	WAN RTT dominated
Read (any node)	0.3 ms	2 ms	Memory‑cached CRDT
15  Implementation roadmap

    Phase 0 – carve existing monolithic mfsmetalogger into reusable library.

    Phase 1 – embed etcd‑raft or Hashicorp Raft; single‑shard HA without CRDTs (linearizable).

    Phase 2 – add CRDT abstraction & ring replication; enable optimistic local writes.

    Phase 3 – implement shard table, bootstrap, rebalance logic.

    Phase 4 – documentation, operator playbooks, CI chaos‑test suite (Jepsen‑like).

Take‑aways

    Raft Leader Locks give fast, strongly consistent leadership where you need it (per‑shard ordering) while CRDTs absorb every other consistency headache, allowing always‑on multi‑writer behavior even across partitions.

    The circular replication ring is trivial to reason about, easy to debug, and cheap—yet with k‑successor widening it achieves logarithmic spread like gossip.

    The design slots cleanly into MooseFS’ existing philosophy: lightweight, performant, and friendly to community contributors.
