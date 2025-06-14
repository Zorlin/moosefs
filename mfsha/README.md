# MooseFS High Availability (mfsha) - Deployment Guide

## Overview

MooseFS HA (`mfsha`) provides zero-RPO, sub-second RTO metadata high availability through a CRDT-based multimaster architecture with Raft consensus and circular replication. This system maintains full backward compatibility with existing MooseFS deployments while adding enterprise-grade HA capabilities.

## Architecture Summary

- **CRDT Multimaster**: True asynchronous conflict-free replicated data types
- **Raft Leader Locks**: Per-shard ordering with leader election
- **Circular Replication**: k-successor topology for delta propagation
- **Cache-Aware Sharding**: HOT/WARM/COLD metadata tiers
- **Zero-RPO**: No metadata loss on node failures
- **Sub-second RTO**: Automatic failover in <1 second

## Prerequisites

- MooseFS 4.57.6+ with HA extension compiled
- 3+ nodes for production (5+ recommended for multi-region)
- Network connectivity between all HA nodes
- Synchronized time (NTP recommended)
- Sufficient RAM for metadata cache (8GB+ recommended per node)

## Quick Start

### 1. Basic 3-Node HA Cluster

**Node Configuration** (`/usr/local/etc/mfsha.cfg`):

```ini
# Node Identity
MFSHA_NODE_ID = 1                    # Unique node ID (1, 2, 3...)
MFSHA_PORT = 9430                    # HA communication port

# Cluster Configuration
MFSHA_PEERS = "192.168.1.10:9430,192.168.1.11:9430,192.168.1.12:9430"
HA_SHARD_COUNT = 8                   # Number of metadata shards
HA_CRDT_MAX_MEMORY = 8589934592      # 8GB cache per node

# Raft Consensus
HA_RAFT_ELECTION_TIMEOUT = 5000      # 5 second election timeout
HA_RAFT_HEARTBEAT_TIMEOUT = 1000     # 1 second heartbeat

# Synchronization
HA_METADATA_SYNC_INTERVAL = 30.0     # Sync to disk every 30s
HA_GOSSIP_INTERVAL = 5.0             # Gossip every 5s

# Storage
DATA_PATH = /usr/local/var/mfs       # Metadata storage path
```

**Start Commands**:

```bash
# Node 1 (192.168.1.10)
export MFSHA_NODE_ID=1
/usr/local/sbin/mfsha

# Node 2 (192.168.1.11) 
export MFSHA_NODE_ID=2
/usr/local/sbin/mfsha

# Node 3 (192.168.1.12)
export MFSHA_NODE_ID=3
/usr/local/sbin/mfsha
```

### 2. Integration with Existing MooseFS

**Traditional Setup**:
```bash
# Old way - single master
mfsmaster start
```

**HA Setup**:
```bash
# New way - True active-active HA cluster
mfsha start                    # Start HA metadata cluster (active-active)
mfsmaster start --ha-proxy     # Optional: Start legacy compatibility proxy
```

The HA-integrated setup maintains full compatibility:
- All `mfsha` nodes are active and can serve requests
- `mfsclient` and `mfsmount` work unchanged
- `metadata.mfs` format preserved
- All existing tools (`mfstools`, `mfscli`) continue to work
- No single points of failure - true active-active architecture

## Production Deployment

### Multi-Region Configuration

**Region 1** (Primary datacenter):
```ini
# Node 1A (Primary region leader candidate)
MFSHA_NODE_ID = 1
MFSHA_REGION = "us-east-1"
MFSHA_PEERS = "10.1.1.10:9430,10.1.1.11:9430,10.2.1.10:9430,10.2.1.11:9430,10.3.1.10:9430"
HA_RAFT_PRIORITY = 100               # Higher priority for leadership
```

**Region 2** (Secondary datacenter):
```ini
# Node 2A (Secondary region)
MFSHA_NODE_ID = 3
MFSHA_REGION = "us-west-1"
MFSHA_PEERS = "10.1.1.10:9430,10.1.1.11:9430,10.2.1.10:9430,10.2.1.11:9430,10.3.1.10:9430"
HA_RAFT_PRIORITY = 50                # Lower priority
```

**Region 3** (Disaster recovery):
```ini
# Node 3A (DR region)
MFSHA_NODE_ID = 5
MFSHA_REGION = "eu-west-1"
MFSHA_PEERS = "10.1.1.10:9430,10.1.1.11:9430,10.2.1.10:9430,10.2.1.11:9430,10.3.1.10:9430"
HA_RAFT_PRIORITY = 25                # Lowest priority
```

### Performance Tuning

**High-Performance Configuration**:
```ini
# Memory Configuration
HA_CRDT_MAX_MEMORY = 34359738368     # 32GB for large deployments
HA_SHARD_COUNT = 64                  # More shards for better parallelism

# Network Tuning
MFSHA_PORT = 9430
HA_GOSSIP_INTERVAL = 2.0             # Faster failure detection
HA_RAFT_HEARTBEAT_TIMEOUT = 500      # 500ms for faster failover

# I/O Optimization  
HA_METADATA_SYNC_INTERVAL = 60.0     # Less frequent disk sync
HA_ASYNC_REPLICATION = true          # Enable async delta streaming
```

### Security Configuration

**Network Security**:
```ini
# Bind to specific interfaces
BIND_HOST = "10.1.1.10"              # Internal network only
MFSHA_TLS_ENABLE = true               # Enable TLS for inter-node communication
MFSHA_TLS_CERT_PATH = "/etc/mfs/certs/node.crt"
MFSHA_TLS_KEY_PATH = "/etc/mfs/certs/node.key"
MFSHA_TLS_CA_PATH = "/etc/mfs/certs/ca.crt"

# Authentication
MFSHA_AUTH_TOKEN = "your-cluster-auth-token"
```

## Operational Procedures

### Cluster Initialization

**First-time cluster setup**:
```bash
# 1. Initialize first node with empty metadata
mfsha --init-cluster --node-id=1

# 2. Start additional nodes (they'll sync from node 1)
mfsha --join-cluster --node-id=2 --peer=192.168.1.10:9430
mfsha --join-cluster --node-id=3 --peer=192.168.1.10:9430

# 3. Verify cluster health
mfsha-admin status
```

### Adding New Nodes

**Scale cluster from 3 to 5 nodes**:
```bash
# 1. Configure new node
echo "MFSHA_NODE_ID = 4" >> /usr/local/etc/mfsha.cfg
echo "MFSHA_PEERS = \"existing-peers,new-node:9430\"" >> /usr/local/etc/mfsha.cfg

# 2. Start new node
mfsha --join-cluster --node-id=4 --peer=192.168.1.10:9430

# 3. Update cluster configuration on all nodes
mfsha-admin rebalance --new-nodes=4,5

# 4. Verify rebalancing
mfsha-admin shard-status
```

### Failure Scenarios

**Node Failure Recovery**:
```bash
# Automatic failover (no action needed for <2 node failures)
# Monitor logs:
tail -f /var/log/mfsha.log

# Manual intervention only if >50% nodes fail:
mfsha-admin force-leader-election --node-id=1
```

**Network Partition Recovery**:
```bash
# Check cluster status after partition heals
mfsha-admin partition-status

# Force reconciliation if needed
mfsha-admin reconcile --force

# Verify metadata consistency
mfsha-admin consistency-check
```

### Backup and Disaster Recovery

**Continuous Backup**:
```bash
# Automated backup (runs on each node)
mfsha-backup --output=/backup/metadata-$(date +%Y%m%d-%H%M%S).mfs

# Backup includes:
# - Full metadata.mfs snapshot
# - CRDT vector clocks
# - Raft log checkpoints
# - Configuration state
```

**Disaster Recovery**:
```bash
# 1. Restore from backup on new cluster
mfsha --restore-cluster --backup=/backup/metadata-20250614-120000.mfs

# 2. Initialize new cluster with restored data
mfsha --init-cluster --from-backup --node-id=1

# 3. Add additional nodes
mfsha --join-cluster --node-id=2 --peer=new-node-1:9430
mfsha --join-cluster --node-id=3 --peer=new-node-1:9430

# 4. Verify restoration
mfsha-admin verify-restore
```

## Monitoring and Maintenance

### Health Checks

**Cluster Status**:
```bash
# Overall cluster health
mfsha-admin status
# Output:
# Cluster Status: HEALTHY
# Nodes: 5/5 online
# Shards: 64/64 available  
# Leaders: 64/64 elected
# Replication: 100% synchronized

# Per-node status
mfsha-admin node-status --node-id=1
# Output:
# Node 1: LEADER (shards: 12-15, 24-27, 36-39, 48-51)
# Memory: 12.5GB / 32GB (39%)
# Network: 1.2MB/s in, 0.8MB/s out
# CRDT Clock: (1735942234, 1, 12847)
```

**Performance Metrics**:
```bash
# CRDT operation statistics
mfsha-admin crdt-stats
# Output:
# Operations/sec: 1,247 (reads: 891, writes: 356)
# Conflicts resolved: 23 (LWW: 23, counter: 0, set: 0)
# Cache hit rate: 94.2%
# Memory efficiency: 87.3%

# Raft consensus metrics  
mfsha-admin raft-stats
# Output:
# Elections: 3 total, 0 failed
# Log entries: 234,567 committed, 0 pending
# Heartbeat RTT: 2.3ms avg
# Leadership changes: 0 in last 24h
```

### Configuration Management

**Dynamic Reconfiguration**:
```bash
# Update cache size without restart
mfsha-admin config set HA_CRDT_MAX_MEMORY 68719476736  # 64GB

# Update sync interval
mfsha-admin config set HA_METADATA_SYNC_INTERVAL 45.0

# Reload configuration
mfsha-admin reload-config
```

**Shard Rebalancing**:
```bash
# Manual rebalancing (usually automatic)
mfsha-admin rebalance --strategy=balanced

# Migrate specific shard
mfsha-admin migrate-shard --shard-id=15 --from-node=2 --to-node=4

# Monitor migration progress
mfsha-admin migration-status
```

## Troubleshooting

### Common Issues

**Split-Brain Prevention**:
```bash
# If cluster splits, check quorum
mfsha-admin quorum-status
# Output: 3/5 nodes reachable (QUORUM AVAILABLE)

# Force quorum if needed (DANGER: only if you're sure)
mfsha-admin force-quorum --nodes=1,2,3
```

**Memory Issues**:
```bash
# Check memory usage
mfsha-admin memory-stats
# Output:
# Total: 32GB, Used: 28.1GB (87.8%)
# Hot tier: 24.2GB (12,847 entries)
# Warm tier: 3.9GB (8,234 entries) 
# Warning: Memory usage high, consider increasing HA_CRDT_MAX_MEMORY

# Force cache cleanup
mfsha-admin cache-cleanup --aggressive
```

**Network Connectivity**:
```bash
# Test inter-node connectivity
mfsha-admin ping-nodes
# Output:
# Node 1: OK (2.3ms)
# Node 2: OK (3.1ms)  
# Node 3: TIMEOUT
# Node 4: OK (1.8ms)
# Node 5: OK (4.2ms)

# Check port availability
mfsha-admin port-check --port=9430
```

### Log Analysis

**Important Log Files**:
```bash
# Main HA log
tail -f /var/log/mfsha.log

# CRDT operations
tail -f /var/log/mfsha-crdt.log

# Raft consensus
tail -f /var/log/mfsha-raft.log

# Network communications
tail -f /var/log/mfsha-network.log
```

**Key Log Patterns**:
```bash
# Successful operations
grep "CRDT delta applied" /var/log/mfsha.log
grep "Raft leader elected" /var/log/mfsha.log  
grep "Node joined cluster" /var/log/mfsha.log

# Error conditions
grep "ERROR\|CRITICAL" /var/log/mfsha.log
grep "split.brain\|partition" /var/log/mfsha.log
grep "memory.exhausted\|cache.full" /var/log/mfsha.log
```

## Integration Examples

### Kubernetes Deployment

**StatefulSet Configuration**:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mfsha-cluster
spec:
  serviceName: mfsha
  replicas: 5
  template:
    spec:
      containers:
      - name: mfsha
        image: moosefs/mfsha:4.57.6
        env:
        - name: MFSHA_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['mfsha.node.id']
        - name: MFSHA_PEERS
          value: "mfsha-0.mfsha:9430,mfsha-1.mfsha:9430,mfsha-2.mfsha:9430,mfsha-3.mfsha:9430,mfsha-4.mfsha:9430"
        volumeMounts:
        - name: metadata-storage
          mountPath: /usr/local/var/mfs
  volumeClaimTemplates:
  - metadata:
      name: metadata-storage
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi
```

### Docker Compose

**Multi-Node Setup**:
```yaml
version: '3.8'
services:
  mfsha-1:
    image: moosefs/mfsha:4.57.6
    environment:
      MFSHA_NODE_ID: 1
      MFSHA_PEERS: "mfsha-1:9430,mfsha-2:9430,mfsha-3:9430"
    volumes:
      - mfs-data-1:/usr/local/var/mfs
    ports:
      - "9430:9430"
      
  mfsha-2:
    image: moosefs/mfsha:4.57.6
    environment:
      MFSHA_NODE_ID: 2
      MFSHA_PEERS: "mfsha-1:9430,mfsha-2:9430,mfsha-3:9430"
    volumes:
      - mfs-data-2:/usr/local/var/mfs
    ports:
      - "9431:9430"
      
  mfsha-3:
    image: moosefs/mfsha:4.57.6
    environment:
      MFSHA_NODE_ID: 3
      MFSHA_PEERS: "mfsha-1:9430,mfsha-2:9430,mfsha-3:9430"
    volumes:
      - mfs-data-3:/usr/local/var/mfs
    ports:
      - "9432:9430"

volumes:
  mfs-data-1:
  mfs-data-2:
  mfs-data-3:
```

## Migration from Single Master

### Step-by-Step Migration

**Phase 1: Prepare HA Infrastructure**
```bash
# 1. Install HA extension on all nodes
make install-ha

# 2. Configure first HA node
cp /usr/local/etc/mfsmaster.cfg /usr/local/etc/mfsha.cfg
echo "MFSHA_NODE_ID = 1" >> /usr/local/etc/mfsha.cfg

# 3. Import existing metadata
mfsha --import-metadata /usr/local/var/mfs/metadata.mfs

# 4. Start first HA node
mfsha start
```

**Phase 2: Add HA Nodes**
```bash
# 5. Add second and third nodes
mfsha --join-cluster --node-id=2 --peer=existing-master:9430
mfsha --join-cluster --node-id=3 --peer=existing-master:9430

# 6. Verify cluster health
mfsha-admin status
```

**Phase 3: Switch Traffic**
```bash
# 7. Update client mount points to use HA cluster
# Clients can connect directly to any HA node
mfsmount /mnt/mfs -H node1:9430,node2:9430,node3:9430

# 8. Verify clients connect successfully
mfsmount --test-connection
```

**Phase 4: Full Cutover**
```bash
# 10. Stop old single master
systemctl stop mfsmaster-legacy

# 11. Update client configurations to point to HA cluster
# (Optional - clients auto-discover HA nodes through gossip)

# 12. Verify full HA operation
mfsha-admin failover-test --simulate
```

This comprehensive deployment guide provides everything needed to successfully deploy, operate, and maintain a MooseFS HA cluster in production environments.