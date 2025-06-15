/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA.
 */

#ifndef _RAFTCONSENSUS_H_
#define _RAFTCONSENSUS_H_

#include <inttypes.h>
#include <stdio.h>
#include <poll.h>

/* Raft Consensus Implementation for MooseFS HA */

/* Raft node states */
typedef enum {
	RAFT_STATE_FOLLOWER,
	RAFT_STATE_CANDIDATE,
	RAFT_STATE_LEADER
} raft_state_t;

/* Raft log entry */
typedef struct raft_log_entry {
	uint64_t term;        /* Term when entry was received by leader */
	uint64_t index;       /* Index in log */
	uint32_t type;        /* Entry type (metadata operation) */
	uint32_t shard_id;    /* Shard this entry belongs to */
	uint32_t data_size;   /* Size of data */
	uint8_t *data;        /* Serialized CRDT operation */
	struct raft_log_entry *next;
} raft_log_entry_t;

/* Raft peer information */
typedef struct raft_peer {
	uint32_t node_id;     /* Peer node identifier */
	char *host;           /* Peer hostname/IP */
	uint16_t port;        /* Peer port */
	uint64_t next_index;  /* Next log index to send to this peer */
	uint64_t match_index; /* Highest log index known to be replicated */
	uint64_t last_contact; /* Last time we heard from this peer */
	struct raft_peer *next;
} raft_peer_t;

#include "crdtstore.h" /* For hlc_timestamp_t */

/* Raft shard context */
typedef struct raft_shard {
	uint32_t shard_id;    /* Shard identifier */
	raft_state_t state;   /* Current state (follower/candidate/leader) */
	uint64_t current_term; /* Latest term server has seen */
	uint32_t voted_for;   /* CandidateId that received vote in current term */
	uint64_t commit_index; /* Index of highest log entry known to be committed */
	uint64_t last_applied; /* Index of highest log entry applied to state machine */
	
	/* Leader state with HLC-based leases */
	uint32_t current_leader;   /* Current leader node ID */
	hlc_timestamp_t leader_lease_hlc; /* HLC when leader lease expires */
	uint64_t leader_lease_epoch; /* Leader lease epoch for validation */
	
	/* Leader state */
	raft_peer_t *peers;   /* List of peers in this shard */
	uint32_t peer_count;  /* Number of peers */
	uint32_t votes_received; /* Votes received in current election */
	
	/* Log */
	raft_log_entry_t *log_head; /* First log entry */
	raft_log_entry_t *log_tail; /* Last log entry */
	uint64_t log_count;   /* Number of log entries */
	
	/* Timing */
	uint64_t election_timeout; /* Election timeout (ms) */
	uint64_t heartbeat_timeout; /* Heartbeat timeout (ms) */
	uint64_t last_heartbeat; /* Last heartbeat received */
	uint64_t election_start; /* When current election started */
	
	struct raft_shard *next;
} raft_shard_t;

/* Raft message types */
typedef enum {
	RAFT_MSG_REQUEST_VOTE,
	RAFT_MSG_REQUEST_VOTE_RESPONSE,
	RAFT_MSG_APPEND_ENTRIES,
	RAFT_MSG_APPEND_ENTRIES_RESPONSE
} raft_msg_type_t;

/* Raft RequestVote RPC */
typedef struct {
	uint64_t term;        /* Candidate's term */
	uint32_t candidate_id; /* Candidate requesting vote */
	uint64_t last_log_index; /* Index of candidate's last log entry */
	uint64_t last_log_term; /* Term of candidate's last log entry */
} raft_request_vote_t;

typedef struct {
	uint64_t term;        /* Current term, for candidate to update itself */
	uint8_t vote_granted; /* True means candidate received vote */
} raft_request_vote_response_t;

/* Raft AppendEntries RPC */
typedef struct {
	uint64_t term;        /* Leader's term */
	uint32_t leader_id;   /* So follower can redirect clients */
	uint64_t prev_log_index; /* Index of log entry immediately preceding new ones */
	uint64_t prev_log_term; /* Term of prev_log_index entry */
	uint32_t entry_count; /* Number of entries */
	raft_log_entry_t *entries; /* Log entries to store (empty for heartbeat) */
	uint64_t leader_commit; /* Leader's commit_index */
} raft_append_entries_t;

typedef struct {
	uint64_t term;        /* Current term, for leader to update itself */
	uint8_t success;      /* True if follower contained entry matching prev_log_index and prev_log_term */
	uint64_t match_index; /* Index of highest log entry known to match */
} raft_append_entries_response_t;

/* Raft message wrapper */
typedef struct {
	raft_msg_type_t type;
	uint32_t shard_id;
	union {
		raft_request_vote_t request_vote;
		raft_request_vote_response_t request_vote_response;
		raft_append_entries_t append_entries;
		raft_append_entries_response_t append_entries_response;
	} data;
} raft_message_t;

/* Raft API */
int raftconsensus_init(void);
void raftconsensus_term(void);

/* Shard management */
raft_shard_t* raft_create_shard(uint32_t shard_id, raft_peer_t *peers, uint32_t peer_count);
void raft_destroy_shard(raft_shard_t *shard);
int raft_add_peer(raft_shard_t *shard, uint32_t node_id, const char *host, uint16_t port);
int raft_remove_peer(raft_shard_t *shard, uint32_t node_id);

/* Leader lock operations */
int raft_is_leader(uint32_t shard_id);
uint32_t raft_get_leader(uint32_t shard_id);
int raft_transfer_leadership(uint32_t shard_id, uint32_t target_node);

/* Log operations */
int raft_append_entry(uint32_t shard_id, uint32_t type, const void *data, uint32_t data_size);
int raft_commit_entries(uint32_t shard_id, uint64_t commit_index);

/* Message handling */
int raft_handle_message(const raft_message_t *msg, uint32_t from_node);
int raft_send_message(const raft_message_t *msg, uint32_t to_node);
void raft_handle_incoming_message(uint32_t from_node, const uint8_t *data, uint32_t length);

/* Timing and maintenance */
void raft_tick(void); /* Called periodically to handle timeouts */
void raftconsensus_tick(double now); /* Main loop periodic maintenance */
void raftconsensus_reload(void); /* Configuration reload */
void raft_start_election(raft_shard_t *shard);
void raft_send_heartbeats(raft_shard_t *shard);

/* Network integration */
void raftconsensus_desc(struct pollfd *pdesc, uint32_t *ndesc);
void raftconsensus_serve(struct pollfd *pdesc);
void raftconsensus_info(FILE *fd);

/* State queries */
raft_state_t raft_get_state(uint32_t shard_id);
uint64_t raft_get_term(uint32_t shard_id);
uint64_t raft_get_commit_index(uint32_t shard_id);

/* Persistence */
int raft_save_state(raft_shard_t *shard);
int raft_load_state(raft_shard_t *shard);

/* Statistics */
typedef struct {
	uint32_t shard_count;
	uint32_t leader_count;
	uint32_t follower_count;
	uint32_t candidate_count;
	uint64_t total_log_entries;
	uint64_t committed_entries;
	uint32_t active_peers;
} raft_stats_t;

void raft_get_stats(raft_stats_t *stats);

#endif

