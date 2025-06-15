/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 * 
 * Simplified Raft implementation with single global leader
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>

#include "raftconsensus.h"
#include "cfg.h"
#include "mfslog.h"
#include "massert.h"
#include "clocks.h"
#include "random.h"
#include "haconn.h"
#include "datapack.h"
#include "hamaster.h"
#include "main.h"
#include "metadata.h"

/* Global Raft state - single group, no sharding */
static raft_context_t raft_state;
static uint32_t local_node_id = 0;
static pthread_mutex_t raft_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Configuration */
static uint64_t election_timeout_base = 1000;  /* 1 second base */
static uint64_t heartbeat_interval = 100;      /* 100ms heartbeats */
static uint64_t lease_duration = 30;           /* 30 second lease */

/* Forward declarations */
static void raft_send_request_vote(void);
static void raft_send_append_entries(raft_peer_t *peer);
static void raft_apply_committed_entries(void);
static void raft_become_leader(void);
static int raft_has_minimum_peers(void);
void raftconsensus_tick_wrapper(void);

/* Get random election timeout */
static uint64_t get_election_timeout(void) {
	return election_timeout_base + (rndu32_ranged(election_timeout_base / 2));
}

/* Initialize Raft consensus */
int raftconsensus_init(void) {
	pthread_mutex_lock(&raft_mutex);
	
	/* Initialize state */
	memset(&raft_state, 0, sizeof(raft_state));
	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.current_term = 0;
	raft_state.voted_for = 0;
	raft_state.commit_index = 0;
	raft_state.last_applied = 0;
	raft_state.current_version = meta_version();
	
	/* Load configuration */
	local_node_id = ha_get_node_id();
	election_timeout_base = cfg_getuint32("RAFT_ELECTION_TIMEOUT", 1000);
	heartbeat_interval = cfg_getuint32("RAFT_HEARTBEAT_INTERVAL", 100);
	lease_duration = cfg_getuint32("RAFT_LEASE_DURATION", 30);
	
	/* Set initial timeouts */
	raft_state.election_timeout = get_election_timeout();
	raft_state.heartbeat_timeout = heartbeat_interval;
	raft_state.last_heartbeat = monotonic_useconds() / 1000; /* Don't trigger immediate election */
	
	/* No leader initially */
	raft_state.current_leader = 0;
	raft_state.leader_lease_expiry = 0;
	raft_state.lease_duration = lease_duration;
	
	pthread_mutex_unlock(&raft_mutex);
	
	/* Register with main loop */
	main_time_register(1, 0, raftconsensus_tick_wrapper);  /* Call every 1 second */
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Raft consensus initialized: node_id=%u version=%"PRIu64,
	        local_node_id, raft_state.current_version);
	
	/* Don't start election immediately - wait for peers to connect */
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Waiting for peers to connect before starting election");
	
	return 0;
}

/* Terminate Raft consensus */
void raftconsensus_term(void) {
	raft_log_entry_t *entry, *next;
	raft_peer_t *peer, *next_peer;
	
	pthread_mutex_lock(&raft_mutex);
	
	/* Free log entries */
	entry = raft_state.log_head;
	while (entry) {
		next = entry->next;
		if (entry->data) {
			free(entry->data);
		}
		free(entry);
		entry = next;
	}
	
	/* Free peers */
	peer = raft_state.peers;
	while (peer) {
		next_peer = peer->next;
		if (peer->host) {
			free(peer->host);
		}
		free(peer);
		peer = next_peer;
	}
	
	pthread_mutex_unlock(&raft_mutex);
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Raft consensus terminated");
}

/* Check if we are the leader */
int raft_is_leader(void) {
	int is_leader;
	
	pthread_mutex_lock(&raft_mutex);
	is_leader = (raft_state.state == RAFT_STATE_LEADER);
	pthread_mutex_unlock(&raft_mutex);
	
	return is_leader;
}

/* Get current leader */
uint32_t raft_get_leader(void) {
	uint32_t leader;
	
	pthread_mutex_lock(&raft_mutex);
	leader = raft_state.current_leader;
	pthread_mutex_unlock(&raft_mutex);
	
	return leader;
}

/* Check if we have a valid lease */
int raft_has_valid_lease(void) {
	int valid = 0;
	uint64_t now;
	
	pthread_mutex_lock(&raft_mutex);
	
	if (raft_state.state == RAFT_STATE_LEADER) {
		now = monotonic_seconds();
		valid = (now < raft_state.leader_lease_expiry);
	}
	
	pthread_mutex_unlock(&raft_mutex);
	
	return valid;
}

/* Get next version number (leader only with lease) */
uint64_t raft_get_next_version(void) {
	uint64_t version = 0;
	uint64_t now;
	
	pthread_mutex_lock(&raft_mutex);
	
	if (raft_state.state == RAFT_STATE_LEADER) {
		now = monotonic_seconds();
		
		/* Check lease validity */
		if (now < raft_state.leader_lease_expiry) {
			/* We have a valid lease - allocate version */
			version = ++raft_state.current_version;
			
			mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "Raft allocated version %"PRIu64" (lease valid for %"PRIu64"s)",
			        version, raft_state.leader_lease_expiry - now);
		} else {
			/* Lease expired - cannot allocate */
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "Raft lease expired - cannot allocate version (now=%"PRIu64", lease_expiry=%"PRIu64")",
			        now, raft_state.leader_lease_expiry);
		}
	} else {
		/* Not leader */
		mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "Not Raft leader (leader is %u) - cannot allocate version", 
		        raft_state.current_leader);
	}
	
	pthread_mutex_unlock(&raft_mutex);
	
	return version;
}

/* Append entry to Raft log */
int raft_append_entry(uint32_t type, const void *data, uint32_t data_size, uint64_t version) {
	raft_log_entry_t *entry;
	int result = -1;
	
	pthread_mutex_lock(&raft_mutex);
	
	if (raft_state.state != RAFT_STATE_LEADER) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "Cannot append to Raft log - not leader");
		pthread_mutex_unlock(&raft_mutex);
		return -1;
	}
	
	/* Create new log entry */
	entry = malloc(sizeof(raft_log_entry_t));
	if (!entry) {
		pthread_mutex_unlock(&raft_mutex);
		return -1;
	}
	
	entry->term = raft_state.current_term;
	entry->index = raft_state.log_count + 1;
	entry->version = version;
	entry->type = type;
	entry->data_size = data_size;
	entry->data = malloc(data_size);
	entry->next = NULL;
	
	if (!entry->data) {
		free(entry);
		pthread_mutex_unlock(&raft_mutex);
		return -1;
	}
	
	memcpy(entry->data, data, data_size);
	
	/* Append to log */
	if (raft_state.log_tail) {
		raft_state.log_tail->next = entry;
	} else {
		raft_state.log_head = entry;
	}
	raft_state.log_tail = entry;
	raft_state.log_count++;
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "Appended to Raft log: index=%"PRIu64" version=%"PRIu64" type=%u size=%u",
	        entry->index, entry->version, type, data_size);
	
	/* Send to followers immediately */
	raft_peer_t *peer = raft_state.peers;
	while (peer) {
		raft_send_append_entries(peer);
		peer = peer->next;
	}
	
	result = 0;
	
	pthread_mutex_unlock(&raft_mutex);
	
	return result;
}

/* Start election */
void raft_start_election(void) {
	pthread_mutex_lock(&raft_mutex);
	
	/* Increment term and transition to candidate */
	raft_state.current_term++;
	raft_state.state = RAFT_STATE_CANDIDATE;
	raft_state.voted_for = local_node_id;
	raft_state.votes_received = 1; /* Vote for self */
	raft_state.election_start = monotonic_useconds() / 1000;
	raft_state.current_leader = 0;
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Starting election for term %"PRIu64" (peer_count=%u)", 
	        raft_state.current_term, raft_state.peer_count);
	
	/* Reset election timeout */
	raft_state.election_timeout = get_election_timeout();
	
	/* Calculate quorum needed */
	uint32_t total_nodes = raft_state.peer_count + 1; /* peers + self */
	uint32_t quorum_needed = (total_nodes / 2) + 1;
	
	/* Check if we have quorum */
	if (raft_state.peer_count < (quorum_needed - 1)) {
		/* Not enough peers connected for quorum */
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "Not enough peers for quorum - need %u peers, have %u", 
		        quorum_needed - 1, raft_state.peer_count);
		pthread_mutex_unlock(&raft_mutex);
		return;
	}
	
	/* Send RequestVote to all peers */
	raft_send_request_vote();
	
	pthread_mutex_unlock(&raft_mutex);
}

/* Send heartbeats to all peers */
void raft_send_heartbeats(void) {
	raft_peer_t *peer;
	
	pthread_mutex_lock(&raft_mutex);
	
	if (raft_state.state != RAFT_STATE_LEADER) {
		pthread_mutex_unlock(&raft_mutex);
		return;
	}
	
	/* Send empty AppendEntries as heartbeat */
	peer = raft_state.peers;
	while (peer) {
		raft_send_append_entries(peer);
		peer = peer->next;
	}
	
	pthread_mutex_unlock(&raft_mutex);
}

/* Internal version that assumes mutex is already held */
static void raft_send_heartbeats_internal(void) {
	raft_peer_t *peer;
	
	/* Send empty AppendEntries as heartbeat */
	peer = raft_state.peers;
	while (peer) {
		raft_send_append_entries(peer);
		peer = peer->next;
	}
}

/* Become leader */
static void raft_become_leader(void) {
	uint64_t now;
	
	/* Already holding raft_mutex */
	
	raft_state.state = RAFT_STATE_LEADER;
	raft_state.current_leader = local_node_id;
	raft_state.votes_received = 0;
	
	/* Set leader lease */
	now = monotonic_seconds();
	raft_state.leader_lease_expiry = now + raft_state.lease_duration;
	
	/* Ensure version is up to date */
	uint64_t meta_ver = meta_version();
	if (meta_ver > raft_state.current_version) {
		raft_state.current_version = meta_ver;
	}
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Became leader for term %"PRIu64" with lease until %"PRIu64" (now=%"PRIu64", duration=%"PRIu64") starting at version %"PRIu64,
	        raft_state.current_term, raft_state.leader_lease_expiry, now, raft_state.lease_duration, raft_state.current_version);
	
	/* Send initial heartbeats (we already hold the mutex) */
	raft_send_heartbeats_internal();
}

/* Send RequestVote RPC */
static void raft_send_request_vote(void) {
	raft_message_t msg;
	raft_log_entry_t *last_entry;
	
	/* Already holding raft_mutex */
	
	/* Prepare RequestVote message */
	msg.type = RAFT_MSG_REQUEST_VOTE;
	msg.data.request_vote.term = raft_state.current_term;
	msg.data.request_vote.candidate_id = local_node_id;
	
	/* Get last log entry info */
	last_entry = raft_state.log_tail;
	if (last_entry) {
		msg.data.request_vote.last_log_index = last_entry->index;
		msg.data.request_vote.last_log_term = last_entry->term;
	} else {
		msg.data.request_vote.last_log_index = 0;
		msg.data.request_vote.last_log_term = 0;
	}
	
	/* Send to all peers */
	raft_peer_t *peer = raft_state.peers;
	while (peer) {
		/* Send via haconn */
		uint8_t buffer[1024];
		uint8_t *ptr = buffer;
		
		put32bit(&ptr, msg.type);
		put64bit(&ptr, msg.data.request_vote.term);
		put32bit(&ptr, msg.data.request_vote.candidate_id);
		put64bit(&ptr, msg.data.request_vote.last_log_index);
		put64bit(&ptr, msg.data.request_vote.last_log_term);
		
		haconn_send_raft_request(peer->node_id, buffer, ptr - buffer);
		
		peer = peer->next;
	}
}

/* Send AppendEntries RPC */
static void raft_send_append_entries(raft_peer_t *peer) {
	raft_message_t msg;
	raft_log_entry_t *entry;
	uint64_t prev_index, prev_term;
	
	/* Already holding raft_mutex */
	
	if (!peer) return;
	
	/* Prepare AppendEntries message */
	msg.type = RAFT_MSG_APPEND_ENTRIES;
	msg.data.append_entries.term = raft_state.current_term;
	msg.data.append_entries.leader_id = local_node_id;
	msg.data.append_entries.leader_commit = raft_state.commit_index;
	
	/* Find entries to send */
	prev_index = peer->next_index - 1;
	prev_term = 0;
	
	/* Find prev log entry */
	entry = raft_state.log_head;
	while (entry && entry->index < prev_index) {
		entry = entry->next;
	}
	if (entry && entry->index == prev_index) {
		prev_term = entry->term;
	}
	
	msg.data.append_entries.prev_log_index = prev_index;
	msg.data.append_entries.prev_log_term = prev_term;
	
	/* For now, send as heartbeat (no entries) */
	msg.data.append_entries.entry_count = 0;
	msg.data.append_entries.entries = NULL;
	
	/* Send via haconn */
	uint8_t buffer[1024];
	uint8_t *ptr = buffer;
	
	put32bit(&ptr, msg.type);
	put64bit(&ptr, msg.data.append_entries.term);
	put32bit(&ptr, msg.data.append_entries.leader_id);
	put64bit(&ptr, msg.data.append_entries.prev_log_index);
	put64bit(&ptr, msg.data.append_entries.prev_log_term);
	put64bit(&ptr, msg.data.append_entries.leader_commit);
	put32bit(&ptr, msg.data.append_entries.entry_count);
	
	haconn_send_raft_request(peer->node_id, buffer, ptr - buffer);
}

/* Handle incoming Raft message */
void raft_handle_incoming_message(uint32_t from_node, const uint8_t *data, uint32_t length) {
	const uint8_t *ptr = data;
	uint32_t msg_type;
	
	if (length < 4) return;
	
	msg_type = get32bit(&ptr);
	
	pthread_mutex_lock(&raft_mutex);
	
	switch (msg_type) {
		case RAFT_MSG_REQUEST_VOTE: {
			if (length < 28) break;
			
			uint64_t term = get64bit(&ptr);
			uint32_t candidate_id = get32bit(&ptr);
			uint64_t last_log_index = get64bit(&ptr);
			uint64_t last_log_term = get64bit(&ptr);
			
			int vote_granted = 0;
			
			mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "RequestVote from node %u term %"PRIu64" (our term %"PRIu64", our state %d)",
			        candidate_id, term, raft_state.current_term, raft_state.state);
			
			/* Update term if necessary */
			if (term > raft_state.current_term) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "RequestVote: updating term from %"PRIu64" to %"PRIu64,
				        raft_state.current_term, term);
				raft_state.current_term = term;
				raft_state.voted_for = 0;
				raft_state.state = RAFT_STATE_FOLLOWER;
				raft_state.current_leader = 0;
			}
			
			/* Grant vote if we haven't voted and candidate's log is up to date */
			if (term == raft_state.current_term && 
			    (raft_state.voted_for == 0 || raft_state.voted_for == candidate_id)) {
				
				/* Check if candidate's log is at least as up to date as ours */
				raft_log_entry_t *last = raft_state.log_tail;
				uint64_t our_last_term = last ? last->term : 0;
				uint64_t our_last_index = last ? last->index : 0;
				
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "Vote check: voted_for=%u, candidate=%u, their_log=%"PRIu64":%"PRIu64", our_log=%"PRIu64":%"PRIu64,
				        raft_state.voted_for, candidate_id, last_log_term, last_log_index, our_last_term, our_last_index);
				
				if (last_log_term > our_last_term ||
				    (last_log_term == our_last_term && last_log_index >= our_last_index)) {
					vote_granted = 1;
					raft_state.voted_for = candidate_id;
					raft_state.last_heartbeat = monotonic_useconds() / 1000;
				}
			}
			
			/* Send response */
			uint8_t resp[16];
			uint8_t *resp_ptr = resp;
			put32bit(&resp_ptr, RAFT_MSG_REQUEST_VOTE_RESPONSE);
			put64bit(&resp_ptr, raft_state.current_term);
			put8bit(&resp_ptr, vote_granted);
			
			haconn_send_raft_response(from_node, resp, resp_ptr - resp);
			
			mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "RequestVote from %u term %"PRIu64" - %s",
			        candidate_id, term, vote_granted ? "granted" : "denied");
			break;
		}
		
		case RAFT_MSG_REQUEST_VOTE_RESPONSE: {
			if (length < 13) break;
			
			uint64_t term = get64bit(&ptr);
			uint8_t vote_granted = get8bit(&ptr);
			
			/* Ignore if not candidate */
			if (raft_state.state != RAFT_STATE_CANDIDATE) {
				break;
			}
			
			/* Update term if necessary */
			if (term > raft_state.current_term) {
				raft_state.current_term = term;
				raft_state.state = RAFT_STATE_FOLLOWER;
				raft_state.voted_for = 0;
				raft_state.current_leader = 0;
				break;
			}
			
			/* Count vote if for current term */
			if (term == raft_state.current_term && vote_granted) {
				raft_state.votes_received++;
				
				/* Check if we have majority */
				uint32_t majority = (raft_state.peer_count + 1) / 2 + 1;
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Vote granted from node %u - votes=%u/%u needed=%u",
				        from_node, raft_state.votes_received, raft_state.peer_count + 1, majority);
				
				if (raft_state.votes_received >= majority) {
					mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Won election with %u votes out of %u nodes",
					        raft_state.votes_received, raft_state.peer_count + 1);
					raft_become_leader();
				}
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "Vote response: term=%"PRIu64" current=%"PRIu64" granted=%d from node %u",
				        term, raft_state.current_term, vote_granted, from_node);
			}
			break;
		}
		
		case RAFT_MSG_APPEND_ENTRIES: {
			if (length < 40) break;
			
			uint64_t term = get64bit(&ptr);
			uint32_t leader_id = get32bit(&ptr);
			uint64_t prev_log_index = get64bit(&ptr);
			uint64_t prev_log_term = get64bit(&ptr);
			uint64_t leader_commit = get64bit(&ptr);
			uint32_t entry_count = get32bit(&ptr);
			
			int success = 0;
			uint64_t match_index = 0;
			
			/* Update term if necessary */
			if (term > raft_state.current_term) {
				raft_state.current_term = term;
				raft_state.voted_for = 0;
				raft_state.state = RAFT_STATE_FOLLOWER;
			}
			
			/* Reset election timeout on valid leader message */
			if (term == raft_state.current_term) {
				if (raft_state.state == RAFT_STATE_CANDIDATE) {
					mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Stepping down from candidate to follower - leader %u established for term %"PRIu64,
					        leader_id, term);
				}
				raft_state.state = RAFT_STATE_FOLLOWER;
				raft_state.current_leader = leader_id;
				raft_state.last_heartbeat = monotonic_useconds() / 1000;
				success = 1;
				
				/* Update commit index */
				if (leader_commit > raft_state.commit_index) {
					raft_state.commit_index = leader_commit;
					/* Apply committed entries */
					raft_apply_committed_entries();
				}
			}
			
			/* Send response */
			uint8_t resp[24];
			uint8_t *resp_ptr = resp;
			put32bit(&resp_ptr, RAFT_MSG_APPEND_ENTRIES_RESPONSE);
			put64bit(&resp_ptr, raft_state.current_term);
			put8bit(&resp_ptr, success);
			put64bit(&resp_ptr, match_index);
			
			haconn_send_raft_response(from_node, resp, resp_ptr - resp);
			break;
		}
		
		case RAFT_MSG_APPEND_ENTRIES_RESPONSE: {
			/* Handle response - update peer tracking */
			break;
		}
	}
	
	pthread_mutex_unlock(&raft_mutex);
}

/* Apply committed log entries */
static void raft_apply_committed_entries(void) {
	raft_log_entry_t *entry;
	
	/* Already holding raft_mutex */
	
	entry = raft_state.log_head;
	while (entry && entry->index <= raft_state.commit_index) {
		if (entry->index > raft_state.last_applied) {
			/* Apply this entry */
			/* For now, just update our version */
			if (entry->version > raft_state.current_version) {
				raft_state.current_version = entry->version;
			}
			
			raft_state.last_applied = entry->index;
			
			mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "Applied log entry %"PRIu64" version %"PRIu64,
			        entry->index, entry->version);
		}
		entry = entry->next;
	}
}

/* Periodic tick function */
void raftconsensus_tick(double now) {
	uint64_t now_ms = (uint64_t)(now * 1000);
	static uint64_t last_tick_log = 0;
	static int first_call = 1;
	
	if (first_call) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "raftconsensus_tick first call: now=%f, now_ms=%"PRIu64, now, now_ms);
		first_call = 0;
	}
	
	pthread_mutex_lock(&raft_mutex);
	
	/* Log tick status every 10 seconds */
	if (now_ms - last_tick_log > 10000) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "Raft tick: state=%d, last_heartbeat=%"PRIu64", now=%"PRIu64", timeout=%"PRIu64", peers=%u",
		        raft_state.state, raft_state.last_heartbeat, now_ms, raft_state.election_timeout, raft_state.peer_count);
		last_tick_log = now_ms;
	}
	
	switch (raft_state.state) {
		case RAFT_STATE_FOLLOWER:
			/* Check election timeout */
			if (now_ms - raft_state.last_heartbeat > raft_state.election_timeout) {
				/* Check if we have minimum peers before starting election */
				if (!raft_has_minimum_peers()) {
					uint32_t expected_cluster_size = cfg_getuint32("RAFT_CLUSTER_SIZE", 3);
					uint32_t majority = (expected_cluster_size / 2) + 1;
					mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "Election timeout but no quorum (%u total nodes = self + %u peers, need %u for majority)",
					        1 + raft_state.peer_count, raft_state.peer_count, majority);
					/* Reset timeout to check again later */
					raft_state.last_heartbeat = now_ms;
					break;
				}
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Election timeout - starting election (last_heartbeat=%"PRIu64", now=%"PRIu64", timeout=%"PRIu64")",
				        raft_state.last_heartbeat, now_ms, raft_state.election_timeout);
				pthread_mutex_unlock(&raft_mutex);
				raft_start_election();
				return;
			}
			break;
			
		case RAFT_STATE_CANDIDATE:
			/* Check election timeout */
			if (now_ms - raft_state.election_start > raft_state.election_timeout) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Election timeout in candidate state - restarting");
				pthread_mutex_unlock(&raft_mutex);
				raft_start_election();
				return;
			}
			break;
			
		case RAFT_STATE_LEADER:
			/* Send periodic heartbeats */
			if (now_ms - raft_state.last_heartbeat > raft_state.heartbeat_timeout) {
				raft_state.last_heartbeat = now_ms;
				pthread_mutex_unlock(&raft_mutex);
				raft_send_heartbeats();
				return;
			}
			
			/* Check lease expiry */
			{
				uint64_t now_seconds = monotonic_seconds();
				if (now_seconds > raft_state.leader_lease_expiry - 5) {
					/* Lease about to expire - renew by sending heartbeats */
					raft_state.leader_lease_expiry = now_seconds + raft_state.lease_duration;
					mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "Renewed leader lease until %"PRIu64" (now=%"PRIu64")",
					        raft_state.leader_lease_expiry, now_seconds);
				}
			}
			break;
	}
	
	pthread_mutex_unlock(&raft_mutex);
}

/* Wrapper for main loop */
void raftconsensus_tick_wrapper(void) {
	static int first_tick = 1;
	if (first_tick) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Raft tick started!");
		first_tick = 0;
	}
	raftconsensus_tick(monotonic_seconds());
}

/* Add peer */
int raft_add_peer(uint32_t node_id, const char *host, uint16_t port) {
	raft_peer_t *peer;
	int first_time_has_quorum = 0;
	
	pthread_mutex_lock(&raft_mutex);
	
	/* Check if peer already exists */
	peer = raft_state.peers;
	while (peer) {
		if (peer->node_id == node_id) {
			pthread_mutex_unlock(&raft_mutex);
			return 0; /* Already exists */
		}
		peer = peer->next;
	}
	
	/* Check if we're about to reach quorum for the first time */
	uint32_t expected_cluster_size = cfg_getuint32("RAFT_CLUSTER_SIZE", 3);
	uint32_t majority = (expected_cluster_size / 2) + 1;
	uint32_t current_total = 1 + raft_state.peer_count;  /* self + peers */
	uint32_t new_total = 1 + raft_state.peer_count + 1;  /* after adding this peer */
	
	if (current_total < majority && new_total >= majority) {
		first_time_has_quorum = 1;
	}
	
	/* Create new peer */
	peer = malloc(sizeof(raft_peer_t));
	if (!peer) {
		pthread_mutex_unlock(&raft_mutex);
		return -1;
	}
	
	peer->node_id = node_id;
	peer->host = strdup(host);
	peer->port = port;
	peer->next_index = 1;
	peer->match_index = 0;
	peer->last_contact = 0;
	
	/* Add to list */
	peer->next = raft_state.peers;
	raft_state.peers = peer;
	raft_state.peer_count++;
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Added Raft peer %u at %s:%u (total peers: %u)",
	        node_id, host, port, raft_state.peer_count);
	
	/* If we just reached quorum and haven't had an election yet, trigger one */
	if (first_time_has_quorum && raft_state.state == RAFT_STATE_FOLLOWER && 
	    raft_state.current_term == 0) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Reached quorum (%u total nodes = self + %u peers) - starting first election immediately",
		        new_total, raft_state.peer_count);
		pthread_mutex_unlock(&raft_mutex);
		/* Start election immediately */
		raft_start_election();
		return 0;
	}
	
	pthread_mutex_unlock(&raft_mutex);
	
	return 0;
}

/* Get current state */
raft_state_t raft_get_state(void) {
	raft_state_t state;
	
	pthread_mutex_lock(&raft_mutex);
	state = raft_state.state;
	pthread_mutex_unlock(&raft_mutex);
	
	return state;
}

/* Get current term */
uint64_t raft_get_term(void) {
	uint64_t term;
	
	pthread_mutex_lock(&raft_mutex);
	term = raft_state.current_term;
	pthread_mutex_unlock(&raft_mutex);
	
	return term;
}

/* Get current version */
uint64_t raft_get_current_version(void) {
	uint64_t version;
	
	pthread_mutex_lock(&raft_mutex);
	version = raft_state.current_version;
	pthread_mutex_unlock(&raft_mutex);
	
	return version;
}

/* Check if we have minimum peers for quorum */
static int raft_has_minimum_peers(void) {
	uint32_t min_peers;
	uint32_t total_nodes;
	
	/* Already holding raft_mutex */
	
	/* Get minimum required peers from config (default: majority - 1) */
	/* For 3-node cluster: need 2 total nodes (self + 1 peer) */
	/* For 5-node cluster: need 3 total nodes (self + 2 peers) */
	min_peers = cfg_getuint32("RAFT_MIN_PEERS", 1);  /* Default 1 peer for 3-node cluster */
	
	/* Total nodes = self + connected peers */
	total_nodes = 1 + raft_state.peer_count;
	
	/* Check if we have majority */
	uint32_t expected_cluster_size = cfg_getuint32("RAFT_CLUSTER_SIZE", 3);
	uint32_t majority = (expected_cluster_size / 2) + 1;
	
	return (total_nodes >= majority);
}

/* Get statistics */
void raft_get_stats(raft_stats_t *stats) {
	if (!stats) return;
	
	pthread_mutex_lock(&raft_mutex);
	
	stats->current_state = raft_state.state;
	stats->current_leader = raft_state.current_leader;
	stats->current_term = raft_state.current_term;
	stats->current_version = raft_state.current_version;
	stats->total_log_entries = raft_state.log_count;
	stats->committed_entries = raft_state.commit_index;
	stats->active_peers = raft_state.peer_count;
	
	pthread_mutex_unlock(&raft_mutex);
}

/* Display Raft info */
void raftconsensus_info(FILE *fd) {
	const char *state_str;
	uint64_t now;
	
	pthread_mutex_lock(&raft_mutex);
	
	switch (raft_state.state) {
		case RAFT_STATE_FOLLOWER: state_str = "FOLLOWER"; break;
		case RAFT_STATE_CANDIDATE: state_str = "CANDIDATE"; break;
		case RAFT_STATE_LEADER: state_str = "LEADER"; break;
		default: state_str = "UNKNOWN";
	}
	
	fprintf(fd, "[Raft Consensus]\n");
	fprintf(fd, "state: %s\n", state_str);
	fprintf(fd, "node_id: %u\n", local_node_id);
	fprintf(fd, "current_term: %"PRIu64"\n", raft_state.current_term);
	fprintf(fd, "current_leader: %u\n", raft_state.current_leader);
	fprintf(fd, "current_version: %"PRIu64"\n", raft_state.current_version);
	fprintf(fd, "voted_for: %u\n", raft_state.voted_for);
	fprintf(fd, "commit_index: %"PRIu64"\n", raft_state.commit_index);
	fprintf(fd, "last_applied: %"PRIu64"\n", raft_state.last_applied);
	fprintf(fd, "log_entries: %"PRIu64"\n", raft_state.log_count);
	fprintf(fd, "peer_count: %u\n", raft_state.peer_count);
	
	if (raft_state.state == RAFT_STATE_LEADER) {
		now = monotonic_seconds();
		fprintf(fd, "lease_valid: %s\n", now < raft_state.leader_lease_expiry ? "yes" : "no");
		fprintf(fd, "lease_remaining: %"PRId64"s\n", 
		        (int64_t)(raft_state.leader_lease_expiry - now));
	}
	
	fprintf(fd, "\n[Raft Peers]\n");
	raft_peer_t *peer = raft_state.peers;
	while (peer) {
		fprintf(fd, "peer %u: %s:%u next_index=%"PRIu64" match_index=%"PRIu64"\n",
		        peer->node_id, peer->host, peer->port, 
		        peer->next_index, peer->match_index);
		peer = peer->next;
	}
	
	pthread_mutex_unlock(&raft_mutex);
}

/* Stub implementations for network integration */
void raftconsensus_desc(struct pollfd *pdesc, uint32_t *ndesc) {
	/* Raft messages are handled through haconn */
}

void raftconsensus_serve(struct pollfd *pdesc) {
	/* Raft messages are handled through haconn */
}

void raftconsensus_reload(void) {
	pthread_mutex_lock(&raft_mutex);
	
	election_timeout_base = cfg_getuint32("RAFT_ELECTION_TIMEOUT", 1000);
	heartbeat_interval = cfg_getuint32("RAFT_HEARTBEAT_INTERVAL", 100);
	lease_duration = cfg_getuint32("RAFT_LEASE_DURATION", 30);
	
	raft_state.heartbeat_timeout = heartbeat_interval;
	raft_state.lease_duration = lease_duration;
	
	pthread_mutex_unlock(&raft_mutex);
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Raft configuration reloaded");
}

/* Stub implementations - not used in simplified design */
int raft_handle_message(const raft_message_t *msg, uint32_t from_node) {
	return 0;
}

int raft_send_message(const raft_message_t *msg, uint32_t to_node) {
	return 0;
}

int raft_save_state(void) {
	return 0;
}

int raft_load_state(void) {
	return 0;
}

void raft_tick(void) {
	/* Use raftconsensus_tick instead */
}

