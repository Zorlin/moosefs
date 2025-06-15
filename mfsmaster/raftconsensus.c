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
#include "crdtstore.h"
#include "haconn.h"
#include "datapack.h"
#include "hamaster.h"
#include "main.h"

static raft_shard_t *shards = NULL;
static uint32_t local_node_id = 0;
static pthread_mutex_t raft_mutex = PTHREAD_MUTEX_INITIALIZER;
static hlc_timestamp_t global_hlc;
static uint64_t leader_lease_duration = 30000; /* 30 second leader leases */
static uint32_t total_nodes = 3; /* Total nodes in cluster - should be from config */

/* Forward declarations */
void raftconsensus_tick_wrapper(void);

/* Random election timeout between min and max */
static uint64_t get_election_timeout(uint64_t base_timeout) {
	return base_timeout + (rndu32_ranged(base_timeout / 2));
}

/* Find shard by ID */
static raft_shard_t* find_shard(uint32_t shard_id) {
	raft_shard_t *shard = shards;
	
	while (shard != NULL) {
		if (shard->shard_id == shard_id) {
			return shard;
		}
		shard = shard->next;
	}
	
	return NULL;
}

/* Become leader for a shard */
static void raft_become_leader(raft_shard_t *shard) {
	hlc_timestamp_t now_hlc;
	
	if (shard == NULL) {
		return;
	}
	
	/* Transition to leader */
	shard->state = RAFT_STATE_LEADER;
	shard->current_leader = local_node_id;
	shard->votes_received = 0;
	
	/* Set leader lease using HLC */
	hlc_init(&now_hlc);
	hlc_update(&now_hlc, &global_hlc);
	shard->leader_lease_hlc = now_hlc;
	shard->leader_lease_hlc.physical_time += leader_lease_duration * 1000; /* Convert ms to us */
	shard->leader_lease_epoch++;
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "raft_become_leader: became leader for shard %"PRIu32" term %"PRIu64" lease_epoch %"PRIu64, 
	       shard->shard_id, shard->current_term, shard->leader_lease_epoch);
	
	/* Send initial heartbeats */
	raft_send_heartbeats(shard);
}

/* Create new raft shard */
raft_shard_t* raft_create_shard(uint32_t shard_id, raft_peer_t *peers, uint32_t peer_count) {
	raft_shard_t *shard;
	raft_peer_t *peer;
	uint32_t i;
	uint64_t base_timeout;
	
	shard = malloc(sizeof(raft_shard_t));
	if (shard == NULL) {
		return NULL;
	}
	
	memset(shard, 0, sizeof(raft_shard_t));
	
	shard->shard_id = shard_id;
	shard->state = RAFT_STATE_FOLLOWER;
	shard->current_term = 0;
	shard->voted_for = 0;
	shard->current_leader = 0;
	shard->votes_received = 0;
	shard->commit_index = 0;
	shard->last_applied = 0;
	shard->peer_count = peer_count;
	
	/* Initialize HLC for leader lease */
	hlc_init(&shard->leader_lease_hlc);
	shard->leader_lease_epoch = 0;
	
	/* Set timeouts from configuration */
	base_timeout = cfg_getuint32("HA_RAFT_ELECTION_TIMEOUT", 1000);
	shard->election_timeout = get_election_timeout(base_timeout);
	shard->heartbeat_timeout = cfg_getuint32("HA_RAFT_HEARTBEAT_TIMEOUT", 100);
	shard->last_heartbeat = monotonic_useconds() / 1000;
	
	/* Copy peers */
	shard->peers = NULL;
	for (i = 0; i < peer_count && peers != NULL; i++) {
		peer = malloc(sizeof(raft_peer_t));
		if (peer == NULL) {
			/* Cleanup on failure */
			raft_destroy_shard(shard);
			return NULL;
		}
		
		peer->node_id = peers[i].node_id;
		peer->host = strdup(peers[i].host);
		peer->port = peers[i].port;
		peer->next_index = 1;
		peer->match_index = 0;
		peer->last_contact = 0;
		
		peer->next = shard->peers;
		shard->peers = peer;
	}
	
	/* Add to global shard list */
	pthread_mutex_lock(&raft_mutex);
	shard->next = shards;
	shards = shard;
	pthread_mutex_unlock(&raft_mutex);
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "raft_create_shard: created shard %"PRIu32" with %"PRIu32" peers", 
	       shard_id, peer_count);
	
	return shard;
}

/* Destroy raft shard */
void raft_destroy_shard(raft_shard_t *shard) {
	raft_peer_t *peer, *next_peer;
	raft_log_entry_t *entry, *next_entry;
	raft_shard_t *current, *prev;
	
	if (shard == NULL) {
		return;
	}
	
	/* Remove from global list */
	pthread_mutex_lock(&raft_mutex);
	current = shards;
	prev = NULL;
	
	while (current != NULL) {
		if (current == shard) {
			if (prev != NULL) {
				prev->next = current->next;
			} else {
				shards = current->next;
			}
			break;
		}
		prev = current;
		current = current->next;
	}
	pthread_mutex_unlock(&raft_mutex);
	
	/* Clean up peers */
	peer = shard->peers;
	while (peer != NULL) {
		next_peer = peer->next;
		if (peer->host) {
			free(peer->host);
		}
		free(peer);
		peer = next_peer;
	}
	
	/* Clean up log entries */
	entry = shard->log_head;
	while (entry != NULL) {
		next_entry = entry->next;
		if (entry->data) {
			free(entry->data);
		}
		free(entry);
		entry = next_entry;
	}
	
	free(shard);
}

/* Check if local node is leader for shard */
int raft_is_leader(uint32_t shard_id) {
	raft_shard_t *shard;
	int is_leader;
	
	pthread_mutex_lock(&raft_mutex);
	shard = find_shard(shard_id);
	is_leader = (shard != NULL && shard->state == RAFT_STATE_LEADER);
	pthread_mutex_unlock(&raft_mutex);
	
	return is_leader;
}

/* Get current leader for shard */
uint32_t raft_get_leader(uint32_t shard_id) {
	raft_shard_t *shard;
	uint32_t leader = 0;
	hlc_timestamp_t now_hlc;
	
	pthread_mutex_lock(&raft_mutex);
	shard = find_shard(shard_id);
	if (shard != NULL) {
		if (shard->state == RAFT_STATE_LEADER) {
			/* We are the leader - check if lease is still valid */
			hlc_init(&now_hlc);
			if (hlc_compare(&now_hlc, &shard->leader_lease_hlc) < 0) {
				leader = local_node_id;
			}
		} else if (shard->current_leader > 0) {
			/* Return known leader */
			leader = shard->current_leader;
		}
	}
	pthread_mutex_unlock(&raft_mutex);
	
	return leader;
}

/* Start election for shard */
void raft_start_election(raft_shard_t *shard) {
	uint8_t msg[64];
	uint8_t *ptr;
	uint32_t msg_size;
	
	if (shard == NULL) {
		return;
	}
	
	/* Transition to candidate */
	shard->state = RAFT_STATE_CANDIDATE;
	shard->current_term++;
	shard->voted_for = local_node_id;
	shard->votes_received = 1; /* Vote for self */
	shard->election_start = monotonic_useconds() / 1000;
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "raft_start_election: starting election for shard %"PRIu32" term %"PRIu64, 
	       shard->shard_id, shard->current_term);
	
	/* Build RequestVote message */
	ptr = msg;
	put8bit(&ptr, RAFT_MSG_REQUEST_VOTE);
	put32bit(&ptr, shard->shard_id);
	put64bit(&ptr, shard->current_term);
	put32bit(&ptr, local_node_id);
	put64bit(&ptr, shard->log_count);
	put64bit(&ptr, shard->log_tail ? shard->log_tail->term : 0);
	msg_size = ptr - msg;
	
	/* Send RequestVote to all peers */
	if (total_nodes > 1) {
		haconn_send_raft_broadcast(msg, msg_size);
	}
	
	/* Check if we already have majority (single node cluster) */
	if (total_nodes == 1 || shard->votes_received > total_nodes / 2) {
		raft_become_leader(shard);
	}
}

/* Send heartbeats to followers */
void raft_send_heartbeats(raft_shard_t *shard) {
	raft_peer_t *peer;
	raft_message_t msg;
	
	if (shard == NULL || shard->state != RAFT_STATE_LEADER) {
		return;
	}
	
	/* Send AppendEntries (heartbeat) to all peers */
	msg.type = RAFT_MSG_APPEND_ENTRIES;
	msg.shard_id = shard->shard_id;
	msg.data.append_entries.term = shard->current_term;
	msg.data.append_entries.leader_id = local_node_id;
	msg.data.append_entries.prev_log_index = shard->log_count;
	msg.data.append_entries.prev_log_term = (shard->log_tail ? shard->log_tail->term : 0);
	msg.data.append_entries.entry_count = 0;
	msg.data.append_entries.entries = NULL;
	msg.data.append_entries.leader_commit = shard->commit_index;
	
	peer = shard->peers;
	while (peer != NULL) {
		raft_send_message(&msg, peer->node_id);
		peer = peer->next;
	}
	
	shard->last_heartbeat = monotonic_useconds() / 1000;
}

/* Append entry to Raft log */
int raft_append_entry(uint32_t shard_id, uint32_t type, const void *data, uint32_t data_size) {
	raft_shard_t *shard;
	raft_log_entry_t *entry;
	int result = -1;
	
	pthread_mutex_lock(&raft_mutex);
	
	shard = find_shard(shard_id);
	if (shard == NULL || shard->state != RAFT_STATE_LEADER) {
		pthread_mutex_unlock(&raft_mutex);
		return -1;
	}
	
	entry = malloc(sizeof(raft_log_entry_t));
	if (entry != NULL) {
		entry->term = shard->current_term;
		entry->index = shard->log_count + 1;
		entry->type = type;
		entry->shard_id = shard_id;
		entry->data_size = data_size;
		entry->next = NULL;
		
		if (data_size > 0 && data != NULL) {
			entry->data = malloc(data_size);
			if (entry->data != NULL) {
				memcpy(entry->data, data, data_size);
			} else {
				free(entry);
				entry = NULL;
			}
		} else {
			entry->data = NULL;
		}
		
		if (entry != NULL) {
			/* Add to log */
			if (shard->log_tail != NULL) {
				shard->log_tail->next = entry;
			} else {
				shard->log_head = entry;
			}
			shard->log_tail = entry;
			shard->log_count++;
			result = 0;
		}
	}
	
	pthread_mutex_unlock(&raft_mutex);
	
	return result;
}

/* Periodic Raft maintenance */
void raft_tick(void) {
	raft_shard_t *shard;
	uint64_t current_time;
	hlc_timestamp_t now_hlc;
	
	current_time = monotonic_useconds() / 1000;
	
	/* Update global HLC */
	hlc_init(&now_hlc);
	hlc_update(&global_hlc, &now_hlc);
	
	pthread_mutex_lock(&raft_mutex);
	
	shard = shards;
	while (shard != NULL) {
		switch (shard->state) {
			case RAFT_STATE_FOLLOWER:
				/* Check for election timeout */
				if (current_time - shard->last_heartbeat > shard->election_timeout) {
					raft_start_election(shard);
				}
				break;
				
			case RAFT_STATE_CANDIDATE:
				/* Check for election timeout */
				if (current_time - shard->election_start > shard->election_timeout) {
					raft_start_election(shard); /* Restart election */
				}
				break;
				
			case RAFT_STATE_LEADER:
				/* Check if leader lease is still valid */
				if (hlc_compare(&now_hlc, &shard->leader_lease_hlc) >= 0) {
					/* Lease expired, step down */
					mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "raft_tick: leader lease expired for shard %"PRIu32, shard->shard_id);
					shard->state = RAFT_STATE_FOLLOWER;
					shard->current_leader = 0;
					shard->voted_for = 0;
				} else {
					/* Send heartbeats */
					if (current_time - shard->last_heartbeat > shard->heartbeat_timeout) {
						raft_send_heartbeats(shard);
					}
				}
				break;
		}
		
		shard = shard->next;
	}
	
	pthread_mutex_unlock(&raft_mutex);
}

/* Get Raft state for shard */
raft_state_t raft_get_state(uint32_t shard_id) {
	raft_shard_t *shard;
	raft_state_t state = RAFT_STATE_FOLLOWER;
	
	pthread_mutex_lock(&raft_mutex);
	shard = find_shard(shard_id);
	if (shard != NULL) {
		state = shard->state;
	}
	pthread_mutex_unlock(&raft_mutex);
	
	return state;
}

/* Placeholder message handling - would integrate with network layer */
int raft_handle_message(const raft_message_t *msg, uint32_t from_node) {
	/* TODO: Implement full Raft message handling */
	return 0;
}

int raft_send_message(const raft_message_t *msg, uint32_t to_node) {
	/* TODO: Implement message sending via network layer */
	return 0;
}

/* Handle incoming Raft message */
void raft_handle_incoming_message(uint32_t from_node, const uint8_t *data, uint32_t length) {
	const uint8_t *ptr = data;
	uint8_t msg_type;
	uint32_t shard_id;
	uint64_t term, log_index, log_term;
	uint32_t candidate_id;
	raft_shard_t *shard;
	uint8_t response[64];
	uint8_t *rptr;
	uint32_t response_size;
	
	if (length < 5) {
		return;
	}
	
	/* Parse message header */
	msg_type = get8bit(&ptr);
	shard_id = get32bit(&ptr);
	
	pthread_mutex_lock(&raft_mutex);
	
	shard = find_shard(shard_id);
	if (shard == NULL) {
		pthread_mutex_unlock(&raft_mutex);
		return;
	}
	
	switch (msg_type) {
		case RAFT_MSG_REQUEST_VOTE:
			if (length >= 33) { /* 1 + 4 + 8 + 4 + 8 + 8 */
				term = get64bit(&ptr);
				candidate_id = get32bit(&ptr);
				log_index = get64bit(&ptr);
				log_term = get64bit(&ptr);
				
				/* Update HLC from remote */
				hlc_update(&global_hlc, NULL);
				
				/* Check if we should grant vote */
				int vote_granted = 0;
				if (term > shard->current_term) {
					/* New term, update and reset vote */
					shard->current_term = term;
					shard->voted_for = 0;
					shard->state = RAFT_STATE_FOLLOWER;
					shard->current_leader = 0;
				}
				
				if (term == shard->current_term && 
				    (shard->voted_for == 0 || shard->voted_for == candidate_id) &&
				    log_term >= (shard->log_tail ? shard->log_tail->term : 0) &&
				    log_index >= shard->log_count) {
					/* Grant vote */
					shard->voted_for = candidate_id;
					vote_granted = 1;
					shard->last_heartbeat = monotonic_useconds() / 1000;
				}
				
				/* Send response */
				rptr = response;
				put8bit(&rptr, RAFT_MSG_REQUEST_VOTE_RESPONSE);
				put32bit(&rptr, shard_id);
				put64bit(&rptr, shard->current_term);
				put8bit(&rptr, vote_granted);
				response_size = rptr - response;
				
				haconn_send_raft_response(from_node, response, response_size);
				
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "raft: vote %s for shard %u candidate %u term %"PRIu64,
				        vote_granted ? "granted" : "denied", shard_id, candidate_id, term);
			}
			break;
			
		case RAFT_MSG_REQUEST_VOTE_RESPONSE:
			if (length >= 14 && shard->state == RAFT_STATE_CANDIDATE) { /* 1 + 4 + 8 + 1 */
				term = get64bit(&ptr);
				uint8_t vote_granted = get8bit(&ptr);
				
				if (term > shard->current_term) {
					/* Newer term, step down */
					shard->current_term = term;
					shard->state = RAFT_STATE_FOLLOWER;
					shard->voted_for = 0;
					shard->current_leader = 0;
				} else if (term == shard->current_term && vote_granted) {
					/* Got a vote */
					shard->votes_received++;
					
					/* Check if we have majority */
					if (shard->votes_received > total_nodes / 2) {
						raft_become_leader(shard);
					}
				}
			}
			break;
			
		case RAFT_MSG_APPEND_ENTRIES:
			/* Heartbeat/log replication */
			if (length >= 13) { /* 1 + 4 + 8 minimum */
				term = get64bit(&ptr);
				
				/* Update HLC from leader */
				hlc_update(&global_hlc, NULL);
				
				if (term >= shard->current_term) {
					/* Valid leader */
					shard->current_term = term;
					shard->state = RAFT_STATE_FOLLOWER;
					shard->voted_for = 0;
					shard->current_leader = from_node;
					shard->last_heartbeat = monotonic_useconds() / 1000;
					
					/* TODO: Handle log entries if any */
				}
				
				/* Send response */
				rptr = response;
				put8bit(&rptr, RAFT_MSG_APPEND_ENTRIES_RESPONSE);
				put32bit(&rptr, shard_id);
				put64bit(&rptr, shard->current_term);
				put8bit(&rptr, 1); /* Success */
				response_size = rptr - response;
				
				haconn_send_raft_response(from_node, response, response_size);
			}
			break;
	}
	
	pthread_mutex_unlock(&raft_mutex);
}

/* Initialize Raft consensus module */
int raftconsensus_init(void) {
	int i;
	raft_shard_t *shard;
	char *peers_str;
	
	/* Get node ID from config */
	local_node_id = ha_get_node_id();
	if (local_node_id == 0) {
		local_node_id = cfg_getuint32("MFSHA_NODE_ID", 1);
	}
	
	/* Initialize HLC */
	hlc_init(&global_hlc);
	
	/* Get total nodes from peer config */
	peers_str = cfg_getstr("MFSHA_PEERS", "");
	if (peers_str && strlen(peers_str) > 0) {
		total_nodes = 1; /* Count self */
		char *p = peers_str;
		while (*p) {
			if (*p == ',') total_nodes++;
			p++;
		}
		free(peers_str);
	}
	
	/* Create shards 0-7 for metadata partitioning */
	for (i = 0; i < 8; i++) {
		shard = raft_create_shard(i, NULL, 0);
		if (shard == NULL) {
			mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "raftconsensus_init: failed to create shard %d", i);
			return -1;
		}
	}
	
	/* Register periodic tick */
	main_time_register(0, 50000, raftconsensus_tick_wrapper); /* 50ms intervals */
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "raftconsensus_init: initialized with node_id=%"PRIu32" total_nodes=%"PRIu32, 
	        local_node_id, total_nodes);
	return 0;
}

void raftconsensus_term(void) {
	raft_shard_t *shard, *next;
	
	pthread_mutex_lock(&raft_mutex);
	
	shard = shards;
	while (shard != NULL) {
		next = shard->next;
		raft_destroy_shard(shard);
		shard = next;
	}
	shards = NULL;
	
	pthread_mutex_unlock(&raft_mutex);
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "raftconsensus_term: terminated");
}

/* Main loop tick function - wraps raft_tick() */
void raftconsensus_tick_wrapper(void) {
	raft_tick();
}

/* Compatibility wrapper for main loop */
void raftconsensus_tick(double now) {
	(void)now; /* Parameter used for future timing optimizations */
	raft_tick();
}

/* Reload configuration */
void raftconsensus_reload(void) {
	/* TODO: Reload configuration parameters */
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "raftconsensus_reload: configuration reloaded");
}

/* Collect file descriptors for polling */
void raftconsensus_desc(struct pollfd *pdesc, uint32_t *ndesc) {
	/* TODO: Add network sockets for Raft communication */
	(void)pdesc;
	(void)ndesc;
}

/* Service events from polling */
void raftconsensus_serve(struct pollfd *pdesc) {
	/* TODO: Handle network events for Raft communication */
	(void)pdesc;
}

/* Display status information */
void raftconsensus_info(FILE *fd) {
	raft_shard_t *shard;
	int shard_count = 0;
	
	pthread_mutex_lock(&raft_mutex);
	
	fprintf(fd, "[raftconsensus status]\n");
	fprintf(fd, "local_node_id: %"PRIu32"\n", local_node_id);
	
	shard = shards;
	while (shard != NULL) {
		fprintf(fd, "shard %"PRIu32": state=%s term=%"PRIu64" peers=%"PRIu32" log_entries=%"PRIu64"\n",
		        shard->shard_id,
		        (shard->state == RAFT_STATE_LEADER) ? "LEADER" :
		        (shard->state == RAFT_STATE_CANDIDATE) ? "CANDIDATE" : "FOLLOWER",
		        shard->current_term,
		        shard->peer_count,
		        shard->log_count);
		shard = shard->next;
		shard_count++;
	}
	
	fprintf(fd, "total_shards: %d\n", shard_count);
	
	pthread_mutex_unlock(&raft_mutex);
}
