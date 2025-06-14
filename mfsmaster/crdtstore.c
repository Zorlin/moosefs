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
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include "crdtstore.h"
#include "cfg.h"
#include "mfslog.h"
#include "massert.h"
#include "clocks.h"
#include "datapack.h"
#include "metadata.h"
#include "hamaster.h"
#include "haconn.h"

static crdt_store_t *main_store = NULL;
static pthread_mutex_t store_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Get main store instance */
crdt_store_t* crdtstore_get_main_store(void) {
	pthread_mutex_lock(&store_mutex);
	if (main_store == NULL) {
		// Initialize main store if not created yet
		main_store = crdtstore_create(ha_get_node_id(), 16384);
	}
	pthread_mutex_unlock(&store_mutex);
	return main_store;
}

/* Hash function for CRDT entries */
static inline uint32_t hash_key(uint64_t key, uint32_t table_size) {
	key ^= key >> 16;
	key *= 0x85ebca6b;
	key ^= key >> 13;
	key *= 0xc2b2ae35;
	key ^= key >> 16;
	return key % table_size;
}

/* Create a new CRDT store */
crdt_store_t* crdtstore_create(uint32_t node_id, uint32_t table_size) {
	return crdtstore_create_with_cache(node_id, table_size, 0, 0, 1, 
	                                   1024*1024*100, /* 100MB default */
	                                   NULL);
}

/* Create cache-aware CRDT store */
crdt_store_t* crdtstore_create_with_cache(uint32_t node_id, uint32_t table_size,
                                         uint32_t cache_shard_start, uint32_t cache_shard_end,
                                         uint32_t total_shards, uint64_t max_memory_bytes,
                                         const char *backing_store_path) {
	crdt_store_t *store;
	
	store = malloc(sizeof(crdt_store_t));
	if (store == NULL) {
		return NULL;
	}
	
	store->table = calloc(table_size, sizeof(crdt_entry_t*));
	if (store->table == NULL) {
		free(store);
		return NULL;
	}
	
	store->table_size = table_size;
	store->entry_count = 0;
	store->hot_entry_count = 0;
	store->node_id = node_id;
	
	/* Cache management setup */
	store->cache_shard_start = cache_shard_start;
	store->cache_shard_end = cache_shard_end;
	store->total_shards = total_shards;
	store->max_memory_bytes = max_memory_bytes;
	store->current_memory_bytes = 0;
	
	/* Backing store setup */
	if (backing_store_path) {
		store->backing_store_path = strdup(backing_store_path);
	} else {
		store->backing_store_path = NULL;
	}
	store->last_full_sync = 0;
	
	/* Initialize logical clock */
	store->clock.timestamp = monotonic_useconds() / 1000000;
	store->clock.node_id = node_id;
	store->clock.counter = 0;
	
	return store;
}

/* Destroy CRDT store */
void crdtstore_destroy(crdt_store_t *store) {
	uint32_t i;
	crdt_entry_t *entry, *next;
	
	if (store == NULL) {
		return;
	}
	
	for (i = 0; i < store->table_size; i++) {
		entry = store->table[i];
		while (entry != NULL) {
			next = entry->next;
			if (entry->value) {
				free(entry->value);
			}
			free(entry);
			entry = next;
		}
	}
	
	free(store->table);
	if (store->backing_store_path) {
		free(store->backing_store_path);
	}
	free(store);
}

/* Get current logical time */
lamport_time_t crdtstore_get_time(crdt_store_t *store) {
	lamport_time_t current;
	
	current.timestamp = monotonic_useconds() / 1000000;
	current.node_id = store->node_id;
	current.counter = ++store->clock.counter;
	
	/* Update local clock */
	if (current.timestamp > store->clock.timestamp) {
		store->clock = current;
	} else {
		store->clock.counter = current.counter;
	}
	
	return current;
}

/* Update logical clock with remote time */
void crdtstore_update_clock(crdt_store_t *store, const lamport_time_t *remote_time) {
	if (remote_time->timestamp > store->clock.timestamp ||
	    (remote_time->timestamp == store->clock.timestamp && 
	     remote_time->counter > store->clock.counter)) {
		store->clock.timestamp = remote_time->timestamp;
		store->clock.counter = remote_time->counter + 1;
	} else {
		store->clock.counter++;
	}
}

/* Compare Lamport timestamps */
static int compare_lamport_time(const lamport_time_t *a, const lamport_time_t *b) {
	if (a->timestamp != b->timestamp) {
		return (a->timestamp > b->timestamp) ? 1 : -1;
	}
	if (a->counter != b->counter) {
		return (a->counter > b->counter) ? 1 : -1;
	}
	if (a->node_id != b->node_id) {
		return (a->node_id > b->node_id) ? 1 : -1;
	}
	return 0;
}

/* Find entry in hash table */
static crdt_entry_t* find_entry(crdt_store_t *store, uint64_t key) {
	uint32_t hash = hash_key(key, store->table_size);
	crdt_entry_t *entry = store->table[hash];
	
	while (entry != NULL) {
		if (entry->key == key) {
			return entry;
		}
		entry = entry->next;
	}
	
	return NULL;
}

/* Put entry into CRDT store */
int crdtstore_put(crdt_store_t *store, uint64_t key, crdt_type_t type, 
                  const void *value, uint32_t value_size) {
	uint32_t hash;
	crdt_entry_t *entry, *new_entry;
	lamport_time_t ts;
	
	if (store == NULL || value == NULL) {
		return -1;
	}
	
	ts = crdtstore_get_time(store);
	hash = hash_key(key, store->table_size);
	entry = find_entry(store, key);
	
	if (entry != NULL) {
		/* Update existing entry if our timestamp is newer */
		if (type == CRDT_LWW_REGISTER && compare_lamport_time(&ts, &entry->ts) > 0) {
			if (entry->value) {
				free(entry->value);
			}
			entry->value = malloc(value_size);
			if (entry->value == NULL) {
				return -1;
			}
			memcpy(entry->value, value, value_size);
			entry->value_size = value_size;
			entry->ts = ts;
		} else if (type == CRDT_G_COUNTER || type == CRDT_PN_COUNTER) {
			/* For counters, merge values */
			if (entry->value_size == sizeof(uint64_t) && value_size == sizeof(uint64_t)) {
				uint64_t old_value = *((uint64_t*)entry->value);
				uint64_t new_value = *((uint64_t*)value);
				if (type == CRDT_G_COUNTER) {
					*((uint64_t*)entry->value) = (old_value > new_value) ? old_value : new_value;
				} else {
					*((uint64_t*)entry->value) = old_value + new_value;
				}
				entry->ts = ts;
			}
		}
		
		/* Broadcast the update to other nodes if HA mode is enabled */
		if (ha_mode_enabled()) {
			uint8_t *delta_data = NULL;
			uint32_t delta_size = 0;
			
			if (crdtstore_serialize_entry(entry, &delta_data, &delta_size) == 0 && delta_data != NULL) {
				haconn_send_crdt_delta(delta_data, delta_size);
				free(delta_data);
			}
		}
		
		return 0;
	}
	
	/* Create new entry */
	new_entry = malloc(sizeof(crdt_entry_t));
	if (new_entry == NULL) {
		return -1;
	}
	
	new_entry->key = key;
	new_entry->type = type;
	new_entry->ts = ts;
	new_entry->value_size = value_size;
	new_entry->value = malloc(value_size);
	if (new_entry->value == NULL) {
		free(new_entry);
		return -1;
	}
	memcpy(new_entry->value, value, value_size);
	
	/* Insert at head of bucket */
	new_entry->next = store->table[hash];
	store->table[hash] = new_entry;
	store->entry_count++;
	
	/* Broadcast the change to other nodes if HA mode is enabled */
	if (ha_mode_enabled()) {
		uint8_t *delta_data = NULL;
		uint32_t delta_size = 0;
		
		if (crdtstore_serialize_entry(new_entry, &delta_data, &delta_size) == 0 && delta_data != NULL) {
			haconn_send_crdt_delta(delta_data, delta_size);
			free(delta_data);
		}
	}
	
	return 0;
}

/* Get entry from CRDT store */
crdt_entry_t* crdtstore_get(crdt_store_t *store, uint64_t key) {
	if (store == NULL) {
		return NULL;
	}
	
	return find_entry(store, key);
}

/* Merge remote CRDT entry */
int crdtstore_merge(crdt_store_t *store, const crdt_entry_t *remote_entry) {
	crdt_entry_t *local_entry;
	
	if (store == NULL || remote_entry == NULL) {
		return -1;
	}
	
	crdtstore_update_clock(store, &remote_entry->ts);
	
	local_entry = find_entry(store, remote_entry->key);
	
	if (local_entry == NULL) {
		/* No local entry, add remote entry */
		return crdtstore_put(store, remote_entry->key, remote_entry->type,
		                     remote_entry->value, remote_entry->value_size);
	}
	
	/* Merge based on CRDT type */
	switch (remote_entry->type) {
		case CRDT_LWW_REGISTER:
			/* Last-Writer-Wins: use entry with latest timestamp */
			if (compare_lamport_time(&remote_entry->ts, &local_entry->ts) > 0) {
				if (local_entry->value) {
					free(local_entry->value);
				}
				local_entry->value = malloc(remote_entry->value_size);
				if (local_entry->value == NULL) {
					return -1;
				}
				memcpy(local_entry->value, remote_entry->value, remote_entry->value_size);
				local_entry->value_size = remote_entry->value_size;
				local_entry->ts = remote_entry->ts;
			}
			break;
			
		case CRDT_G_COUNTER:
			/* Grow-Only Counter: take maximum */
			if (local_entry->value_size == sizeof(uint64_t) && 
			    remote_entry->value_size == sizeof(uint64_t)) {
				uint64_t local_val = *((uint64_t*)local_entry->value);
				uint64_t remote_val = *((uint64_t*)remote_entry->value);
				if (remote_val > local_val) {
					*((uint64_t*)local_entry->value) = remote_val;
					local_entry->ts = remote_entry->ts;
				}
			}
			break;
			
		case CRDT_PN_COUNTER:
			/* Plus-Negative Counter: add values */
			if (local_entry->value_size == sizeof(int64_t) && 
			    remote_entry->value_size == sizeof(int64_t)) {
				int64_t local_val = *((int64_t*)local_entry->value);
				int64_t remote_val = *((int64_t*)remote_entry->value);
				*((int64_t*)local_entry->value) = local_val + remote_val;
				local_entry->ts = remote_entry->ts;
			}
			break;
			
		case CRDT_OR_SET:
			/* Observed-Remove Set: complex merge logic would go here */
			/* For now, use LWW semantics */
			if (compare_lamport_time(&remote_entry->ts, &local_entry->ts) > 0) {
				if (local_entry->value) {
					free(local_entry->value);
				}
				local_entry->value = malloc(remote_entry->value_size);
				if (local_entry->value == NULL) {
					return -1;
				}
				memcpy(local_entry->value, remote_entry->value, remote_entry->value_size);
				local_entry->value_size = remote_entry->value_size;
				local_entry->ts = remote_entry->ts;
			}
			break;
	}
	
	return 0;
}

/* Metadata-specific operations */

int crdtstore_put_node(crdt_store_t *store, const mfs_node_t *node) {
	return crdtstore_put(store, node->inode, CRDT_LWW_REGISTER, 
	                     node, sizeof(mfs_node_t));
}

int crdtstore_get_node(crdt_store_t *store, uint32_t inode, mfs_node_t *node) {
	crdt_entry_t *entry = crdtstore_get(store, inode);
	
	if (entry == NULL || entry->type != CRDT_LWW_REGISTER || 
	    entry->value_size != sizeof(mfs_node_t)) {
		return -1;
	}
	
	memcpy(node, entry->value, sizeof(mfs_node_t));
	return 0;
}

/* Global store management */
int crdtstore_init(void) {
	char *node_id_str;
	uint32_t node_id = 1;
	uint32_t table_size = 65536;
	
	node_id_str = cfg_getstr("HA_NODE_ID", "");
	if (strlen(node_id_str) > 0) {
		node_id = hash_key((uint64_t)node_id_str, 0xFFFFFFFF);
	}
	
	main_store = crdtstore_create(node_id, table_size);
	if (main_store == NULL) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "crdtstore_init: failed to create main store");
		return -1;
	}
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "crdtstore_init: initialized with node_id=%"PRIu32, node_id);
	return 0;
}

void crdtstore_term(void) {
	pthread_mutex_lock(&store_mutex);
	if (main_store) {
		crdtstore_destroy(main_store);
		main_store = NULL;
	}
	pthread_mutex_unlock(&store_mutex);
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "crdtstore_term: terminated");
}

/* Get statistics */
void crdtstore_get_stats(crdt_store_t *store, uint32_t *entries, uint32_t *memory_usage) {
	uint32_t i, mem_usage = 0;
	crdt_entry_t *entry;
	
	if (store == NULL) {
		*entries = 0;
		*memory_usage = 0;
		return;
	}
	
	*entries = store->entry_count;
	
	mem_usage = sizeof(crdt_store_t) + (store->table_size * sizeof(crdt_entry_t*));
	
	for (i = 0; i < store->table_size; i++) {
		entry = store->table[i];
		while (entry != NULL) {
			mem_usage += sizeof(crdt_entry_t) + entry->value_size;
			entry = entry->next;
		}
	}
	
	*memory_usage = mem_usage;
}

/* Cache-aware operations */

/* Get which node should preferentially cache this key */
uint32_t crdtstore_get_preferred_node(uint64_t key, uint32_t total_shards) {
	return hash_key(key, total_shards);
}

/* Get cache tier for a key */
cache_tier_t crdtstore_get_cache_tier(crdt_store_t *store, uint64_t key) {
	uint32_t preferred_shard;
	crdt_entry_t *entry;
	
	if (store == NULL) {
		return CACHE_TIER_COLD;
	}
	
	/* Check if it's in memory */
	entry = find_entry(store, key);
	if (entry == NULL) {
		return CACHE_TIER_COLD; /* Not in memory */
	}
	
	/* Check if this is our preferred shard (HOT) */
	preferred_shard = crdtstore_get_preferred_node(key, store->total_shards);
	if (preferred_shard >= store->cache_shard_start && preferred_shard < store->cache_shard_end) {
		return CACHE_TIER_HOT;
	} else {
		return CACHE_TIER_WARM;
	}
}

/* Ensure key is cached in memory */
int crdtstore_ensure_cached(crdt_store_t *store, uint64_t key) {
	crdt_entry_t *entry;
	
	if (store == NULL) {
		return -1;
	}
	
	/* Check if already in memory */
	entry = find_entry(store, key);
	if (entry != NULL) {
		return 0; /* Already cached */
	}
	
	/* TODO: Load from backing store (metadata.mfs) */
	/* For now, return success indicating it would be loaded */
	return 0;
}

/* Evict key from cache to save memory */
int crdtstore_evict_from_cache(crdt_store_t *store, uint64_t key) {
	uint32_t hash;
	crdt_entry_t *entry, *prev;
	
	if (store == NULL) {
		return -1;
	}
	
	hash = hash_key(key, store->table_size);
	entry = store->table[hash];
	prev = NULL;
	
	while (entry != NULL) {
		if (entry->key == key) {
			/* TODO: Save to backing store if modified */
			
			/* Remove from hash table */
			if (prev != NULL) {
				prev->next = entry->next;
			} else {
				store->table[hash] = entry->next;
			}
			
			/* Update memory usage */
			store->current_memory_bytes -= sizeof(crdt_entry_t) + entry->value_size;
			store->entry_count--;
			
			/* Check if it was a HOT entry */
			if (crdtstore_get_cache_tier(store, key) == CACHE_TIER_HOT) {
				store->hot_entry_count--;
			}
			
			/* Free memory */
			if (entry->value) {
				free(entry->value);
			}
			free(entry);
			
			return 0;
		}
		prev = entry;
		entry = entry->next;
	}
	
	return -1; /* Not found */
}

/* Promote key to HOT tier (high priority caching) */
int crdtstore_promote_to_hot(crdt_store_t *store, uint64_t key) {
	cache_tier_t current_tier;
	
	if (store == NULL) {
		return -1;
	}
	
	/* Ensure it's cached first */
	if (crdtstore_ensure_cached(store, key) != 0) {
		return -1;
	}
	
	current_tier = crdtstore_get_cache_tier(store, key);
	if (current_tier == CACHE_TIER_HOT) {
		return 0; /* Already HOT */
	}
	
	/* TODO: Implement promotion logic (move to high-priority cache area) */
	/* For now, just increment hot count if it became hot */
	if (current_tier == CACHE_TIER_WARM) {
		store->hot_entry_count++;
	}
	
	return 0;
}

/* Periodic CRDT store maintenance */
void crdtstore_tick(double now) {
	static double last_cleanup_time = 0.0;
	static double last_sync_time = 0.0;
	const double cleanup_interval = 300.0; /* 5 minutes */
	const double sync_interval = 60.0; /* 1 minute */
	
	if (main_store == NULL) {
		return;
	}
	
	pthread_mutex_lock(&store_mutex);
	
	/* Periodic cache cleanup */
	if (now - last_cleanup_time >= cleanup_interval) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "crdtstore_tick: performing cache cleanup");
		/* TODO: Implement cache eviction for memory management */
		/* TODO: Remove expired entries */
		/* TODO: Compact hash table if needed */
		last_cleanup_time = now;
	}
	
	/* Periodic disk synchronization */
	if (now - last_sync_time >= sync_interval) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "crdtstore_tick: checking disk sync");
		/* TODO: Sync dirty entries to backing store */
		/* TODO: Update metadata.mfs file */
		main_store->last_full_sync = (time_t)now;
		last_sync_time = now;
	}
	
	/* Update logical clock */
	main_store->clock.timestamp = (uint64_t)(now * 1000000); /* Convert to microseconds */
	main_store->clock.counter++;
	
	pthread_mutex_unlock(&store_mutex);
}

/* Reload CRDT store configuration */
void crdtstore_reload(void) {
	uint64_t old_max_memory;
	
	if (main_store == NULL) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "crdtstore_reload: no store to reload");
		return;
	}
	
	pthread_mutex_lock(&store_mutex);
	
	old_max_memory = main_store->max_memory_bytes;
	main_store->max_memory_bytes = cfg_getuint64("HA_CRDT_MAX_MEMORY", 1024*1024*1024); /* 1GB default */
	
	if (old_max_memory != main_store->max_memory_bytes) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "crdtstore_reload: max memory changed from %"PRIu64" to %"PRIu64" bytes",
		        old_max_memory, main_store->max_memory_bytes);
		/* TODO: Handle memory limit changes - may need to evict cache */
	} else {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "crdtstore_reload: configuration reloaded");
	}
	
	pthread_mutex_unlock(&store_mutex);
}

/* Display CRDT store status information */
void crdtstore_info(FILE *fd) {
	if (main_store == NULL) {
		fprintf(fd, "[crdtstore status]\n");
		fprintf(fd, "status: not initialized\n");
		return;
	}
	
	pthread_mutex_lock(&store_mutex);
	
	fprintf(fd, "[crdtstore status]\n");
	fprintf(fd, "node_id: %"PRIu32"\n", main_store->node_id);
	fprintf(fd, "table_size: %"PRIu32"\n", main_store->table_size);
	fprintf(fd, "entry_count: %"PRIu32"\n", main_store->entry_count);
	fprintf(fd, "hot_entry_count: %"PRIu32"\n", main_store->hot_entry_count);
	fprintf(fd, "current_memory: %"PRIu64" bytes\n", main_store->current_memory_bytes);
	fprintf(fd, "max_memory: %"PRIu64" bytes\n", main_store->max_memory_bytes);
	fprintf(fd, "memory_usage: %.1f%%\n", 
	        main_store->max_memory_bytes > 0 ? 
	        (100.0 * main_store->current_memory_bytes) / main_store->max_memory_bytes : 0.0);
	fprintf(fd, "cache_shard: %"PRIu32"-%"PRIu32" of %"PRIu32"\n", 
	        main_store->cache_shard_start, main_store->cache_shard_end, main_store->total_shards);
	fprintf(fd, "logical_clock: %"PRIu64".%"PRIu32"\n", 
	        main_store->clock.timestamp, main_store->clock.counter);
	fprintf(fd, "last_full_sync: %ld\n", (long)main_store->last_full_sync);
	
	/* TODO: Add more detailed statistics */
	/* TODO: Show cache hit/miss ratios */
	/* TODO: Display CRDT conflict resolution stats */
	
	pthread_mutex_unlock(&store_mutex);
}

/* Forward declaration */
static int try_sync_from_peer(const char *host, int port);

/* Attempt to sync from existing cluster members when bootstrapping */
int crdt_cluster_sync_attempt(void) {
	char *ha_peers_env;
	char *ha_node_id_env;
	char *peers_copy, *peer, *saveptr;
	int attempts_made = 0;
	int successful_peers = 0;
	
	mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, "attempting to sync from existing cluster members...");
	
	/* Check if HA mode is configured */
	ha_node_id_env = getenv("MFSHA_NODE_ID");
	ha_peers_env = getenv("MFSHA_PEERS");
	
	if (ha_node_id_env == NULL || ha_peers_env == NULL) {
		mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_NOTICE, "HA mode not configured (MFSHA_NODE_ID or MFSHA_PEERS not set) - starting with empty metadata");
		return -1;
	}
	
	mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, "HA mode detected - node_id: %s, peers: %s", ha_node_id_env, ha_peers_env);
	
	/* Parse peers and attempt connections */
	peers_copy = strdup(ha_peers_env);
	if (peers_copy == NULL) {
		mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_ERR, "memory allocation failed during cluster sync");
		return -1;
	}
	
	peer = strtok_r(peers_copy, ",", &saveptr);
	while (peer != NULL) {
		char *colon_pos;
		char *host;
		int port = 9421; /* Default MooseFS master port */
		
		attempts_made++;
		
		/* Parse host:port */
		colon_pos = strchr(peer, ':');
		if (colon_pos != NULL) {
			*colon_pos = '\0';
			port = atoi(colon_pos + 1);
		}
		host = peer;
		
		mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, "attempting to sync from peer: %s:%d", host, port);
		
		/* Try to connect and download metadata */
		if (try_sync_from_peer(host, port) == 0) {
			successful_peers++;
			mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, "successfully synced from peer: %s:%d", host, port);
			break; /* One successful sync is enough */
		} else {
			mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_WARNING, "failed to sync from peer: %s:%d", host, port);
		}
		
		peer = strtok_r(NULL, ",", &saveptr);
	}
	
	free(peers_copy);
	
	if (successful_peers > 0) {
		mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, "cluster sync completed successfully (%d/%d peers)", successful_peers, attempts_made);
		return 0;
	} else {
		mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_WARNING, "cluster sync failed - no peers available (%d attempts)", attempts_made);
		return -1;
	}
}

/* Helper function to attempt sync from a specific peer */
static int try_sync_from_peer(const char *host, int port) {
	int sock;
	struct sockaddr_in addr;
	struct hostent *he;
	int timeout = 10; /* 10 second timeout */
	uint8_t packet[24];
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t cmd, length, version;
	int result = -1;
	
	/* Create socket */
	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "socket creation failed: %s", strerror(errno));
		return -1;
	}
	
	/* Set timeout */
	struct timeval tv;
	tv.tv_sec = timeout;
	tv.tv_usec = 0;
	setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
	setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof(tv));
	
	/* Resolve hostname */
	he = gethostbyname(host);
	if (he == NULL) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "hostname resolution failed for %s", host);
		close(sock);
		return -1;
	}
	
	/* Connect to peer */
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	memcpy(&addr.sin_addr, he->h_addr, he->h_length);
	
	if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "connection to %s:%d failed: %s", host, port, strerror(errno));
		close(sock);
		return -1;
	}
	
	mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, "connected to peer %s:%d, performing HA cluster handshake", host, port);
	
	/* Send HA cluster metadata request using MooseFS protocol */
	wptr = packet;
	put32bit(&wptr, CLTOMA_HA_CLUSTER_INFO); /* Use existing HA protocol message */
	put32bit(&wptr, 8); /* length */
	put32bit(&wptr, 0x12345678); /* magic for metadata request */
	put32bit(&wptr, 1); /* request type: metadata download */
	
	if (send(sock, packet, 16, 0) != 16) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "failed to send metadata request to %s:%d", host, port);
		close(sock);
		return -1;
	}
	
	/* Read response header */
	if (recv(sock, packet, 8, MSG_WAITALL) != 8) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "failed to receive response header from %s:%d", host, port);
		close(sock);
		return -1;
	}
	
	rptr = packet;
	cmd = get32bit(&rptr);
	length = get32bit(&rptr);
	
	/* Check if peer supports HA metadata transfer */
	if (cmd == MATOCL_HA_CLUSTER_INFO && length >= 4) {
		/* Read response data */
		if (recv(sock, packet, 4, MSG_WAITALL) != 4) {
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "failed to receive response data from %s:%d", host, port);
			close(sock);
			return -1;
		}
		
		rptr = packet;
		version = get32bit(&rptr);
		
		if (version == 0x12345678) {
			mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, "peer %s:%d supports HA metadata transfer - downloading", host, port);
			
			/* Now use the established connection for metadata download */
			result = meta_downloadall(sock);
			
			if (result > 0) {
				mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, "metadata downloaded successfully from %s:%d", host, port);
				close(sock);
				return 0;
			}
		} else {
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "peer %s:%d does not support HA metadata transfer (version mismatch)", host, port);
		}
	} else {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "peer %s:%d does not support HA cluster protocol", host, port);
	}
	
	close(sock);
	mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_WARNING, "metadata download failed from %s:%d", host, port);
	return -1;
}

/* Serialization for Raft log and ring deltas */
int crdtstore_serialize_entry(const crdt_entry_t *entry, uint8_t **data, uint32_t *size) {
	uint8_t *ptr;
	uint32_t total_size;
	
	if (entry == NULL || data == NULL || size == NULL) {
		return -1;
	}
	
	/* Calculate total size */
	total_size = 8 + 4 + 8 + 4 + 4 + 4 + entry->value_size;  /* key + type + timestamp + node_id + counter + value_size + value */
	
	*data = malloc(total_size);
	if (*data == NULL) {
		return -1;
	}
	
	ptr = *data;
	
	/* Serialize fields */
	put64bit(&ptr, entry->key);
	put32bit(&ptr, entry->type);
	put64bit(&ptr, entry->ts.timestamp);
	put32bit(&ptr, entry->ts.node_id);
	put32bit(&ptr, entry->ts.counter);
	put32bit(&ptr, entry->value_size);
	memcpy(ptr, entry->value, entry->value_size);
	
	*size = total_size;
	return 0;
}

int crdtstore_deserialize_entry(const uint8_t *data, uint32_t size, crdt_entry_t **entry) {
	const uint8_t *ptr = data;
	crdt_entry_t *new_entry;
	uint32_t value_size;
	
	if (data == NULL || entry == NULL || size < 32) {
		return -1;
	}
	
	new_entry = malloc(sizeof(crdt_entry_t));
	if (new_entry == NULL) {
		return -1;
	}
	
	/* Deserialize fields */
	new_entry->key = get64bit(&ptr);
	new_entry->type = get32bit(&ptr);
	new_entry->ts.timestamp = get64bit(&ptr);
	new_entry->ts.node_id = get32bit(&ptr);
	new_entry->ts.counter = get32bit(&ptr);
	value_size = get32bit(&ptr);
	
	if (size < 32 + value_size) {
		free(new_entry);
		return -1;
	}
	
	new_entry->value_size = value_size;
	new_entry->value = malloc(value_size);
	if (new_entry->value == NULL) {
		free(new_entry);
		return -1;
	}
	
	memcpy(new_entry->value, ptr, value_size);
	new_entry->next = NULL;
	
	*entry = new_entry;
	return 0;
}

/* Chunkserver operations */

/* Generate key for chunkserver entry */
static uint64_t chunkserver_key(uint32_t servip, uint16_t servport) {
	return ((uint64_t)servip << 16) | servport;
}

/* Put chunkserver information into CRDT store */
int crdtstore_put_chunkserver(crdt_store_t *store, const mfs_chunkserver_t *cs) {
	uint64_t key = chunkserver_key(cs->servip, cs->servport);
	return crdtstore_put(store, key, CRDT_LWW_REGISTER, cs, sizeof(mfs_chunkserver_t));
}

/* Get chunkserver information from CRDT store */
int crdtstore_get_chunkserver(crdt_store_t *store, uint32_t servip, uint16_t servport, mfs_chunkserver_t *cs) {
	uint64_t key = chunkserver_key(servip, servport);
	crdt_entry_t *entry;
	
	entry = crdtstore_get(store, key);
	if (entry == NULL || entry->type != CRDT_LWW_REGISTER || entry->value_size != sizeof(mfs_chunkserver_t)) {
		return -1;
	}
	
	memcpy(cs, entry->value, sizeof(mfs_chunkserver_t));
	return 0;
}

/* Remove chunkserver from CRDT store (mark as unregistered) */
int crdtstore_remove_chunkserver(crdt_store_t *store, uint32_t servip, uint16_t servport) {
	mfs_chunkserver_t cs;
	uint64_t key = chunkserver_key(servip, servport);
	crdt_entry_t *entry;
	
	/* Get existing entry */
	entry = crdtstore_get(store, key);
	if (entry == NULL || entry->type != CRDT_LWW_REGISTER || entry->value_size != sizeof(mfs_chunkserver_t)) {
		/* Create new unregistered entry */
		memset(&cs, 0, sizeof(cs));
		cs.servip = servip;
		cs.servport = servport;
		cs.registered = 0;
	} else {
		/* Mark existing as unregistered */
		memcpy(&cs, entry->value, sizeof(mfs_chunkserver_t));
		cs.registered = 0;
	}
	
	return crdtstore_put_chunkserver(store, &cs);
}

/* Get all registered chunkservers from CRDT store */
int crdtstore_get_all_chunkservers(crdt_store_t *store, mfs_chunkserver_t **cs_array, uint32_t *count) {
	uint32_t i, registered_count = 0;
	crdt_entry_t *entry;
	mfs_chunkserver_t *result;
	uint32_t allocated = 0;
	
	*cs_array = NULL;
	*count = 0;
	
	/* First pass: count registered chunkservers */
	for (i = 0; i < store->table_size; i++) {
		for (entry = store->table[i]; entry != NULL; entry = entry->next) {
			if (entry->type == CRDT_LWW_REGISTER && entry->value_size == sizeof(mfs_chunkserver_t)) {
				mfs_chunkserver_t *cs = (mfs_chunkserver_t *)entry->value;
				if (cs->registered) {
					registered_count++;
				}
			}
		}
	}
	
	if (registered_count == 0) {
		return 0;
	}
	
	/* Allocate result array */
	result = malloc(registered_count * sizeof(mfs_chunkserver_t));
	if (result == NULL) {
		return -1;
	}
	
	/* Second pass: copy registered chunkservers */
	for (i = 0; i < store->table_size && allocated < registered_count; i++) {
		for (entry = store->table[i]; entry != NULL && allocated < registered_count; entry = entry->next) {
			if (entry->type == CRDT_LWW_REGISTER && entry->value_size == sizeof(mfs_chunkserver_t)) {
				mfs_chunkserver_t *cs = (mfs_chunkserver_t *)entry->value;
				if (cs->registered) {
					memcpy(&result[allocated], cs, sizeof(mfs_chunkserver_t));
					allocated++;
				}
			}
		}
	}
	
	*cs_array = result;
	*count = allocated;
	return 0;
}
