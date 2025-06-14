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

#include "crdtstore.h"
#include "cfg.h"
#include "mfslog.h"
#include "massert.h"
#include "clocks.h"
#include "datapack.h"

static crdt_store_t *main_store = NULL;
static pthread_mutex_t store_mutex = PTHREAD_MUTEX_INITIALIZER;

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
