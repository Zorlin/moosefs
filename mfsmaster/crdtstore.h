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

#ifndef _CRDTSTORE_H_
#define _CRDTSTORE_H_

#include <inttypes.h>
#include <stdio.h>
#include <pthread.h>

/* CRDT Store - manages metadata using Conflict-free Replicated Data Types */

/* CRDT Types */
typedef enum {
	CRDT_LWW_REGISTER,    /* Last-Writer-Wins register */
	CRDT_G_COUNTER,       /* Grow-Only counter */
	CRDT_PN_COUNTER,      /* Plus-Negative counter */
	CRDT_OR_SET           /* Observed-Remove set */
} crdt_type_t;

/* Lamport timestamp for LWW ordering */
typedef struct {
	uint64_t timestamp;   /* Wall clock time */
	uint32_t node_id;     /* Node identifier for tie-breaking */
	uint32_t counter;     /* Local counter for ordering */
} lamport_time_t;

/* CRDT Entry */
typedef struct crdt_entry {
	uint64_t key;         /* Entry key (inode_id for filesystem objects) */
	crdt_type_t type;     /* CRDT type */
	lamport_time_t ts;    /* Lamport timestamp */
	uint32_t value_size;  /* Size of value data */
	uint8_t *value;       /* Serialized value data */
	struct crdt_entry *next;
} crdt_entry_t;

/* Cache tier for metadata */
typedef enum {
	CACHE_TIER_HOT,       /* Actively cached in RAM (local shard) */
	CACHE_TIER_WARM,      /* Available but not cached (can be loaded) */
	CACHE_TIER_COLD       /* On disk only (requires disk I/O) */
} cache_tier_t;

/* CRDT Store with cache-aware architecture */
typedef struct {
	crdt_entry_t **table; /* Hash table of CRDT entries */
	uint32_t table_size;  /* Size of hash table */
	uint32_t entry_count; /* Number of entries in memory */
	uint32_t hot_entry_count; /* Number of HOT entries */
	lamport_time_t clock; /* Local logical clock */
	uint32_t node_id;     /* This node's identifier */
	
	/* Cache management */
	uint32_t cache_shard_start; /* Start of this node's cache shard */
	uint32_t cache_shard_end;   /* End of this node's cache shard */
	uint32_t total_shards;      /* Total number of cache shards */
	uint64_t max_memory_bytes;  /* Maximum memory for cache */
	uint64_t current_memory_bytes; /* Current memory usage */
	
	/* Disk backing store (metadata.mfs compatible) */
	char *backing_store_path;   /* Path to metadata.mfs file */
	uint64_t last_full_sync;    /* Last time we did full disk sync */
} crdt_store_t;

/* Metadata-specific CRDT operations */

/* Node (inode) operations */
typedef struct {
	uint32_t inode;
	uint8_t type;         /* File type (from metadata.mfs NODE section) */
	uint32_t storage_class;
	uint32_t flags;
	uint16_t mode;
	uint32_t uid;
	uint32_t gid;
	uint32_t atime;
	uint32_t mtime;
	uint32_t ctime;
	uint32_t nlink;       /* G-Counter for link count */
	uint64_t length;
	uint32_t chunks;
} mfs_node_t;

/* Chunkserver operations */
typedef struct {
	uint32_t servip;
	uint16_t servport;
	uint16_t csid;
	uint64_t usedspace;
	uint64_t totalspace;
	uint32_t chunkscount;
	uint64_t todelusedspace;
	uint64_t todeltotalspace;
	uint32_t todelchunkscount;
	uint32_t version;
	uint16_t timeout;
	uint8_t registered;   /* 0=unregistered, 1=registered */
} mfs_chunkserver_t;

/* Hybrid Logical Clock (HLC) for distributed timestamps
 * Combines physical time with logical counter for causality
 */
typedef struct {
	uint64_t physical_time;   /* Wall clock time in microseconds */
	uint32_t logical_counter; /* Logical counter for same physical time */
} hlc_timestamp_t;

/* Global version using HLCs, vector clocks and leader leases
 * 
 * Version Management Strategy:
 * 1. Leader leases ensure only one leader per shard can assign versions
 * 2. Each shard uses its Raft log index as the version component
 * 3. HLC provides global time ordering with causality
 * 4. Vector clock tracks all shard versions for conflict resolution
 * 5. Monotonic version computed for client compatibility
 *
 * This ensures:
 * - Strong consistency within shards via leader leases
 * - Causal consistency across shards via HLC
 * - Deterministic conflict resolution via vector clocks
 * - Backward compatibility with existing clients/chunkservers
 */
typedef struct {
	uint32_t shard_id;        /* Shard that assigned this version */
	uint64_t raft_index;      /* Position in shard's Raft log */
	uint32_t leader_node;     /* Leader that assigned (has valid lease) */
	hlc_timestamp_t hlc;      /* Hybrid logical clock timestamp */
	uint64_t lease_epoch;     /* Leader lease epoch for validity */
} shard_version_t;

typedef struct {
	uint32_t shard_count;                    /* Number of shards */
	uint64_t *shard_versions;                /* Array: ShardID -> Latest RaftIndex */
	hlc_timestamp_t hlc;                     /* Global HLC timestamp */
	uint64_t *lease_epochs;                  /* Array: ShardID -> Current lease epoch */
} global_version_t;

/* Version mapper for client compatibility */
typedef struct version_mapping {
	uint64_t monotonic;                      /* Client-visible version */
	global_version_t vector;                 /* Internal vector clock */
	struct version_mapping *next;
} version_mapping_t;

typedef struct {
	version_mapping_t **table;               /* Hash table of mappings */
	uint32_t table_size;
	uint64_t next_monotonic;                 /* Next monotonic version to assign */
	pthread_mutex_t lock;
} version_mapper_t;

/* Chunk operations */
typedef struct {
	uint64_t chunkid;
	global_version_t version;          /* Global vector clock version */
	uint32_t storage_class;
	uint8_t locked;
	uint8_t archflag;
	uint64_t lockedto;               /* Lock expiration timestamp */
} mfs_chunk_crdt_t;

/* Edge (directory entry) operations */
typedef struct {
	uint32_t parent_inode;
	uint32_t child_inode;
	uint16_t name_len;
	char name[];          /* Variable length name */
} mfs_edge_t;

/* Chunk operations */
typedef struct {
	uint64_t chunkid;
	uint32_t version;
	uint32_t storage_class;
	uint8_t archive_flag;
} mfs_chunk_t;

/* CRDT Store API */
int crdtstore_init(void);
void crdtstore_term(void);
void crdtstore_tick(double now);
void crdtstore_reload(void);
void crdtstore_info(FILE *fd);

/* Cache-aware store creation */
crdt_store_t* crdtstore_create(uint32_t node_id, uint32_t table_size);
crdt_store_t* crdtstore_create_with_cache(uint32_t node_id, uint32_t table_size,
                                         uint32_t cache_shard_start, uint32_t cache_shard_end,
                                         uint32_t total_shards, uint64_t max_memory_bytes,
                                         const char *backing_store_path);
void crdtstore_destroy(crdt_store_t *store);

/* Get main store instance */
crdt_store_t* crdtstore_get_main_store(void);

/* Entry management */
int crdtstore_put(crdt_store_t *store, uint64_t key, crdt_type_t type, 
                  const void *value, uint32_t value_size);
crdt_entry_t* crdtstore_get(crdt_store_t *store, uint64_t key);
int crdtstore_merge(crdt_store_t *store, const crdt_entry_t *entry);

/* Cache-aware operations */
cache_tier_t crdtstore_get_cache_tier(crdt_store_t *store, uint64_t key);
int crdtstore_ensure_cached(crdt_store_t *store, uint64_t key); /* Load from disk if needed */
int crdtstore_evict_from_cache(crdt_store_t *store, uint64_t key); /* Move to disk */
int crdtstore_promote_to_hot(crdt_store_t *store, uint64_t key); /* Move to HOT tier */
uint32_t crdtstore_get_preferred_node(uint64_t key, uint32_t total_shards); /* Which node should cache this? */

/* Metadata-specific operations */
int crdtstore_put_node(crdt_store_t *store, const mfs_node_t *node);
int crdtstore_get_node(crdt_store_t *store, uint32_t inode, mfs_node_t *node);
int crdtstore_put_edge(crdt_store_t *store, const mfs_edge_t *edge);
int crdtstore_get_edge(crdt_store_t *store, uint32_t parent, const char *name, mfs_edge_t **edge);
int crdtstore_remove_edge(crdt_store_t *store, const mfs_edge_t *edge);
int crdtstore_put_chunk(crdt_store_t *store, const mfs_chunk_crdt_t *chunk);
int crdtstore_get_chunk(crdt_store_t *store, uint64_t chunkid, mfs_chunk_crdt_t *chunk);

/* Chunkserver operations */
int crdtstore_put_chunkserver(crdt_store_t *store, const mfs_chunkserver_t *cs);
int crdtstore_get_chunkserver(crdt_store_t *store, uint32_t servip, uint16_t servport, mfs_chunkserver_t *cs);
int crdtstore_remove_chunkserver(crdt_store_t *store, uint32_t servip, uint16_t servport);
int crdtstore_get_all_chunkservers(crdt_store_t *store, mfs_chunkserver_t **cs_array, uint32_t *count);

/* Serialization for Raft log and ring deltas */
int crdtstore_serialize_entry(const crdt_entry_t *entry, uint8_t **data, uint32_t *size);
int crdtstore_deserialize_entry(const uint8_t *data, uint32_t size, crdt_entry_t **entry);

/* Metadata.mfs compatibility */
int crdtstore_export_metadata(crdt_store_t *store, const char *filename);
int crdtstore_import_metadata(crdt_store_t *store, const char *filename);

/* Statistics and monitoring */
void crdtstore_get_stats(crdt_store_t *store, uint32_t *entries, uint32_t *memory_usage);

/* Clock management */
lamport_time_t crdtstore_get_time(crdt_store_t *store);
void crdtstore_update_clock(crdt_store_t *store, const lamport_time_t *remote_time);

/* HLC management */
void hlc_init(hlc_timestamp_t *hlc);
void hlc_update(hlc_timestamp_t *hlc, const hlc_timestamp_t *remote);
int hlc_compare(const hlc_timestamp_t *a, const hlc_timestamp_t *b);
uint64_t hlc_to_physical_ms(const hlc_timestamp_t *hlc);

/* Version management */
int crdtstore_version_init(uint32_t shard_count);
void crdtstore_version_term(void);
int crdtstore_version_new(uint32_t shard_id, uint64_t raft_index, 
                         uint32_t leader_node, uint64_t lease_epoch,
                         global_version_t *version);
int crdtstore_version_merge(const global_version_t *local, 
                           const global_version_t *remote,
                           global_version_t *result);
uint64_t crdtstore_version_to_monotonic(const global_version_t *version);
int crdtstore_version_from_monotonic(uint64_t monotonic, global_version_t *version);

/* Leader lease management */
int crdtstore_lease_acquire(uint32_t shard_id, uint32_t node_id, 
                           uint64_t *lease_epoch, uint64_t *lease_expiry);
int crdtstore_lease_renew(uint32_t shard_id, uint32_t node_id,
                         uint64_t lease_epoch, uint64_t *new_expiry);
int crdtstore_lease_release(uint32_t shard_id, uint32_t node_id, uint64_t lease_epoch);
int crdtstore_lease_check(uint32_t shard_id, uint64_t lease_epoch);

/* Cluster sync for bootstrapping */
int crdt_cluster_sync_attempt(void);

#endif
