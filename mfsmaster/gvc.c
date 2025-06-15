/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 * 
 * Global Version Coordinator (GVC) implementation
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>

#include "gvc.h"
#include "main.h"
#include "cfg.h"
#include "mfslog.h"
#include "massert.h"
#include "hamaster.h"
#include "metadata.h"
#include "crdtstore.h"
#include "clocks.h"
#include "hashfn.h"
#include "raftconsensus.h"

/* GVC state structure */
typedef struct {
    uint64_t current_version;       /* Current global version counter */
    uint64_t local_version_start;   /* Start of our allocated range */
    uint64_t local_version_end;     /* End of our allocated range */
    uint64_t local_version_next;    /* Next version to use */
    
    uint8_t role;                   /* Current GVC role */
    uint32_t leader_node_id;        /* Current GVC leader */
    uint64_t leader_lease_expiry;   /* When leader lease expires */
    
    /* Provisional operation queue */
    versioned_op_t *provisional_ops;
    uint32_t provisional_count;
    uint32_t provisional_capacity;
    
    /* Client session tracking */
    struct {
        uint32_t session_id;
        uint64_t highest_seen;
    } *client_views;
    uint32_t client_count;
    uint32_t client_capacity;
    
    /* Configuration */
    uint32_t batch_size;
    uint32_t prefetch_threshold;
    uint32_t safety_gap;
    
    /* Statistics */
    uint64_t total_allocated;
    uint64_t total_returned;
    uint32_t allocation_requests;
    uint32_t leader_elections;
} gvc_state_t;

static gvc_state_t gvc_state;
static pthread_mutex_t gvc_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Forward declarations */
static int gvc_request_version_range(uint32_t count);
static uint64_t gvc_calculate_adaptive_batch_size(void);

/* Initialize GVC */
int gvc_init(void) {
    pthread_mutex_lock(&gvc_mutex);
    
    memset(&gvc_state, 0, sizeof(gvc_state));
    
    /* Load configuration */
    gvc_state.batch_size = cfg_getuint32("GVC_BATCH_SIZE", GVC_DEFAULT_BATCH_SIZE);
    if (gvc_state.batch_size < GVC_MIN_BATCH_SIZE) {
        gvc_state.batch_size = GVC_MIN_BATCH_SIZE;
    }
    if (gvc_state.batch_size > GVC_MAX_BATCH_SIZE) {
        gvc_state.batch_size = GVC_MAX_BATCH_SIZE;
    }
    
    gvc_state.prefetch_threshold = (uint32_t)(gvc_state.batch_size * GVC_ADAPTIVE_THRESHOLD);
    gvc_state.safety_gap = cfg_getuint32("GVC_SAFETY_GAP", 10000);
    
    /* Initialize provisional operation queue */
    gvc_state.provisional_capacity = 1000;
    gvc_state.provisional_ops = malloc(sizeof(versioned_op_t) * gvc_state.provisional_capacity);
    passert(gvc_state.provisional_ops);
    
    /* Initialize client view tracking */
    gvc_state.client_capacity = 100;
    gvc_state.client_views = malloc(sizeof(gvc_state.client_views[0]) * gvc_state.client_capacity);
    passert(gvc_state.client_views);
    
    /* Start with no role */
    gvc_state.role = GVC_ROLE_NONE;
    gvc_state.leader_node_id = 0;
    
    /* Initialize version from metadata if available */
    gvc_state.current_version = meta_version();
    
    pthread_mutex_unlock(&gvc_mutex);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "GVC initialized with batch_size=%"PRIu32", starting version=%"PRIu64,
            gvc_state.batch_size, gvc_state.current_version);
    
    return 0;
}

/* Terminate GVC */
void gvc_term(void) {
    pthread_mutex_lock(&gvc_mutex);
    
    /* Return any unused versions */
    if (gvc_state.local_version_next < gvc_state.local_version_end) {
        gvc_return_unused_versions(gvc_state.local_version_next, gvc_state.local_version_end);
    }
    
    /* Free resources */
    if (gvc_state.provisional_ops) {
        free(gvc_state.provisional_ops);
        gvc_state.provisional_ops = NULL;
    }
    
    if (gvc_state.client_views) {
        free(gvc_state.client_views);
        gvc_state.client_views = NULL;
    }
    
    pthread_mutex_unlock(&gvc_mutex);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "GVC terminated");
}

/* Reload configuration */
void gvc_reload(void) {
    uint32_t new_batch_size;
    
    pthread_mutex_lock(&gvc_mutex);
    
    new_batch_size = cfg_getuint32("GVC_BATCH_SIZE", GVC_DEFAULT_BATCH_SIZE);
    if (new_batch_size < GVC_MIN_BATCH_SIZE) {
        new_batch_size = GVC_MIN_BATCH_SIZE;
    }
    if (new_batch_size > GVC_MAX_BATCH_SIZE) {
        new_batch_size = GVC_MAX_BATCH_SIZE;
    }
    
    if (new_batch_size != gvc_state.batch_size) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "GVC batch size changed from %"PRIu32" to %"PRIu32,
                gvc_state.batch_size, new_batch_size);
        gvc_state.batch_size = new_batch_size;
        gvc_state.prefetch_threshold = (uint32_t)(gvc_state.batch_size * GVC_ADAPTIVE_THRESHOLD);
    }
    
    pthread_mutex_unlock(&gvc_mutex);
}

/* Get next version number */
uint64_t gvc_get_next_version(void) {
    uint64_t version = 0;
    uint32_t remaining;
    
    pthread_mutex_lock(&gvc_mutex);
    
    /* Check if we have versions available */
    if (gvc_state.local_version_next >= gvc_state.local_version_end) {
        /* Need to allocate more versions */
        uint32_t batch_size = gvc_calculate_adaptive_batch_size();
        
        if (gvc_request_version_range(batch_size) < 0) {
            pthread_mutex_unlock(&gvc_mutex);
            return 0; /* Failed to allocate */
        }
    }
    
    /* Use next available version */
    version = gvc_state.local_version_next++;
    
    /* Check if we should prefetch more versions */
    remaining = gvc_state.local_version_end - gvc_state.local_version_next;
    if (remaining < gvc_state.prefetch_threshold) {
        /* Start async prefetch */
        /* TODO: Implement async prefetch */
    }
    
    pthread_mutex_unlock(&gvc_mutex);
    
    return version;
}

/* Allocate a range of versions */
int gvc_allocate_versions(uint32_t count, version_range_t *range) {
    int result = -1;
    
    pthread_mutex_lock(&gvc_mutex);
    
    if (gvc_state.role == GVC_ROLE_LEADER) {
        /* We are the leader, allocate directly */
        range->start = gvc_state.current_version;
        range->end = gvc_state.current_version + count;
        range->allocated_at = monotonic_useconds();
        range->node_id = ha_get_node_id();
        
        gvc_state.current_version = range->end;
        gvc_state.total_allocated += count;
        gvc_state.allocation_requests++;
        
        /* Store allocation in CRDT for persistence */
        crdt_store_t *store = crdtstore_get_main_store();
        if (store) {
            char key[64];
            snprintf(key, sizeof(key), "gvc:allocation:%"PRIu64, range->start);
            crdtstore_put(store, range->start, 
                         CRDT_LWW_REGISTER, range, sizeof(version_range_t));
        }
        
        result = 0;
    } else {
        /* Forward request to leader */
        /* TODO: Implement RPC to leader */
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "GVC allocation request failed - not leader");
    }
    
    pthread_mutex_unlock(&gvc_mutex);
    
    return result;
}

/* Internal: Request version range from leader */
static int gvc_request_version_range(uint32_t count) {
    version_range_t range;
    
    /* Check if we're the leader of shard 0 (GVC shard) */
    if (raft_is_leader(0)) {
        /* We are the leader, allocate directly */
        if (gvc_state.current_version == 0) {
            gvc_state.current_version = meta_version();
        }
        
        range.start = gvc_state.current_version;
        range.end = gvc_state.current_version + count;
        gvc_state.current_version = range.end;
        
        gvc_state.local_version_start = range.start;
        gvc_state.local_version_end = range.end;
        gvc_state.local_version_next = range.start;
        
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "GVC allocated versions [%"PRIu64"-%"PRIu64"] as shard 0 leader",
                range.start, range.end);
        
        return 0;
    }
    
    /* TODO: Implement RPC request to leader */
    /* For now, fall back to incrementing from last known version */
    if (gvc_state.current_version == 0) {
        gvc_state.current_version = meta_version();
    }
    
    gvc_state.local_version_start = gvc_state.current_version;
    gvc_state.local_version_end = gvc_state.current_version + count;
    gvc_state.local_version_next = gvc_state.local_version_start;
    gvc_state.current_version = gvc_state.local_version_end;
    
    uint32_t leader = raft_get_leader(0);
    mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "GVC using local allocation [%"PRIu64"-%"PRIu64"] - leader is node %u",
            gvc_state.local_version_start, gvc_state.local_version_end, leader);
    
    return 0;
}

/* Calculate adaptive batch size based on usage patterns */
static uint64_t gvc_calculate_adaptive_batch_size(void) {
    /* TODO: Implement adaptive sizing based on:
     * - Recent allocation rate
     * - Number of active writers
     * - Available memory
     * For now, use fixed batch size */
    return gvc_state.batch_size;
}

/* Return unused versions */
void gvc_return_unused_versions(uint64_t start, uint64_t end) {
    pthread_mutex_lock(&gvc_mutex);
    
    if (start < end) {
        gvc_state.total_returned += (end - start);
        
        /* TODO: Return to leader for reuse */
        /* For now, just log it */
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "GVC returning unused versions [%"PRIu64"-%"PRIu64"]",
                start, end);
    }
    
    pthread_mutex_unlock(&gvc_mutex);
}

/* Add provisional operation */
int gvc_add_provisional_op(const void *op_data, uint32_t op_size, versioned_op_t *versioned) {
    pthread_mutex_lock(&gvc_mutex);
    
    /* Ensure capacity */
    if (gvc_state.provisional_count >= gvc_state.provisional_capacity) {
        uint32_t new_capacity = gvc_state.provisional_capacity * 2;
        versioned_op_t *new_ops = realloc(gvc_state.provisional_ops, 
                                         sizeof(versioned_op_t) * new_capacity);
        if (!new_ops) {
            pthread_mutex_unlock(&gvc_mutex);
            return -1;
        }
        gvc_state.provisional_ops = new_ops;
        gvc_state.provisional_capacity = new_capacity;
    }
    
    /* Add provisional operation */
    versioned->global_version = 0; /* Provisional */
    versioned->local_timestamp = monotonic_useconds();
    /* Simple hash of operation data */
    uint32_t hash = 0;
    const uint8_t *data = (const uint8_t*)op_data;
    uint32_t i;
    for (i = 0; i < op_size; i++) {
        hash = hash * 31 + data[i];
    }
    versioned->operation_hash = hash;
    versioned->state = OP_STATE_PROVISIONAL;
    
    gvc_state.provisional_ops[gvc_state.provisional_count++] = *versioned;
    
    pthread_mutex_unlock(&gvc_mutex);
    
    return 0;
}

/* Finalize operation with assigned version */
int gvc_finalize_op(uint32_t op_hash, uint64_t assigned_version) {
    uint32_t i;
    int found = 0;
    
    pthread_mutex_lock(&gvc_mutex);
    
    for (i = 0; i < gvc_state.provisional_count; i++) {
        if (gvc_state.provisional_ops[i].operation_hash == op_hash &&
            gvc_state.provisional_ops[i].state == OP_STATE_PROVISIONAL) {
            gvc_state.provisional_ops[i].global_version = assigned_version;
            gvc_state.provisional_ops[i].state = OP_STATE_VERSIONED;
            found = 1;
            break;
        }
    }
    
    pthread_mutex_unlock(&gvc_mutex);
    
    return found ? 0 : -1;
}

/* Update client view for monotonic guarantees */
int gvc_update_client_view(uint32_t session_id, uint64_t seen_version) {
    uint32_t i;
    int found = 0;
    
    pthread_mutex_lock(&gvc_mutex);
    
    /* Find or create client entry */
    for (i = 0; i < gvc_state.client_count; i++) {
        if (gvc_state.client_views[i].session_id == session_id) {
            if (seen_version > gvc_state.client_views[i].highest_seen) {
                gvc_state.client_views[i].highest_seen = seen_version;
            }
            found = 1;
            break;
        }
    }
    
    if (!found) {
        /* Add new client */
        if (gvc_state.client_count >= gvc_state.client_capacity) {
            uint32_t new_capacity = gvc_state.client_capacity * 2;
            void *new_views = realloc(gvc_state.client_views, 
                                    sizeof(gvc_state.client_views[0]) * new_capacity);
            if (!new_views) {
                pthread_mutex_unlock(&gvc_mutex);
                return -1;
            }
            gvc_state.client_views = new_views;
            gvc_state.client_capacity = new_capacity;
        }
        
        gvc_state.client_views[gvc_state.client_count].session_id = session_id;
        gvc_state.client_views[gvc_state.client_count].highest_seen = seen_version;
        gvc_state.client_count++;
    }
    
    pthread_mutex_unlock(&gvc_mutex);
    
    return 0;
}

/* Get client's highest seen version */
uint64_t gvc_get_client_view(uint32_t session_id) {
    uint32_t i;
    uint64_t highest_seen = 0;
    
    pthread_mutex_lock(&gvc_mutex);
    
    for (i = 0; i < gvc_state.client_count; i++) {
        if (gvc_state.client_views[i].session_id == session_id) {
            highest_seen = gvc_state.client_views[i].highest_seen;
            break;
        }
    }
    
    pthread_mutex_unlock(&gvc_mutex);
    
    return highest_seen;
}

/* Become GVC leader */
int gvc_become_leader(void) {
    pthread_mutex_lock(&gvc_mutex);
    
    /* Get highest version from all nodes */
    uint64_t highest_version = meta_version();
    
    /* TODO: Query all nodes for their highest version */
    
    /* Start from highest + safety gap */
    gvc_state.current_version = highest_version + gvc_state.safety_gap;
    gvc_state.role = GVC_ROLE_LEADER;
    gvc_state.leader_node_id = ha_get_node_id();
    gvc_state.leader_lease_expiry = monotonic_seconds() + 30; /* 30 second lease */
    gvc_state.leader_elections++;
    
    pthread_mutex_unlock(&gvc_mutex);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "GVC became leader, starting from version %"PRIu64,
            gvc_state.current_version);
    
    return 0;
}

/* Get current role */
uint8_t gvc_get_role(void) {
    uint8_t role;
    
    pthread_mutex_lock(&gvc_mutex);
    role = gvc_state.role;
    pthread_mutex_unlock(&gvc_mutex);
    
    return role;
}

/* Get statistics */
void gvc_get_stats(uint64_t *current_version, uint64_t *allocated_versions, 
                   uint32_t *pending_ops, uint32_t *active_clients) {
    pthread_mutex_lock(&gvc_mutex);
    
    if (current_version) *current_version = gvc_state.current_version;
    if (allocated_versions) *allocated_versions = gvc_state.total_allocated;
    if (pending_ops) *pending_ops = gvc_state.provisional_count;
    if (active_clients) *active_clients = gvc_state.client_count;
    
    pthread_mutex_unlock(&gvc_mutex);
}

/* Display GVC information */
void gvc_info(FILE *fd) {
    pthread_mutex_lock(&gvc_mutex);
    
    fprintf(fd, "[Global Version Coordinator]\n");
    fprintf(fd, "role: %s\n", 
            gvc_state.role == GVC_ROLE_LEADER ? "LEADER" :
            gvc_state.role == GVC_ROLE_FOLLOWER ? "FOLLOWER" :
            gvc_state.role == GVC_ROLE_CANDIDATE ? "CANDIDATE" : "NONE");
    fprintf(fd, "leader_node: %"PRIu32"\n", gvc_state.leader_node_id);
    fprintf(fd, "current_version: %"PRIu64"\n", gvc_state.current_version);
    fprintf(fd, "local_range: [%"PRIu64"-%"PRIu64"]\n", 
            gvc_state.local_version_start, gvc_state.local_version_end);
    fprintf(fd, "local_next: %"PRIu64"\n", gvc_state.local_version_next);
    fprintf(fd, "batch_size: %"PRIu32"\n", gvc_state.batch_size);
    fprintf(fd, "provisional_ops: %"PRIu32"\n", gvc_state.provisional_count);
    fprintf(fd, "active_clients: %"PRIu32"\n", gvc_state.client_count);
    fprintf(fd, "total_allocated: %"PRIu64"\n", gvc_state.total_allocated);
    fprintf(fd, "total_returned: %"PRIu64"\n", gvc_state.total_returned);
    fprintf(fd, "allocation_requests: %"PRIu32"\n", gvc_state.allocation_requests);
    fprintf(fd, "leader_elections: %"PRIu32"\n", gvc_state.leader_elections);
    fprintf(fd, "\n");
    
    pthread_mutex_unlock(&gvc_mutex);
}