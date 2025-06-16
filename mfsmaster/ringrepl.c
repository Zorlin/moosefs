/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 * 
 * Ring Replication implementation for reliable changelog distribution
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>

#include "ringrepl.h"
#include "hamaster.h"
#include "haconn.h"
#include "changelog.h"
#include "changelog_replay.h"
#include "metadata.h"
#include "metasync.h"
#include "clocks.h"
#include "datapack.h"
#include "mfslog.h"
#include "cfg.h"
#include "main.h"

/* Global state */
static struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    
    /* Ring topology */
    uint32_t *ring_nodes;
    uint32_t ring_size;
    uint32_t my_position;
    
    /* Sync progress tracking */
    ring_sync_progress_t *sync_progress;
    
    /* Batch management */
    uint64_t *batch_buffer;
    uint32_t batch_count;
    uint32_t batch_capacity;
    
    /* Configuration */
    uint32_t max_batch_size;
    double ack_timeout;
    double retry_interval;
    uint32_t max_retries;
    
    /* Statistics */
    uint64_t entries_sent;
    uint64_t entries_acked;
    uint64_t entries_retried;
    uint64_t sync_failures;
} ring_state;

/* Forward declarations */
static void ringrepl_send_batch(uint32_t peer_id);
static void ringrepl_retry_failed_sync(uint32_t peer_id);

/* Initialize ring topology from HA peer configuration */
static int ringrepl_init_topology(void) {
    const char *peers_str;
    char *peers_copy, *peer, *saveptr;
    uint32_t my_id;
    uint32_t node_count = 0;
    uint32_t *temp_nodes;
    uint32_t i, j;
    
    my_id = ha_get_node_id();
    if (my_id == 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "ringrepl: cannot get node ID");
        return -1;
    }
    
    peers_str = ha_get_peers();
    if (!peers_str || strlen(peers_str) == 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "ringrepl: no peers configured");
        return -1;
    }
    
    /* Count nodes (including self) */
    peers_copy = strdup(peers_str);
    if (!peers_copy) {
        return -1;
    }
    
    /* First pass: count peers (includes self) */
    peer = strtok_r(peers_copy, ",", &saveptr);
    while (peer != NULL) {
        node_count++;
        peer = strtok_r(NULL, ",", &saveptr);
    }
    /* Don't add self - already included in peer list */
    
    /* Allocate node array */
    temp_nodes = malloc(node_count * sizeof(uint32_t));
    if (!temp_nodes) {
        free(peers_copy);
        return -1;
    }
    
    /* For now, use simple node IDs 1 through node_count */
    for (i = 0; i < node_count; i++) {
        temp_nodes[i] = i + 1;
    }
    
    /* Sort nodes to ensure consistent ring order */
    for (i = 0; i < node_count - 1; i++) {
        for (j = i + 1; j < node_count; j++) {
            if (temp_nodes[i] > temp_nodes[j]) {
                uint32_t tmp = temp_nodes[i];
                temp_nodes[i] = temp_nodes[j];
                temp_nodes[j] = tmp;
            }
        }
    }
    
    /* Find our position in the ring */
    ring_state.my_position = 0;
    for (i = 0; i < node_count; i++) {
        if (temp_nodes[i] == my_id) {
            ring_state.my_position = i;
            break;
        }
    }
    
    ring_state.ring_nodes = temp_nodes;
    ring_state.ring_size = node_count;
    
    free(peers_copy);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "ringrepl: initialized ring with %u nodes, my position: %u", 
            node_count, ring_state.my_position);
    
    return 0;
}

/* Get sync progress for a peer, create if doesn't exist */
static ring_sync_progress_t* ringrepl_get_progress(uint32_t peer_id) {
    ring_sync_progress_t *progress;
    
    /* Search for existing progress */
    for (progress = ring_state.sync_progress; progress; progress = progress->next) {
        if (progress->peer_id == peer_id) {
            return progress;
        }
    }
    
    /* Create new progress entry */
    progress = malloc(sizeof(ring_sync_progress_t));
    if (!progress) {
        return NULL;
    }
    
    progress->peer_id = peer_id;
    progress->last_sent_version = 0;
    progress->last_acked_version = 0;
    progress->target_version = 0;
    progress->retry_count = 0;
    progress->last_attempt_time = 0;
    progress->last_success_time = 0;
    progress->state = RING_STATE_IDLE;
    progress->next = ring_state.sync_progress;
    ring_state.sync_progress = progress;
    
    return progress;
}

/* Initialize ring replication subsystem */
int ringrepl_init(void) {
    pthread_mutex_init(&ring_state.mutex, NULL);
    pthread_cond_init(&ring_state.cond, NULL);
    
    /* Initialize configuration */
    ring_state.max_batch_size = cfg_getuint32("HA_RING_BATCH_SIZE", RING_MAX_BATCH_SIZE);
    ring_state.ack_timeout = cfg_getdouble("HA_RING_ACK_TIMEOUT", RING_ACK_TIMEOUT);
    ring_state.retry_interval = cfg_getdouble("HA_RING_RETRY_INTERVAL", RING_RETRY_INTERVAL);
    ring_state.max_retries = cfg_getuint32("HA_RING_MAX_RETRIES", RING_MAX_RETRIES);
    
    /* Initialize batch buffer */
    ring_state.batch_capacity = ring_state.max_batch_size;
    ring_state.batch_buffer = malloc(ring_state.batch_capacity * sizeof(uint64_t));
    if (!ring_state.batch_buffer) {
        pthread_mutex_destroy(&ring_state.mutex);
        pthread_cond_destroy(&ring_state.cond);
        return -1;
    }
    ring_state.batch_count = 0;
    
    /* Initialize ring topology */
    if (ringrepl_init_topology() < 0) {
        free(ring_state.batch_buffer);
        pthread_mutex_destroy(&ring_state.mutex);
        pthread_cond_destroy(&ring_state.cond);
        return -1;
    }
    
    /* Initialize statistics */
    ring_state.entries_sent = 0;
    ring_state.entries_acked = 0;
    ring_state.entries_retried = 0;
    ring_state.sync_failures = 0;
    
    /* Register periodic check */
    main_time_register(1, 0, ringrepl_periodic_check);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "ringrepl: initialized with batch_size=%u, ack_timeout=%.1f", 
            ring_state.max_batch_size, ring_state.ack_timeout);
    
    return 0;
}

/* Terminate ring replication subsystem */
void ringrepl_term(void) {
    ring_sync_progress_t *progress, *next;
    
    pthread_mutex_lock(&ring_state.mutex);
    
    /* Free sync progress entries */
    progress = ring_state.sync_progress;
    while (progress) {
        next = progress->next;
        free(progress);
        progress = next;
    }
    ring_state.sync_progress = NULL;
    
    /* Free ring topology */
    if (ring_state.ring_nodes) {
        free(ring_state.ring_nodes);
        ring_state.ring_nodes = NULL;
    }
    
    /* Free batch buffer */
    if (ring_state.batch_buffer) {
        free(ring_state.batch_buffer);
        ring_state.batch_buffer = NULL;
    }
    
    pthread_mutex_unlock(&ring_state.mutex);
    
    pthread_mutex_destroy(&ring_state.mutex);
    pthread_cond_destroy(&ring_state.cond);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "ringrepl: terminated (sent=%"PRIu64", acked=%"PRIu64", retried=%"PRIu64", failures=%"PRIu64")", 
            ring_state.entries_sent, ring_state.entries_acked, 
            ring_state.entries_retried, ring_state.sync_failures);
}

/* Get ring successor for a node */
uint32_t ringrepl_get_successor(uint32_t node_id) {
    uint32_t i;
    
    /* Ring topology is read-only after initialization - no locking needed */
    if (!ring_state.ring_nodes || ring_state.ring_size == 0) {
        return 0;
    }
    
    /* Find node position */
    for (i = 0; i < ring_state.ring_size; i++) {
        if (ring_state.ring_nodes[i] == node_id) {
            /* Return next node in ring */
            uint32_t next_pos = (i + 1) % ring_state.ring_size;
            uint32_t successor = ring_state.ring_nodes[next_pos];
            return successor;
        }
    }
    
    return 0; /* Node not found */
}

/* Get all nodes in ring order */
int ringrepl_get_ring_order(uint32_t *nodes, uint32_t *count) {
    /* Ring topology is read-only after initialization - no locking needed */
    if (!ring_state.ring_nodes || !nodes || !count) {
        return -1;
    }
    
    if (*count < ring_state.ring_size) {
        *count = ring_state.ring_size;
        return -1; /* Buffer too small */
    }
    
    memcpy(nodes, ring_state.ring_nodes, ring_state.ring_size * sizeof(uint32_t));
    *count = ring_state.ring_size;
    
    return 0;
}

/* Send a batch of entries to peer */
static void ringrepl_send_batch(uint32_t peer_id) {
    ring_sync_progress_t *progress;
    uint64_t start_version, end_version;
    uint32_t count;
    uint8_t msg[32];  /* Increased size to handle all message types */
    uint8_t *ptr;
    
    progress = ringrepl_get_progress(peer_id);
    if (!progress) {
        return;
    }
    
    /* Check if peer connection is available before attempting to send */
    extern int haconn_is_peer_connected(uint32_t peer_id);
    if (!haconn_is_peer_connected(peer_id)) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "ringrepl: peer %u not connected, deferring batch send", peer_id);
        progress->state = RING_STATE_IDLE;  /* Will retry later when connection is available */
        return;
    }
    
    /* Determine version range to send */
    start_version = progress->last_acked_version + 1;
    end_version = progress->target_version;
    
    /* Limit batch size */
    if (end_version - start_version + 1 > ring_state.max_batch_size) {
        end_version = start_version + ring_state.max_batch_size - 1;
    }
    
    /* Send batch start message */
    ptr = msg;
    put8bit(&ptr, RING_MSG_BATCH_START);
    put64bit(&ptr, start_version);
    put64bit(&ptr, end_version);
    put32bit(&ptr, ha_get_node_id());  /* Send our node ID, not peer's */
    if (haconn_send_meta_sync_to_peer(peer_id, msg, 21) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "ringrepl: failed to send batch start to peer %u", peer_id);
        progress->state = RING_STATE_RETRYING;
        progress->retry_count++;
        return;
    }
    
    /* Send entries from changelog buffer */
    count = changelog_get_old_changes(start_version, metasync_send_entry_to_peer, 
                                     (void*)(uintptr_t)peer_id, 
                                     end_version - start_version + 1);
    
    /* Send batch end message */
    ptr = msg;
    put8bit(&ptr, RING_MSG_BATCH_END);
    put64bit(&ptr, end_version);
    put32bit(&ptr, count);
    if (haconn_send_meta_sync_to_peer(peer_id, msg, 13) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "ringrepl: failed to send batch end to peer %u", peer_id);
        progress->state = RING_STATE_RETRYING;
        progress->retry_count++;
        return;
    }
    
    /* Update progress */
    progress->last_sent_version = end_version;
    progress->last_attempt_time = monotonic_seconds();
    progress->state = RING_STATE_WAITING_ACK;
    ring_state.entries_sent += count;
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "ringrepl: sent batch [%"PRIu64"-%"PRIu64"] (%u entries) to peer %u", 
            start_version, end_version, count, peer_id);
}

/* Ship changelog entry through ring */
int ringrepl_ship_entry(uint64_t version, const uint8_t *data, uint32_t length) {
    uint32_t successor;
    ring_sync_progress_t *progress;
    
    pthread_mutex_lock(&ring_state.mutex);
    
    /* Get our successor in the ring */
    successor = ringrepl_get_successor(ha_get_node_id());
    if (successor == 0) {
        pthread_mutex_unlock(&ring_state.mutex);
        return -1;
    }
    
    /* Get sync progress for successor */
    progress = ringrepl_get_progress(successor);
    if (!progress) {
        pthread_mutex_unlock(&ring_state.mutex);
        return -1;
    }
    
    /* Update target version */
    if (version > progress->target_version) {
        progress->target_version = version;
    }
    
    /* If idle, start sending */
    if (progress->state == RING_STATE_IDLE) {
        ringrepl_send_batch(successor);
    }
    
    pthread_mutex_unlock(&ring_state.mutex);
    return 0;
}

/* Handle acknowledgment from peer */
static void ringrepl_handle_ack(uint32_t peer_id, uint64_t acked_version) {
    ring_sync_progress_t *progress;
    
    pthread_mutex_lock(&ring_state.mutex);
    
    progress = ringrepl_get_progress(peer_id);
    if (!progress) {
        pthread_mutex_unlock(&ring_state.mutex);
        return;
    }
    
    /* Update acknowledged version */
    if (acked_version > progress->last_acked_version) {
        progress->last_acked_version = acked_version;
        progress->retry_count = 0;
        progress->last_success_time = monotonic_seconds();
        ring_state.entries_acked += (acked_version - progress->last_acked_version);
        
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "ringrepl: peer %u acknowledged up to version %"PRIu64, 
                peer_id, acked_version);
    }
    
    /* Continue sending if more entries pending */
    if (progress->last_acked_version < progress->target_version) {
        progress->state = RING_STATE_IDLE;
        ringrepl_send_batch(peer_id);
    } else {
        progress->state = RING_STATE_IDLE;
    }
    
    pthread_mutex_unlock(&ring_state.mutex);
}

/* Handle negative acknowledgment from peer */
static void ringrepl_handle_nack(uint32_t peer_id, uint64_t last_good_version) {
    ring_sync_progress_t *progress;
    
    pthread_mutex_lock(&ring_state.mutex);
    
    progress = ringrepl_get_progress(peer_id);
    if (!progress) {
        pthread_mutex_unlock(&ring_state.mutex);
        return;
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "ringrepl: peer %u NACK, last good version %"PRIu64, 
            peer_id, last_good_version);
    
    /* Reset to last known good version */
    if (last_good_version < progress->last_acked_version) {
        progress->last_acked_version = last_good_version;
    }
    
    /* Retry with smaller batch or different approach */
    progress->retry_count++;
    if (progress->retry_count < ring_state.max_retries) {
        progress->state = RING_STATE_RETRYING;
        ring_state.entries_retried++;
    } else {
        progress->state = RING_STATE_ERROR;
        ring_state.sync_failures++;
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "ringrepl: sync to peer %u failed after %u retries", 
                peer_id, ring_state.max_retries);
    }
    
    pthread_mutex_unlock(&ring_state.mutex);
}

/* Handle resync request from peer */
static void ringrepl_handle_resync_req(uint32_t peer_id, uint64_t from_version) {
    ring_sync_progress_t *progress;
    
    pthread_mutex_lock(&ring_state.mutex);
    
    progress = ringrepl_get_progress(peer_id);
    if (!progress) {
        pthread_mutex_unlock(&ring_state.mutex);
        return;
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "ringrepl: peer %u requesting resync from version %"PRIu64, 
            peer_id, from_version);
    
    /* Reset progress to requested version */
    progress->last_acked_version = from_version - 1;
    progress->target_version = meta_version();
    progress->state = RING_STATE_IDLE;
    progress->retry_count = 0;
    
    /* Start sending from requested version */
    ringrepl_send_batch(peer_id);
    
    pthread_mutex_unlock(&ring_state.mutex);
}

/* Handle incoming ring replication message */
void ringrepl_handle_message(uint32_t peerid, const uint8_t *data, uint32_t length) {
    const uint8_t *ptr = data;
    uint8_t msgtype;
    
    if (length < 1) {
        return;
    }
    
    msgtype = get8bit(&ptr);
    length--;
    
    switch (msgtype) {
        case RING_MSG_ACK: {
            if (length >= 8) {
                uint64_t acked_version = get64bit(&ptr);
                ringrepl_handle_ack(peerid, acked_version);
            }
            break;
        }
        
        case RING_MSG_NACK: {
            if (length >= 8) {
                uint64_t last_good_version = get64bit(&ptr);
                ringrepl_handle_nack(peerid, last_good_version);
            }
            break;
        }
        
        case RING_MSG_RESYNC_REQ: {
            if (length >= 8) {
                uint64_t from_version = get64bit(&ptr);
                ringrepl_handle_resync_req(peerid, from_version);
            }
            break;
        }
        
        case RING_MSG_HEARTBEAT: {
            /* Heartbeat received - peer is alive */
            mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "ringrepl: heartbeat from peer %u", peerid);
            break;
        }
        
        case RING_MSG_RANGE_REQ: {
            if (length >= 16) {
                uint64_t from_version = get64bit(&ptr);
                uint64_t to_version = get64bit(&ptr);
                
                mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "ringrepl: peer %u requested range [%"PRIu64"-%"PRIu64"]",
                        peerid, from_version, to_version);
                
                /* Send the requested changelog entries directly */
                uint32_t count = changelog_get_old_changes(from_version,
                    metasync_send_entry_to_peer, (void*)(uintptr_t)peerid,
                    (to_version - from_version + 1));
                
                /* Send range response with count */
                uint8_t resp[13];
                uint8_t *rptr = resp;
                put8bit(&rptr, RING_MSG_RANGE_RESP);
                put64bit(&rptr, to_version);
                put32bit(&rptr, count);
                haconn_send_meta_sync_to_peer(peerid, resp, 13);
                
                mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "ringrepl: sent %u entries to peer %u", count, peerid);
            }
            break;
        }
        
        default:
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "ringrepl: unknown message type %u from peer %u", 
                    msgtype, peerid);
            break;
    }
}

/* Resume sync for a specific peer */
int ringrepl_resume_sync(uint32_t peer_id) {
    ring_sync_progress_t *progress;
    
    pthread_mutex_lock(&ring_state.mutex);
    
    progress = ringrepl_get_progress(peer_id);
    if (!progress) {
        pthread_mutex_unlock(&ring_state.mutex);
        return -1;
    }
    
    /* Reset state and retry */
    progress->state = RING_STATE_IDLE;
    progress->retry_count = 0;
    progress->target_version = meta_version();
    
    ringrepl_send_batch(peer_id);
    
    pthread_mutex_unlock(&ring_state.mutex);
    return 0;
}

/* Retry failed sync attempts */
static void ringrepl_retry_failed_sync(uint32_t peer_id) {
    ring_sync_progress_t *progress;
    double now;
    
    progress = ringrepl_get_progress(peer_id);
    if (!progress) {
        return;
    }
    
    now = monotonic_seconds();
    
    /* Check if it's time to retry */
    if (now - progress->last_attempt_time < ring_state.retry_interval) {
        return;
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "ringrepl: retrying sync to peer %u (attempt %u/%u)", 
            peer_id, progress->retry_count + 1, ring_state.max_retries);
    
    /* Reset state and retry */
    progress->state = RING_STATE_IDLE;
    ringrepl_send_batch(peer_id);
}

/* Periodic maintenance */
void ringrepl_periodic_check(void) {
    ring_sync_progress_t *progress;
    double now;
    uint32_t successor;
    
    pthread_mutex_lock(&ring_state.mutex);
    
    now = monotonic_seconds();
    successor = ringrepl_get_successor(ha_get_node_id());
    
    /* Check all sync progress entries */
    for (progress = ring_state.sync_progress; progress; progress = progress->next) {
        switch (progress->state) {
            case RING_STATE_SYNCING:
                /* Currently syncing, check progress */
                break;
                
            case RING_STATE_WAITING_ACK:
                /* Check for timeout */
                if (now - progress->last_attempt_time > ring_state.ack_timeout) {
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                            "ringrepl: timeout waiting for ACK from peer %u", progress->peer_id);
                    progress->state = RING_STATE_RETRYING;
                    progress->retry_count++;
                }
                break;
                
            case RING_STATE_RETRYING:
                /* Retry failed sync */
                ringrepl_retry_failed_sync(progress->peer_id);
                break;
                
            case RING_STATE_ERROR:
                /* Try to recover from error state */
                if (now - progress->last_attempt_time > 60.0) { /* 1 minute cooldown */
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
                            "ringrepl: attempting recovery for peer %u", progress->peer_id);
                    progress->state = RING_STATE_IDLE;
                    progress->retry_count = 0;
                    ringrepl_resume_sync(progress->peer_id);
                }
                break;
                
            case RING_STATE_IDLE:
                /* Check if we have pending entries to send */
                if (progress->last_acked_version < progress->target_version) {
                    ringrepl_send_batch(progress->peer_id);
                }
                /* Send heartbeat to successor */
                else if (progress->peer_id == successor && 
                         now - progress->last_success_time > RING_HEARTBEAT_INTERVAL) {
                    uint8_t msg[1];
                    msg[0] = RING_MSG_HEARTBEAT;
                    haconn_send_meta_sync_to_peer(progress->peer_id, msg, 1);
                }
                break;
        }
    }
    
    pthread_mutex_unlock(&ring_state.mutex);
}

/* Get sync progress for monitoring */
void ringrepl_get_sync_status(uint32_t peer_id, uint64_t *last_sent, 
                             uint64_t *last_acked, ring_state_t *state) {
    ring_sync_progress_t *progress;
    
    pthread_mutex_lock(&ring_state.mutex);
    
    progress = ringrepl_get_progress(peer_id);
    if (progress) {
        if (last_sent) *last_sent = progress->last_sent_version;
        if (last_acked) *last_acked = progress->last_acked_version;
        if (state) *state = progress->state;
    } else {
        if (last_sent) *last_sent = 0;
        if (last_acked) *last_acked = 0;
        if (state) *state = RING_STATE_IDLE;
    }
    
    pthread_mutex_unlock(&ring_state.mutex);
}

/* Force full resync with peer */
int ringrepl_force_resync(uint32_t peer_id, uint64_t from_version) {
    ring_sync_progress_t *progress;
    
    pthread_mutex_lock(&ring_state.mutex);
    
    progress = ringrepl_get_progress(peer_id);
    if (!progress) {
        pthread_mutex_unlock(&ring_state.mutex);
        return -1;
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "ringrepl: forcing resync with peer %u from version %"PRIu64, 
            peer_id, from_version);
    
    /* Reset all progress */
    progress->last_sent_version = from_version - 1;
    progress->last_acked_version = from_version - 1;
    progress->target_version = meta_version();
    progress->retry_count = 0;
    progress->state = RING_STATE_IDLE;
    
    /* Start sync immediately */
    ringrepl_send_batch(peer_id);
    
    pthread_mutex_unlock(&ring_state.mutex);
    return 0;
}

/* Request specific range of changelog entries */
void ringrepl_request_range(uint64_t from_version, uint64_t to_version) {
    uint8_t msg[32];
    uint8_t *ptr = msg;
    uint32_t i;
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "ringrepl: requesting range [%"PRIu64"-%"PRIu64"]", 
            from_version, to_version);
    
    /* Build range request message */
    put8bit(&ptr, RING_MSG_RANGE_REQ);
    put64bit(&ptr, from_version);
    put64bit(&ptr, to_version);
    
    /* Send to all peers in the ring */
    pthread_mutex_lock(&ring_state.mutex);
    
    if (ring_state.ring_nodes && ring_state.ring_size > 0) {
        for (i = 0; i < ring_state.ring_size; i++) {
            uint32_t peer_id = ring_state.ring_nodes[i];
            if (peer_id != ha_get_node_id()) {
                if (haconn_is_peer_connected(peer_id)) {
                    if (haconn_send_meta_sync_to_peer(peer_id, msg, 17) < 0) {
                        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "ringrepl: failed to send range request to peer %u", peer_id);
                    }
                } else {
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "ringrepl: peer %u not connected, skipping range request", peer_id);
                }
            }
        }
    }
    
    pthread_mutex_unlock(&ring_state.mutex);
}