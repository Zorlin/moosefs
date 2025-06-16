/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 * 
 * Metadata synchronization for HA startup
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <poll.h>
#include <errno.h>

#include "metasync.h"
#include "metadata.h"
#include "changelog_replay.h"
#include "changelog.h"
#include "haconn.h"
#include "clocks.h"
#include "mfslog.h"
#include "datapack.h"
#include "cfg.h"
#include "main.h"
#include "hamaster.h"
#include "raftconsensus.h"
#include "ringrepl.h"

/* Sync message types */
#define METASYNC_VERSION_REQ    0x01
#define METASYNC_VERSION_RESP   0x02
#define METASYNC_RANGE_REQ      0x03
#define METASYNC_RANGE_RESP     0x04
#define METASYNC_ENTRY          0x05
#define METASYNC_DONE           0x06

/* Sync states */
enum {
    SYNC_IDLE,
    SYNC_QUERY_VERSIONS,
    SYNC_REQUEST_DATA,
    SYNC_RECEIVING,
    SYNC_COMPLETE,
    SYNC_ERROR
};

/* Peer sync info */
typedef struct peer_info {
    uint32_t nodeid;
    uint64_t version;
    uint8_t responded;
    struct peer_info *next;
} peer_info_t;

/* Global sync state */
static struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    
    uint8_t state;
    uint64_t local_version;
    uint64_t highest_peer_version;
    uint32_t sync_from_nodeid;
    
    peer_info_t *peers;
    uint32_t peers_total;
    uint32_t peers_responded;
    
    double start_time;
    double timeout;
} sync_state;

/* Initialize metadata sync subsystem */
int metasync_init(void) {
    pthread_mutex_init(&sync_state.mutex, NULL);
    pthread_cond_init(&sync_state.cond, NULL);
    
    sync_state.state = SYNC_IDLE;
    sync_state.local_version = 0;
    sync_state.highest_peer_version = 0;
    sync_state.sync_from_nodeid = 0;
    sync_state.peers = NULL;
    sync_state.peers_total = 0;
    sync_state.peers_responded = 0;
    sync_state.timeout = cfg_getdouble("HA_METASYNC_TIMEOUT", 30.0);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "metasync: initialized with timeout %.1fs", sync_state.timeout);
    return 0;
}

/* Terminate metadata sync subsystem */
void metasync_term(void) {
    peer_info_t *peer, *next;
    
    pthread_mutex_lock(&sync_state.mutex);
    
    peer = sync_state.peers;
    while (peer) {
        next = peer->next;
        free(peer);
        peer = next;
    }
    sync_state.peers = NULL;
    
    pthread_mutex_unlock(&sync_state.mutex);
    
    pthread_mutex_destroy(&sync_state.mutex);
    pthread_cond_destroy(&sync_state.cond);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "metasync: terminated");
}

/* Send version query to all peers */
static void metasync_send_version_query(void) {
    uint8_t msg[12];
    uint8_t *ptr = msg;
    
    /* Build message: type + our_version */
    put8bit(&ptr, METASYNC_VERSION_REQ);
    put64bit(&ptr, sync_state.local_version);
    
    /* Send to all peers via haconn */
    haconn_send_meta_sync(msg, 9);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "metasync: sent version query (local version %"PRIu64")", 
            sync_state.local_version);
}

/* Send range request to specific peer */
static void metasync_send_range_request(uint32_t nodeid, uint64_t from_version) {
    uint8_t msg[20];
    uint8_t *ptr = msg;
    
    /* Build message: type + from_version + to_version */
    put8bit(&ptr, METASYNC_RANGE_REQ);
    put64bit(&ptr, from_version);
    put64bit(&ptr, sync_state.highest_peer_version);
    
    /* Send to specific peer */
    haconn_send_meta_sync_to_peer(nodeid, msg, 17);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "metasync: requesting versions %"PRIu64"-%"PRIu64" from node %u", 
            from_version, sync_state.highest_peer_version, nodeid);
}

/* Process version response from peer */
static void metasync_process_version_response(uint32_t peerid, uint64_t peer_version) {
    peer_info_t *peer;
    int all_responded = 0;
    
    pthread_mutex_lock(&sync_state.mutex);
    
    /* Find or create peer info */
    for (peer = sync_state.peers; peer; peer = peer->next) {
        if (peer->nodeid == peerid) {
            break;
        }
    }
    
    if (!peer) {
        peer = malloc(sizeof(peer_info_t));
        if (!peer) {
            pthread_mutex_unlock(&sync_state.mutex);
            return;
        }
        peer->nodeid = peerid;
        peer->responded = 0;
        peer->version = 0;
        peer->next = sync_state.peers;
        sync_state.peers = peer;
        sync_state.peers_total++;
    }
    
    if (!peer->responded) {
        peer->version = peer_version;
        peer->responded = 1;
        sync_state.peers_responded++;
        
        /* Track highest peer version */
        if (peer_version > sync_state.highest_peer_version) {
            sync_state.highest_peer_version = peer_version;
            sync_state.sync_from_nodeid = peerid;
        }
        
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "metasync: peer %u has version %"PRIu64" (%u/%u responded)", 
                peerid, peer_version, sync_state.peers_responded, sync_state.peers_total);
        
        /* Check if all peers responded */
        if (sync_state.peers_responded >= sync_state.peers_total && sync_state.peers_total > 0) {
            all_responded = 1;
        }
    }
    
    if (all_responded) {
        /* All peers responded, decide what to do */
        if (sync_state.highest_peer_version > sync_state.local_version) {
            /* We need to sync from a peer */
            sync_state.state = SYNC_REQUEST_DATA;
            pthread_cond_signal(&sync_state.cond);
        } else {
            /* We're up to date */
            sync_state.state = SYNC_COMPLETE;
            pthread_cond_signal(&sync_state.cond);
        }
    }
    
    pthread_mutex_unlock(&sync_state.mutex);
}

/* Ring-based log shipping implementation */
static uint32_t get_ring_successor(void) {
    /* TODO: Implement proper ring topology based on node IDs */
    /* For now, return next node ID in sequence */
    uint32_t my_id = ha_get_node_id();
    uint32_t next_id = my_id + 1;
    
    /* Wrap around if needed (assuming max 5 masters) */
    if (next_id > 5) {
        next_id = 1;
    }
    
    /* Skip self */
    if (next_id == my_id) {
        next_id++;
        if (next_id > 5) {
            next_id = 1;
        }
    }
    
    return next_id;
}

/* Process metadata entry from peer */
static void metasync_process_entry(uint64_t version, const uint8_t *data, uint32_t length) {
    uint64_t old_metaversion = meta_version();
    
    /* Directly replay the entry without CRDT */
    changelog_replay_entry(version, (const char *)data);
    
    uint64_t new_metaversion = meta_version();
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "metasync: replayed entry v%"PRIu64" (%u bytes) - metaversion %"PRIu64" -> %"PRIu64, 
            version, length, old_metaversion, new_metaversion);
}

/* Callback to send a changelog entry to a peer */
void metasync_send_entry_to_peer(void *userdata, uint64_t version, uint8_t *data, uint32_t length) {
    uint32_t peerid = (uint32_t)(uintptr_t)userdata;
    uint8_t *msg;
    uint8_t *ptr;
    uint32_t msglen = 1 + 8 + 4 + length; /* type + version + length + data */
    
    msg = malloc(msglen);
    if (!msg) {
        return;
    }
    
    ptr = msg;
    put8bit(&ptr, METASYNC_ENTRY);
    put64bit(&ptr, version);
    put32bit(&ptr, length);
    memcpy(ptr, data, length);
    
    haconn_send_meta_sync_to_peer(peerid, msg, msglen);
    free(msg);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "metasync: sent entry v%"PRIu64" to peer %u (%u bytes)",
            version, peerid, length);
}

/* Callback to send a changelog entry to a peer */
static void metasync_send_entry_to_peer(void *userdata, uint64_t version, uint8_t *data, uint32_t length) {
    uint32_t peerid = (uint32_t)(uintptr_t)userdata;
    uint8_t *msg;
    uint8_t *ptr;
    uint32_t msglen = 1 + 8 + 4 + length; /* type + version + length + data */
    
    msg = malloc(msglen);
    if (!msg) {
        return;
    }
    
    ptr = msg;
    put8bit(&ptr, METASYNC_ENTRY);
    put64bit(&ptr, version);
    put32bit(&ptr, length);
    memcpy(ptr, data, length);
    
    haconn_send_meta_sync_to_peer(peerid, msg, msglen);
    free(msg);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "metasync: sent entry v%"PRIu64" to peer %u (%u bytes)",
            version, peerid, length);
}

/* Handle incoming metadata sync messages */
void metasync_handle_message(uint32_t peerid, const uint8_t *data, uint32_t length) {
    const uint8_t *ptr = data;
    uint8_t msgtype;
    
    if (length < 1) {
        return;
    }
    
    msgtype = get8bit(&ptr);
    length--;
    
    switch (msgtype) {
        case METASYNC_VERSION_REQ: {
            /* Peer is asking for our version */
            if (length >= 8) {
                uint64_t peer_version = get64bit(&ptr);
                uint8_t resp[10];
                uint8_t *rptr = resp;
                
                /* Send our version back */
                put8bit(&rptr, METASYNC_VERSION_RESP);
                put64bit(&rptr, meta_version());
                
                haconn_send_meta_sync_to_peer(peerid, resp, 9);
                
                mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "metasync: sent version %"PRIu64" to peer %u (peer has %"PRIu64")", 
                        meta_version(), peerid, peer_version);
            }
            break;
        }
        
        case METASYNC_VERSION_RESP: {
            /* Peer is reporting their version */
            if (length >= 8) {
                uint64_t peer_version = get64bit(&ptr);
                metasync_process_version_response(peerid, peer_version);
            }
            break;
        }
        
        case METASYNC_RANGE_REQ: {
            /* Peer is requesting a range of entries */
            if (length >= 16) {
                uint64_t from_version = get64bit(&ptr);
                uint64_t to_version = get64bit(&ptr);
                
                mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "metasync: peer %u requested versions [%"PRIu64"-%"PRIu64"]",
                        peerid, from_version, to_version);
                
                /* Use the same mechanism as metalogger sync */
                changelog_get_old_changes(from_version,
                    metasync_send_entry_to_peer, (void*)(uintptr_t)peerid,
                    (to_version - from_version + 1));
                
                /* Send done message when finished */
                uint8_t done_msg[1];
                done_msg[0] = METASYNC_DONE;
                haconn_send_meta_sync_to_peer(peerid, done_msg, 1);
            }
            break;
        }
        
        case METASYNC_ENTRY: {
            /* Received a metadata entry */
            if (length >= 12) {
                uint64_t version = get64bit(&ptr);
                uint32_t entry_size = get32bit(&ptr);
                
                if (length >= 12 + entry_size) {
                    /* Process the entry */
                    metasync_process_entry(version, ptr, entry_size);
                    
                    /* Send ACK to sender */
                    uint8_t ack_msg[10];
                    uint8_t *ack_ptr = ack_msg;
                    put8bit(&ack_ptr, RING_MSG_ACK);
                    put64bit(&ack_ptr, version);
                    haconn_send_meta_sync_to_peer(peerid, ack_msg, 9);
                    
                    /* Forward to next node in ring if enabled */
                    if (cfg_getnum("HA_RING_FORWARD", 1) && !raft_is_leader()) {
                        ringrepl_ship_entry(version, ptr, entry_size);
                    }
                }
            }
            break;
        }
        
        case METASYNC_DONE: {
            /* Peer finished sending entries */
            pthread_mutex_lock(&sync_state.mutex);
            if (sync_state.state == SYNC_RECEIVING) {
                sync_state.state = SYNC_COMPLETE;
                pthread_cond_signal(&sync_state.cond);
                mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "metasync: sync complete from peer %u", peerid);
            }
            pthread_mutex_unlock(&sync_state.mutex);
            break;
        }
        
        /* Ring replication messages */
        case RING_MSG_BATCH_START: {
            if (length >= 20) {
                uint64_t start_version = get64bit(&ptr);
                uint64_t end_version = get64bit(&ptr);
                uint32_t sender_id = get32bit(&ptr);
                mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "metasync: batch start [%"PRIu64"-%"PRIu64"] from peer %u",
                        start_version, end_version, sender_id);
            }
            break;
        }
        
        case RING_MSG_BATCH_END: {
            if (length >= 12) {
                uint64_t end_version = get64bit(&ptr);
                uint32_t count = get32bit(&ptr);
                mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "metasync: batch end v%"PRIu64" (%u entries) from peer %u",
                        end_version, count, peerid);
                
                /* Send batch ACK */
                uint8_t ack_msg[10];
                uint8_t *ack_ptr = ack_msg;
                put8bit(&ack_ptr, RING_MSG_ACK);
                put64bit(&ack_ptr, end_version);
                haconn_send_meta_sync_to_peer(peerid, ack_msg, 9);
            }
            break;
        }
        
        case RING_MSG_ACK:
        case RING_MSG_NACK:
        case RING_MSG_RESYNC_REQ:
        case RING_MSG_HEARTBEAT: {
            /* Forward to ring replication handler */
            extern void ringrepl_handle_message(uint32_t peerid, const uint8_t *data, uint32_t length);
            ringrepl_handle_message(peerid, data - 1, length + 1); /* Include msgtype */
            break;
        }
        
        default:
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "metasync: unknown message type %u from peer %u", msgtype, peerid);
            break;
    }
}

/* Perform initial metadata sync on startup */
int metasync_startup_sync(void) {
    double start_time;
    int result = 0;
    
    pthread_mutex_lock(&sync_state.mutex);
    
    /* Initialize sync state */
    sync_state.state = SYNC_QUERY_VERSIONS;
    sync_state.local_version = meta_version();
    sync_state.highest_peer_version = sync_state.local_version;
    sync_state.sync_from_nodeid = 0;
    sync_state.peers_responded = 0;
    sync_state.start_time = start_time = monotonic_seconds();
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "metasync: starting metadata sync (local version %"PRIu64")", 
            sync_state.local_version);
    
    /* Send version query to all peers */
    pthread_mutex_unlock(&sync_state.mutex);
    metasync_send_version_query();
    pthread_mutex_lock(&sync_state.mutex);
    
    /* Wait for version responses or timeout */
    while (sync_state.state == SYNC_QUERY_VERSIONS) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 5; /* 5 second timeout for version query */
        
        if (pthread_cond_timedwait(&sync_state.cond, &sync_state.mutex, &ts) == ETIMEDOUT) {
            /* Timeout - proceed with what we have */
            if (sync_state.peers_responded == 0) {
                /* No peers responded - we're alone or network issue */
                mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "metasync: no peers responded to version query");
                sync_state.state = SYNC_COMPLETE;
            } else if (sync_state.highest_peer_version > sync_state.local_version) {
                /* Some peers responded and have newer data */
                sync_state.state = SYNC_REQUEST_DATA;
            } else {
                /* We're up to date */
                sync_state.state = SYNC_COMPLETE;
            }
            break;
        }
    }
    
    /* Request data if needed */
    if (sync_state.state == SYNC_REQUEST_DATA && sync_state.sync_from_nodeid > 0) {
        uint64_t from_version = sync_state.local_version + 1;
        
        sync_state.state = SYNC_RECEIVING;
        pthread_mutex_unlock(&sync_state.mutex);
        
        /* Request missing entries */
        metasync_send_range_request(sync_state.sync_from_nodeid, from_version);
        
        pthread_mutex_lock(&sync_state.mutex);
        
        /* Wait for sync to complete or timeout */
        while (sync_state.state == SYNC_RECEIVING) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += (int)sync_state.timeout;
            
            if (pthread_cond_timedwait(&sync_state.cond, &sync_state.mutex, &ts) == ETIMEDOUT) {
                mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "metasync: timeout waiting for sync data");
                sync_state.state = SYNC_ERROR;
                result = -1;
                break;
            }
        }
    }
    
    /* Log final status */
    if (sync_state.state == SYNC_COMPLETE) {
        double elapsed = monotonic_seconds() - start_time;
        uint64_t final_version = meta_version();
        
        if (final_version > sync_state.local_version) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
                    "metasync: sync complete in %.1fs, version %"PRIu64" -> %"PRIu64" (+%"PRIu64" entries)", 
                    elapsed, sync_state.local_version, final_version, 
                    final_version - sync_state.local_version);
        } else {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
                    "metasync: already up to date (version %"PRIu64"), completed in %.1fs", 
                    final_version, elapsed);
        }
    } else if (sync_state.state == SYNC_ERROR) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "metasync: sync failed");
        result = -1;
    }
    
    sync_state.state = SYNC_IDLE;
    pthread_mutex_unlock(&sync_state.mutex);
    
    return result;
}

/* Get sync status for monitoring */
void metasync_get_status(uint64_t *local_version, uint64_t *highest_peer_version, 
                        uint32_t *peers_synced, uint32_t *peers_total) {
    pthread_mutex_lock(&sync_state.mutex);
    
    if (local_version) {
        *local_version = sync_state.local_version;
    }
    if (highest_peer_version) {
        *highest_peer_version = sync_state.highest_peer_version;
    }
    if (peers_synced) {
        *peers_synced = sync_state.peers_responded;
    }
    if (peers_total) {
        *peers_total = sync_state.peers_total;
    }
    
    pthread_mutex_unlock(&sync_state.mutex);
}

/* Update filesystem counters after metadata sync */
void metasync_update_fs_counters(void) {
    extern void fs_printinfo(void);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "metasync: updating filesystem counters after sync");
    
    /* Print the current counters to logs */
    fs_printinfo();
}

/* Request specific version range from a peer */
void metasync_request_versions(uint32_t node_id, uint64_t from_version, uint64_t to_version) {
    uint8_t msg[20];
    uint8_t *ptr = msg;
    
    /* Build message: type + from_version + to_version */
    put8bit(&ptr, METASYNC_RANGE_REQ);
    put64bit(&ptr, from_version);
    put64bit(&ptr, to_version);
    
    /* Send to specific peer */
    haconn_send_meta_sync_to_peer(node_id, msg, 17);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "metasync: requesting versions %"PRIu64"-%"PRIu64" from node %u", 
            from_version, to_version, node_id);
}

/* Send changelog entry to ring successor for log shipping */
void metasync_ship_to_ring(uint64_t version, const uint8_t *data, uint32_t length) {
    uint32_t successor;
    uint8_t *msg;
    uint8_t *ptr;
    
    /* Only the leader initiates ring shipping */
    if (!raft_is_leader()) {
        return;
    }
    
    /* Get ring successor */
    successor = get_ring_successor();
    
    /* Build message: type + version + length + data */
    msg = malloc(13 + length);
    if (!msg) {
        return;
    }
    
    ptr = msg;
    put8bit(&ptr, METASYNC_ENTRY);
    put64bit(&ptr, version);
    put32bit(&ptr, length);
    memcpy(ptr, data, length);
    
    /* Send to successor */
    haconn_send_meta_sync_to_peer(successor, msg, 13 + length);
    free(msg);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "ring-ship: sent v%"PRIu64" to successor %u (%u bytes)", 
            version, successor, length);
}