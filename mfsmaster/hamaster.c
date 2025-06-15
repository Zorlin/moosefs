/*
 * MooseFS HA Master Integration
 * Provides HA cluster functionality integrated into mfsmaster
 */

#include "hamaster.h"
#include "cfg.h"
#include "mfslog.h"
#include "haconn.h"
#include "raftconsensus.h"
#include "gossip.h"
#include "gvc.h"
#include "changelog_replay.h"
#include "metasync.h"
#include <string.h>
#include <stdlib.h>

static int ha_enabled = 0;
static char *ha_peers = NULL;
static uint32_t ha_node_id = 0;

int ha_mode_enabled(void) {
    return ha_enabled;
}

int ha_detect_mode(void) {
    char *peers;
    
    // Get node ID for HA cluster
    ha_node_id = cfg_getnum("MFSHA_NODE_ID", 0);
    
    // Get peers configuration
    peers = cfg_getstr("MFSHA_PEERS", "");
    
    // Enable HA mode if both node ID and peers are configured
    if (ha_node_id > 0 && peers && strlen(peers) > 0) {
        ha_enabled = 1;
        ha_peers = strdup(peers);
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA mode enabled: node_id=%"PRIu32", peers=%s", ha_node_id, ha_peers);
    } else {
        ha_enabled = 0;
        if (ha_node_id > 0 && (!peers || strlen(peers) == 0)) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "MFSHA_NODE_ID set but MFSHA_PEERS not configured - HA mode disabled");
        } else if (ha_node_id == 0 && peers && strlen(peers) > 0) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "MFSHA_PEERS set but MFSHA_NODE_ID not configured - HA mode disabled");
        }
    }
    
    if (peers) {
        free(peers);
    }
    
    return 0;
}

int ha_initialize(void) {
    uint32_t i;
    
    // First detect if HA mode should be enabled
    if (ha_detect_mode() < 0) {
        return -1;
    }
    
    // If HA mode is not enabled, return success (no-op)
    if (!ha_enabled) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA mode disabled, running in single master mode");
        return 0;
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Initializing HA cluster support...");
    
    // Initialize HA modules in order
    struct {
        int (*fn)(void);
        char *name;
    } ha_modules[] = {
        {raftconsensus_init, "Raft consensus"},
        {gossip_init, "gossip protocol"},
        {gvc_init, "global version coordinator"},
        {changelog_replay_init, "changelog replay"},
        {metasync_init, "metadata sync"},
        {haconn_init, "HA communication"},
        {NULL, NULL}
    };
    
    for (i = 0; ha_modules[i].fn != NULL; i++) {
        if (ha_modules[i].fn() < 0) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "HA init: %s failed", ha_modules[i].name);
            return -1;
        }
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "HA init: %s initialized", ha_modules[i].name);
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA cluster support initialized successfully");
    return 0;
}

void ha_terminate(void) {
    if (!ha_enabled) {
        return;
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "Terminating HA cluster support...");
    
    // Terminate HA modules in reverse order
    haconn_term();
    metasync_term();
    gossip_term();
    raftconsensus_term();
    
    if (ha_peers) {
        free(ha_peers);
        ha_peers = NULL;
    }
    
    ha_enabled = 0;
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA cluster support terminated");
}

uint32_t ha_get_node_id(void) {
    return ha_node_id;
}

const char* ha_get_peers(void) {
    return ha_peers;
}

int ha_metadata_sync(void) {
    if (!ha_enabled) {
        return 0; /* No-op if HA mode is not enabled */
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA metadata sync: starting CRDT-based differences sync");
    
    /* Perform CRDT-based metadata sync */
    if (metasync_startup_sync() < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "HA metadata sync: failed to sync metadata differences");
        return -1;
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA metadata sync: CRDT differences sync completed successfully");
    return 0;
}

void ha_request_missing_changelog_range(uint64_t start_version, uint64_t end_version) {
    if (!ha_enabled) {
        return; /* No-op if HA mode is not enabled */
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA: requesting missing changelog entries [%"PRIu64"-%"PRIu64"]", 
            start_version, end_version);
    
    /* TODO: Implement protocol to request specific version ranges from peers */
    /* For now, we'll rely on the periodic sync to eventually catch up */
    /* This would involve:
     * 1. Query all connected peers for who has these versions
     * 2. Request the missing entries from the peer with the best connection
     * 3. Replay them through changelog_replay_entry()
     */
}