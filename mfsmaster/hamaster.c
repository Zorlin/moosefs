/*
 * MooseFS HA Master Integration
 * Provides HA cluster functionality integrated into mfsmaster
 */

#include "hamaster.h"
#include "cfg.h"
#include "mfslog.h"
#include "shardmgr.h"
#include "haconn.h"
#include "crdtstore.h"
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
    
    // Check if HA mode is explicitly enabled
    ha_enabled = cfg_getnum("HA_ENABLED", 0);
    
    // If not explicitly set, detect from MFSHA_PEERS configuration
    if (!ha_enabled) {
        peers = cfg_getstr("MFSHA_PEERS", "");
        if (peers && strlen(peers) > 0) {
            // If peers are configured, enable HA mode
            ha_enabled = 1;
            ha_peers = strdup(peers);
            mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA mode auto-detected from MFSHA_PEERS configuration");
        }
        if (peers) {
            free(peers);
        }
    }
    
    // Get node ID for HA cluster
    if (ha_enabled) {
        ha_node_id = cfg_getnum("MFSHA_NODE_ID", 0);
        if (ha_node_id == 0) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "HA mode enabled but MFSHA_NODE_ID not configured");
            ha_enabled = 0;
            return -1;
        }
        
        if (!ha_peers) {
            ha_peers = cfg_getstr("MFSHA_PEERS", "");
            if (!ha_peers || strlen(ha_peers) == 0) {
                mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "HA mode enabled but MFSHA_PEERS not configured");
                ha_enabled = 0;
                return -1;
            }
        }
        
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA mode enabled: node_id=%"PRIu32", peers=%s", ha_node_id, ha_peers);
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
        {shardmgr_init, "shard manager"},
        {crdtstore_init, "CRDT store"},
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
    crdtstore_term();
    shardmgr_term();
    
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