/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 * 
 * Simplified Global Version Coordinator using Raft consensus
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>

#include "gvc.h"
#include "main.h"
#include "cfg.h"
#include "mfslog.h"
#include "massert.h"
#include "raftconsensus.h"
#include "metadata.h"

/* Initialize GVC */
int gvc_init(void) {
    /* GVC is now just a thin wrapper around Raft */
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "GVC initialized (using Raft consensus)");
    return 0;
}

/* Terminate GVC */
void gvc_term(void) {
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "GVC terminated");
}

/* Reload configuration */
void gvc_reload(void) {
    /* Nothing to reload */
}

/* Get next version number */
uint64_t gvc_get_next_version(void) {
    /* Use Raft's version allocation with lease optimization */
    return raft_get_next_version();
}

/* Add provisional operation - not used in simplified design */
int gvc_add_provisional_op(const void *op_data, uint32_t op_size, versioned_op_t *versioned) {
    /* Not implemented in simplified design */
    return -1;
}

/* Finalize operation - not used in simplified design */
int gvc_finalize_op(uint32_t op_hash, uint64_t assigned_version) {
    /* Not implemented in simplified design */
    return -1;
}

/* Update client view - not used in simplified design */
int gvc_update_client_view(uint32_t session_id, uint64_t seen_version) {
    /* Not implemented in simplified design */
    return 0;
}

/* Get client view - not used in simplified design */
uint64_t gvc_get_client_view(uint32_t session_id) {
    return 0;
}

/* Become GVC leader - handled by Raft */
int gvc_become_leader(void) {
    /* Raft handles leader election */
    return 0;
}

/* Get current role */
uint8_t gvc_get_role(void) {
    raft_state_t state = raft_get_state();
    
    switch (state) {
        case RAFT_STATE_LEADER:
            return GVC_ROLE_LEADER;
        case RAFT_STATE_FOLLOWER:
            return GVC_ROLE_FOLLOWER;
        case RAFT_STATE_CANDIDATE:
            return GVC_ROLE_CANDIDATE;
        default:
            return GVC_ROLE_NONE;
    }
}

/* Get statistics */
void gvc_get_stats(uint64_t *current_version, uint64_t *allocated_versions, 
                   uint32_t *pending_ops, uint32_t *active_clients) {
    raft_stats_t stats;
    
    raft_get_stats(&stats);
    
    if (current_version) *current_version = stats.current_version;
    if (allocated_versions) *allocated_versions = stats.current_version; /* Total allocated */
    if (pending_ops) *pending_ops = 0;
    if (active_clients) *active_clients = 0;
}

/* Display GVC information */
void gvc_info(FILE *fd) {
    raft_stats_t stats;
    
    raft_get_stats(&stats);
    
    fprintf(fd, "[Global Version Coordinator]\n");
    fprintf(fd, "mode: Raft consensus\n");
    fprintf(fd, "role: %s\n", 
            stats.current_state == RAFT_STATE_LEADER ? "LEADER" :
            stats.current_state == RAFT_STATE_FOLLOWER ? "FOLLOWER" :
            stats.current_state == RAFT_STATE_CANDIDATE ? "CANDIDATE" : "NONE");
    fprintf(fd, "leader_node: %"PRIu32"\n", stats.current_leader);
    fprintf(fd, "current_version: %"PRIu64"\n", stats.current_version);
    fprintf(fd, "current_term: %"PRIu64"\n", stats.current_term);
    fprintf(fd, "\n");
}