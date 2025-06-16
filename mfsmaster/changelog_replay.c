/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 * 
 * Changelog replay for HA synchronization
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "changelog_replay.h"
#include "restore.h"
#include "metadata.h"
#include "MFSCommunication.h"
#include "mfslog.h"
#include "massert.h"
#include "hamaster.h"
#include "main.h"

/* Declare restore_line function */
int restore_line(const char *filename,uint64_t lv,const char *line,uint32_t *rts);

static pthread_mutex_t replay_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint64_t highest_replayed_version = 0;

/* Initialize changelog replay subsystem */
int changelog_replay_init(void) {
    pthread_mutex_lock(&replay_mutex);
    highest_replayed_version = meta_version();
    pthread_mutex_unlock(&replay_mutex);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "changelog_replay: initialized with version %"PRIu64, 
            highest_replayed_version);
    
    return 0;
}

/* Process a changelog entry received from another master */
int changelog_replay_entry(uint64_t version, const char *entry) {
    uint32_t rts = 0;
    int result;
    
    pthread_mutex_lock(&replay_mutex);
    
    /* Check if we've already processed this version */
    if (version <= highest_replayed_version) {
        pthread_mutex_unlock(&replay_mutex);
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "changelog_replay: skipping already processed version %"PRIu64, version);
        return 0;
    }
    
    /* Check if this is the next expected version */
    if (version != highest_replayed_version + 1) {
        /* Handle version gaps */
        if (version > highest_replayed_version + 1) {
            /* We have a gap - must request missing versions first */
            uint64_t gap = version - highest_replayed_version - 1;
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "changelog_replay: version gap detected - expected %"PRIu64", got %"PRIu64" (gap=%"PRIu64")",
                    highest_replayed_version + 1, version, gap);
            /* Request missing versions from gap */
            uint64_t missing_start = highest_replayed_version + 1;
            uint64_t missing_end = version - 1;
            mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "changelog_replay: requesting missing versions [%"PRIu64"-%"PRIu64"]",
                    missing_start, missing_end);
            /* This will be handled by the HA module to fetch from peers */
            ha_request_missing_changelog_range(missing_start, missing_end);
            /* Store this entry for later replay after gap is filled */
            /* For now, we must reject it to avoid skipping versions */
            pthread_mutex_unlock(&replay_mutex);
            return 0;  /* Don't process out-of-order entries */
        } else {
            /* Version is older than expected, skip it */
            pthread_mutex_unlock(&replay_mutex);
            mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "changelog_replay: skipping old version %"PRIu64" (expected %"PRIu64")",
                    version, highest_replayed_version + 1);
            return 0;
        }
    }
    
    /* Parse and execute the changelog entry */
    result = restore_line("HA_REPLAY", version, entry, &rts);
    
    if (result < 0) {
        pthread_mutex_unlock(&replay_mutex);
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "changelog_replay: failed to parse entry v%"PRIu64": %s", 
                version, entry);
        return -1;
    }
    
    if (result != MFS_STATUS_OK) {
        /* Some operations may fail due to missing dependencies (e.g., sessions) */
        /* Log but continue to avoid blocking synchronization */
        if (result == MFS_ERROR_MISMATCH || result == MFS_ERROR_NOTFOUND || result == MFS_ERROR_BADSESSIONID) {
            /* Check if this is a session-related operation */
            if (strstr(entry, "SESADD") != NULL) {
                mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "changelog_replay: session operation skipped v%"PRIu64" status=%d (session conflict): %s", 
                        version, result, entry);
            } else {
                mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "changelog_replay: operation skipped v%"PRIu64" status=%d (missing dependency): %s", 
                        version, result, entry);
            }
        } else if (result == MFS_ERROR_EEXIST && strstr(entry, "SESADD") != NULL) {
            /* Session already exists - this is fine in HA scenario */
            mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "changelog_replay: session already exists v%"PRIu64": %s", 
                    version, entry);
        } else {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "changelog_replay: operation failed v%"PRIu64" status=%d: %s", 
                    version, result, entry);
        }
        /* Continue anyway to not block synchronization */
    }
    
    /* Update our highest replayed version */
    if (version > highest_replayed_version) {
        highest_replayed_version = version;
        
        /* Also update the main metadata version to stay in sync */
        /* Only update if this is a newer version */
        uint64_t current_meta_version = meta_version();
        if (current_meta_version < version) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "changelog_replay: updating metadata version from %"PRIu64" to %"PRIu64, 
                    current_meta_version, version);
            meta_set_version(version);
        } else if (current_meta_version > version) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "changelog_replay: metadata version %"PRIu64" is ahead of replayed version %"PRIu64, 
                    current_meta_version, version);
        }
    }
    
    pthread_mutex_unlock(&replay_mutex);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "changelog_replay: successfully replayed v%"PRIu64" (meta_version now %"PRIu64"): %.100s%s", 
            version, meta_version(), entry, strlen(entry) > 100 ? "..." : "");
    
    return 0;
}

/* Sync changelogs from a specific version */
int changelog_replay_sync_from_version(uint64_t start_version) {
    /* TODO: Implement non-CRDT based sync mechanism */
    mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "changelog_replay: sync_from_version not implemented without CRDT");
    return 0;
}

/* Get the highest replayed version */
uint64_t changelog_replay_get_version(void) {
    uint64_t version;
    
    pthread_mutex_lock(&replay_mutex);
    version = highest_replayed_version;
    pthread_mutex_unlock(&replay_mutex);
    
    return version;
}

/* Update the highest replayed version after metadata load */
void changelog_replay_update_version(uint64_t version) {
    pthread_mutex_lock(&replay_mutex);
    if (version > highest_replayed_version) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "changelog_replay: updating version from %"PRIu64" to %"PRIu64" after metadata load", 
                highest_replayed_version, version);
        highest_replayed_version = version;
    }
    pthread_mutex_unlock(&replay_mutex);
}