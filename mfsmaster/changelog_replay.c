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
#include "crdtstore.h"
#include "main.h"

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
        pthread_mutex_unlock(&replay_mutex);
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "changelog_replay: version gap detected - expected %"PRIu64", got %"PRIu64,
                highest_replayed_version + 1, version);
        /* TODO: Request missing versions */
        return -1;
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
        pthread_mutex_unlock(&replay_mutex);
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "changelog_replay: operation failed v%"PRIu64" status=%d: %s", 
                version, result, entry);
        return -1;
    }
    
    /* Update our version */
    highest_replayed_version = version;
    
    /* Also update the main metadata version to stay in sync */
    if (meta_version() < version) {
        meta_set_version(version);
    }
    
    pthread_mutex_unlock(&replay_mutex);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "changelog_replay: successfully replayed v%"PRIu64": %.100s%s", 
            version, entry, strlen(entry) > 100 ? "..." : "");
    
    return 0;
}

/* Sync changelogs from a specific version */
int changelog_replay_sync_from_version(uint64_t start_version) {
    crdt_store_t *store;
    uint64_t current_version;
    uint64_t version;
    int synced = 0;
    
    store = crdtstore_get_main_store();
    if (store == NULL) {
        return -1;
    }
    
    pthread_mutex_lock(&replay_mutex);
    current_version = highest_replayed_version;
    pthread_mutex_unlock(&replay_mutex);
    
    /* Sync all versions from start_version to current */
    for (version = start_version; version <= current_version; version++) {
        crdt_entry_t *entry;
        
        /* Try to get this version from CRDT store */
        entry = crdtstore_get(store, version);
        if (entry != NULL && entry->value != NULL) {
            /* Found the changelog entry, replay it */
            if (changelog_replay_entry(version, (const char *)entry->value) == 0) {
                synced++;
            }
        }
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "changelog_replay: synced %d entries from version %"PRIu64" to %"PRIu64,
            synced, start_version, current_version);
    
    return synced;
}

/* Get the highest replayed version */
uint64_t changelog_replay_get_version(void) {
    uint64_t version;
    
    pthread_mutex_lock(&replay_mutex);
    version = highest_replayed_version;
    pthread_mutex_unlock(&replay_mutex);
    
    return version;
}