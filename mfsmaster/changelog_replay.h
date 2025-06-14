/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 * 
 * Changelog replay for HA synchronization
 */

#ifndef _CHANGELOG_REPLAY_H_
#define _CHANGELOG_REPLAY_H_

#include <inttypes.h>

/* Initialize changelog replay subsystem */
int changelog_replay_init(void);

/* Process a changelog entry received from another master */
int changelog_replay_entry(uint64_t version, const char *entry);

/* Sync changelogs from a specific version */
int changelog_replay_sync_from_version(uint64_t start_version);

/* Get the highest replayed version */
uint64_t changelog_replay_get_version(void);

#endif /* _CHANGELOG_REPLAY_H_ */