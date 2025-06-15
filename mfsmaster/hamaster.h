/*
 * MooseFS HA Master Integration
 */

#ifndef _HAMASTER_H_
#define _HAMASTER_H_

#include <stdint.h>

/* HA Mode Detection and Control */
int ha_mode_enabled(void);
int ha_initialize(void);
void ha_terminate(void);

/* HA Configuration */
uint32_t ha_get_node_id(void);
const char* ha_get_peers(void);

/* Perform metadata sync after initial load
 * This should be called after metadata is loaded from disk or peer
 * It will sync any missing changelog entries via CRDT
 * Returns 0 on success, -1 on error
 */
int ha_metadata_sync(void);

/* Request missing changelog entries from peers
 * This is called when a version gap is detected during replay
 * The HA module will attempt to fetch the missing entries from other nodes
 */
void ha_request_missing_changelog_range(uint64_t start_version, uint64_t end_version);

#endif