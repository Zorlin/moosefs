/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 * 
 * Metadata synchronization for HA startup
 */

#ifndef _METASYNC_H_
#define _METASYNC_H_

#include <inttypes.h>

/* Initialize metadata sync subsystem */
int metasync_init(void);

/* Terminate metadata sync subsystem */
void metasync_term(void);

/* Perform initial metadata sync on startup
 * This function will:
 * 1. Query all HA peers for their metadata version
 * 2. Determine if we need to sync from a peer
 * 3. Request and apply missing metadata (CRDT differences)
 * 4. Return when sync is complete or timeout
 * Returns 0 on success, -1 on error
 */
int metasync_startup_sync(void);

/* Handle incoming metadata sync messages */
void metasync_handle_message(uint32_t peerid, const uint8_t *data, uint32_t length);

/* Get sync status for monitoring */
void metasync_get_status(uint64_t *local_version, uint64_t *highest_peer_version, 
                        uint32_t *peers_synced, uint32_t *peers_total);

#endif /* _METASYNC_H_ */