/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 * 
 * Ring Replication for reliable changelog distribution
 */

#ifndef _RINGREPL_H_
#define _RINGREPL_H_

#include <inttypes.h>

/* Ring replication states */
typedef enum {
    RING_STATE_IDLE,
    RING_STATE_SYNCING,
    RING_STATE_WAITING_ACK,
    RING_STATE_RETRYING,
    RING_STATE_ERROR
} ring_state_t;

/* Ring sync progress tracking */
typedef struct ring_sync_progress {
    uint32_t peer_id;
    uint64_t last_sent_version;
    uint64_t last_acked_version;
    uint64_t target_version;
    uint32_t retry_count;
    double last_attempt_time;
    double last_success_time;
    ring_state_t state;
    struct ring_sync_progress *next;
} ring_sync_progress_t;

/* Initialize ring replication subsystem */
int ringrepl_init(void);

/* Terminate ring replication subsystem */
void ringrepl_term(void);

/* Get ring successor for a node */
uint32_t ringrepl_get_successor(uint32_t node_id);

/* Get all nodes in ring order */
int ringrepl_get_ring_order(uint32_t *nodes, uint32_t *count);

/* Ship changelog entry through ring */
int ringrepl_ship_entry(uint64_t version, const uint8_t *data, uint32_t length);

/* Handle incoming ring replication message */
void ringrepl_handle_message(uint32_t peerid, const uint8_t *data, uint32_t length);

/* Resume sync for a specific peer */
int ringrepl_resume_sync(uint32_t peer_id);

/* Get sync progress for monitoring */
void ringrepl_get_sync_status(uint32_t peer_id, uint64_t *last_sent, 
                             uint64_t *last_acked, ring_state_t *state);

/* Periodic maintenance */
void ringrepl_periodic_check(void);

/* Force full resync with peer */
int ringrepl_force_resync(uint32_t peer_id, uint64_t from_version);

/* Ring message types */
#define RING_MSG_ENTRY          0x11
#define RING_MSG_ACK            0x12
#define RING_MSG_NACK           0x13
#define RING_MSG_RESYNC_REQ     0x14
#define RING_MSG_RESYNC_RESP    0x15
#define RING_MSG_BATCH_START    0x16
#define RING_MSG_BATCH_END      0x17
#define RING_MSG_HEARTBEAT      0x18

/* Ring configuration */
#define RING_MAX_BATCH_SIZE     1000
#define RING_ACK_TIMEOUT        5.0     /* 5 seconds */
#define RING_RETRY_INTERVAL     2.0     /* 2 seconds */
#define RING_MAX_RETRIES        10
#define RING_HEARTBEAT_INTERVAL 10.0    /* 10 seconds */

#endif /* _RINGREPL_H_ */