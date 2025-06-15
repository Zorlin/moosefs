/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 * 
 * Global Version Coordinator (GVC) for distributed version assignment
 */

#ifndef _GVC_H_
#define _GVC_H_

#include <inttypes.h>


typedef struct {
    uint64_t global_version;      /* MooseFS compatibility */
    uint64_t local_timestamp;     /* Lamport timestamp */
    uint32_t operation_hash;      /* Operation identifier */
    uint8_t  state;              /* PROVISIONAL or VERSIONED */
} versioned_op_t;

/* States for versioned operations */
#define OP_STATE_PROVISIONAL 0
#define OP_STATE_VERSIONED   1

/* GVC role states */
#define GVC_ROLE_NONE        0
#define GVC_ROLE_CANDIDATE   1
#define GVC_ROLE_LEADER      2
#define GVC_ROLE_FOLLOWER    3


/* Initialization and lifecycle */
int gvc_init(void);
void gvc_term(void);
void gvc_reload(void);

/* Version allocation API */
uint64_t gvc_get_next_version(void);

/* Provisional operation management */
int gvc_add_provisional_op(const void *op_data, uint32_t op_size, versioned_op_t *versioned);
int gvc_finalize_op(uint32_t op_hash, uint64_t assigned_version);
int gvc_get_pending_ops(versioned_op_t **ops, uint32_t *count);

/* GVC leadership */
int gvc_become_leader(void);
int gvc_resign_leadership(void);
uint8_t gvc_get_role(void);
uint32_t gvc_get_leader_node_id(void);

/* Client session tracking for monotonic guarantees */
int gvc_update_client_view(uint32_t session_id, uint64_t seen_version);
uint64_t gvc_get_client_view(uint32_t session_id);

/* Monitoring and statistics */
void gvc_get_stats(uint64_t *current_version, uint64_t *allocated_versions, 
                   uint32_t *pending_ops, uint32_t *active_clients);
void gvc_info(FILE *fd);

/* Network integration for distributed GVC */
int gvc_handle_leader_election(uint32_t proposing_node, uint64_t proposed_version);

#endif /* _GVC_H_ */