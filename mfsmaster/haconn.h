/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 */

#ifndef _HACONN_H_
#define _HACONN_H_

#include <stdio.h>
#include <stdint.h>
#include <poll.h>

/* Module initialization and cleanup */
int haconn_init(void);
void haconn_term(void);

/* Main loop integration */
void haconn_desc(struct pollfd *pdesc, uint32_t *ndesc);
void haconn_serve(struct pollfd *pdesc);

/* Configuration and status */
void haconn_reload(void);
void haconn_info(FILE *fd);

/* Inter-MDS communication functions */
void haconn_send_crdt_delta(const uint8_t *data, uint32_t length);
void haconn_send_raft_request(uint32_t peerid, const uint8_t *data, uint32_t length);
void haconn_send_raft_response(uint32_t peerid, const uint8_t *data, uint32_t length);

/* Send metadata sync message to all peers */
void haconn_send_meta_sync(const uint8_t *data, uint32_t length);

/* Send metadata sync message to specific peer */
void haconn_send_meta_sync_to_peer(uint32_t peerid, const uint8_t *data, uint32_t length);

/* Send Raft message to all peers */
void haconn_send_raft_broadcast(const uint8_t *data, uint32_t length);

/* Send changelog entry to all peers */
void haconn_send_changelog_entry(uint64_t version, const uint8_t *data, uint32_t length);

/* Get leader connection information for client redirection */
int haconn_get_leader_info(uint32_t leader_id, uint32_t *leader_ip, uint16_t *leader_port);

#endif
