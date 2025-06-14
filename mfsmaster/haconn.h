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

#endif
