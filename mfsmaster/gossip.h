/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 */

#ifndef _GOSSIP_H_
#define _GOSSIP_H_

#include <stdio.h>
#include <poll.h>
#include <inttypes.h>

/* Gossip Protocol - maintains cluster membership */

int gossip_init(void);
void gossip_term(void);

/* Periodic maintenance */
void gossip_tick(double now);
void gossip_reload(void);

/* Network integration */
void gossip_desc(struct pollfd *pdesc, uint32_t *ndesc);
void gossip_serve(struct pollfd *pdesc);
void gossip_info(FILE *fd);

#endif
