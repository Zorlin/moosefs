/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 */

#ifndef _METASYNCER_H_
#define _METASYNCER_H_

#include <stdio.h>
#include <poll.h>
#include <inttypes.h>

/* Metadata Synchronizer - integrates with existing metadata.mfs */

int metasyncer_init(void);
void metasyncer_term(void);

/* Periodic maintenance */
void metasyncer_tick(double now);
void metasyncer_reload(void);

/* Network integration */
void metasyncer_desc(struct pollfd *pdesc, uint32_t *ndesc);
void metasyncer_serve(struct pollfd *pdesc);
void metasyncer_info(FILE *fd);

#endif
