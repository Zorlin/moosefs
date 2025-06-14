/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 */

#ifndef _SHARDMGR_H_
#define _SHARDMGR_H_

#include <inttypes.h>
#include <stdio.h>
#include <poll.h>

/* Shard Manager - handles metadata sharding with Blake3 hash */

int shardmgr_init(void);
void shardmgr_term(void);

/* Periodic maintenance */
void shardmgr_tick(double now);
void shardmgr_reload(void);

/* Network integration */
void shardmgr_desc(struct pollfd *pdesc, uint32_t *ndesc);
void shardmgr_serve(struct pollfd *pdesc);
void shardmgr_info(FILE *fd);

/* Shard routing */
uint32_t shardmgr_get_shard(uint32_t inode);
uint32_t shardmgr_get_shard_count(void);
int shardmgr_is_local_shard(uint32_t shard_id);

#endif
