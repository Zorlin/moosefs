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

#endif