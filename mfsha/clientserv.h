/*
 * MooseFS HA Client Server Module
 */

#ifndef _CLIENTSERV_H_
#define _CLIENTSERV_H_

#include <stdint.h>
#include <sys/poll.h>

/* Module initialization and cleanup */
int clientserv_init(void);
void clientserv_term(void);

/* Main loop integration */
void clientserv_desc(struct pollfd *pdesc, uint32_t *ndesc);
void clientserv_serve(struct pollfd *pdesc, uint32_t ndesc);

#endif