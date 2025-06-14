/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA.
 */

/* MooseFS High Availability Metadata Server */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <poll.h>

#include "main.h"
#include "cfg.h"
#include "strerr.h"
#include "mfslog.h"
#include "massert.h"
#include "clocks.h"
#include "sockets.h"

#include "init.h"
#include "raftconsensus.h"
#include "crdtstore.h"
#include "shardmgr.h"
#include "metasyncer.h"
#include "gossip.h"
#include "haconn.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)

static const char id[]="@(#) version: " STR(VERSMAJ) "." STR(VERSMID) "." STR(VERSMIN) ", written by Jakub Kruszona-Zawadzki";

/* MooseFS HA Main initialization function */
int mfsha_init(void) {
	/* TODO: Register periodic tasks when main loop is implemented */
	/* main_time_register(1,0,raft_tick); */
	
	/* TODO: Register termination when main loop is implemented */
	/* main_destruct_register(mfsha_term); */
	
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"mfsha main initialized");
	return 0;
}

void mfsha_term(void) {
	haconn_term();
	metasyncer_term();
	gossip_term();
	shardmgr_term();
	raftconsensus_term();
	crdtstore_term();
	
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"mfsha terminated");
}

/* Forward declarations for main loop functions */
static void mfsha_desc(struct pollfd *pdesc, uint32_t *ndesc);
static void mfsha_serve(struct pollfd *pdesc);
static void mfsha_reload(void);
static void mfsha_info(FILE *fd);

/* Global state for graceful shutdown */
static int terminate = 0;
static void term_handler(int sig) {
	(void)sig;
	terminate = 1;
}

/* Main function with full event loop implementation */
int main(int argc, char **argv) {
	uint32_t i;
	struct pollfd pdesc[1000];
	uint32_t ndesc;
	int pollret;
	double now;
	
	(void)argc; /* Suppress unused parameter warning */
	(void)argv;
	
	/* Install signal handlers for graceful shutdown */
	signal(SIGTERM, term_handler);
	signal(SIGINT, term_handler);
	signal(SIGQUIT, term_handler);
	signal(SIGHUP, SIG_IGN);
	signal(SIGPIPE, SIG_IGN);
	
	/* Initialize all modules */
	for (i = 0; RunTab[i].fn != NULL; i++) {
		if (RunTab[i].fn() < 0) {
			mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "mfsha: %s initialization failed", RunTab[i].name);
			return 1;
		}
	}
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "mfsha: all modules initialized, entering main loop");
	
	/* Main event loop */
	while (!terminate) {
		ndesc = 0;
		
		/* Collect file descriptors from all modules */
		mfsha_desc(pdesc, &ndesc);
		haconn_desc(pdesc, &ndesc);
		gossip_desc(pdesc, &ndesc);
		raftconsensus_desc(pdesc, &ndesc);
		
		/* Poll with 1 second timeout */
		pollret = poll(pdesc, ndesc, 1000);
		
		if (pollret < 0) {
			if (errno == EINTR) {
				continue; /* Signal received, check terminate flag */
			}
			mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "poll error: %s", strerror(errno));
			break;
		}
		
		now = monotonic_seconds();
		
		/* Service all modules */
		mfsha_serve(pdesc);
		haconn_serve(pdesc);
		gossip_serve(pdesc);
		raftconsensus_serve(pdesc);
		
		/* Periodic tasks */
		crdtstore_tick(now);
		raftconsensus_tick(now);
		shardmgr_tick(now);
		metasyncer_tick(now);
		gossip_tick(now);
	}
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "mfsha: shutdown initiated");
	
	/* Graceful shutdown */
	mfsha_term();
	return 0;
}

/* Collect file descriptors for polling */
static void mfsha_desc(struct pollfd *pdesc, uint32_t *ndesc) {
	/* Core mfsha module has no sockets of its own */
	(void)pdesc;
	(void)ndesc;
}

/* Service events from polling */
static void mfsha_serve(struct pollfd *pdesc) {
	/* Core mfsha module has no sockets to service */
	(void)pdesc;
}

/* Reload configuration */
static void mfsha_reload(void) {
	haconn_reload();
	gossip_reload();
	raftconsensus_reload();
	crdtstore_reload();
	shardmgr_reload();
	metasyncer_reload();
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "mfsha: configuration reloaded");
}

/* Display status information */
static void mfsha_info(FILE *fd) {
	fprintf(fd, "[mfsha status]\n");
	fprintf(fd, "terminate flag: %d\n", terminate);
	haconn_info(fd);
	gossip_info(fd);
	raftconsensus_info(fd);
	crdtstore_info(fd);
	shardmgr_info(fd);
	metasyncer_info(fd);
}
