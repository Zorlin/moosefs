/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 */

#include "gossip.h"
#include "mfslog.h"
#include "cfg.h"
#include "haconn.h"
#include "datapack.h"
#include "changelog.h"
#include <stdio.h>
#include <poll.h>
#include <string.h>

#define MAXLOGLINESIZE 200000U

static double last_gossip_time = 0.0;
static double gossip_interval = 5.0; /* Default gossip every 5 seconds */
static uint32_t known_nodes = 1; /* Start with self */

int gossip_init(void) {
	gossip_interval = cfg_getdouble("HA_GOSSIP_INTERVAL", 5.0);
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "gossip_init: initialized with gossip interval %.1fs", gossip_interval);
	return 0;
}

void gossip_term(void) {
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "gossip_term: terminated");
}

/* Periodic gossip protocol maintenance */
void gossip_tick(double now) {
	/* TODO: Send gossip messages to random peers */
	/* TODO: Process incoming gossip messages */
	/* TODO: Update cluster membership information */
	/* TODO: Detect failed nodes and remove from cluster */
	
	if (now - last_gossip_time >= gossip_interval) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "gossip_tick: sending gossip to cluster (%"PRIu32" known nodes)", known_nodes);
		/* TODO: Actual gossip protocol implementation */
		last_gossip_time = now;
	}
}

/* Reload configuration */
void gossip_reload(void) {
	double old_interval = gossip_interval;
	
	gossip_interval = cfg_getdouble("HA_GOSSIP_INTERVAL", 5.0);
	
	if (old_interval != gossip_interval) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "gossip_reload: gossip interval changed from %.1fs to %.1fs", 
		        old_interval, gossip_interval);
	} else {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "gossip_reload: configuration reloaded");
	}
}

/* Collect file descriptors for polling */
void gossip_desc(struct pollfd *pdesc, uint32_t *ndesc) {
	/* TODO: Add UDP socket for gossip protocol */
	/* TODO: Add listening socket for incoming gossip messages */
	(void)pdesc;
	(void)ndesc;
}

/* Service events from polling */
void gossip_serve(struct pollfd *pdesc) {
	/* TODO: Handle incoming gossip messages */
	/* TODO: Process gossip protocol events */
	(void)pdesc;
}

/* Display status information */
void gossip_info(FILE *fd) {
	fprintf(fd, "[gossip status]\n");
	fprintf(fd, "gossip_interval: %.1f seconds\n", gossip_interval);
	fprintf(fd, "last_gossip_time: %.2f\n", last_gossip_time);
	fprintf(fd, "known_nodes: %"PRIu32"\n", known_nodes);
	fprintf(fd, "next_gossip_in: %.1f seconds\n", 
	        gossip_interval - (last_gossip_time > 0 ? (gossip_interval - last_gossip_time) : 0));
	
	/* TODO: Display cluster membership information */
	/* TODO: Show node health status */
	/* TODO: Display gossip message statistics */
}

/* Changelog synchronization via gossip */
void gossip_broadcast_changelog_entry(uint64_t version, const char *data, uint32_t data_len) {
	/* Validate changelog size */
	if (data_len > MAXLOGLINESIZE) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "gossip_broadcast: changelog entry too large (%u bytes)", data_len);
		return;
	}
	
	/* Send changelog entry to all connected HA peers */
	haconn_send_changelog_entry(version, (const uint8_t*)data, data_len);
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "gossip_broadcast: sent changelog v%"PRIu64" (%u bytes) to HA peers", 
	        version, data_len);
}
