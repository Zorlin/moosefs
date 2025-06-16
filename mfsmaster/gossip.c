/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 */

#include "gossip.h"
#include "mfslog.h"
#include "cfg.h"
#include "haconn.h"
#include "datapack.h"
#include "changelog.h"
#include "hamaster.h"
#include "clocks.h"
#include "random.h"
#include <stdio.h>
#include <poll.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>

#define MAXLOGLINESIZE 200000U
#define GOSSIP_PORT 9431
#define GOSSIP_MAX_PACKET 65536
#define GOSSIP_MAGIC 0x4D465347 /* MFSG */

/* Gossip message types */
#define GOSSIP_MSG_PING      0x01
#define GOSSIP_MSG_PONG      0x02
#define GOSSIP_MSG_NODE_LIST 0x03
#define GOSSIP_MSG_HEARTBEAT 0x04

/* Node states */
#define NODE_STATE_ALIVE     0x01
#define NODE_STATE_SUSPECTED 0x02
#define NODE_STATE_DEAD      0x03

/* Node information */
typedef struct gossip_node {
	struct gossip_node *next;
	uint32_t node_id;
	uint32_t ip;
	uint16_t port;
	uint8_t state;
	double last_seen;
	uint64_t incarnation;
	uint64_t current_version;  /* Current metadata version */
	uint64_t highest_version;  /* Highest version this node has */
} gossip_node_t;

static double last_gossip_time = 0.0;
static double gossip_interval = 5.0; /* Default gossip every 5 seconds */
static uint32_t known_nodes = 1; /* Start with self */
static gossip_node_t *nodes_head = NULL;
static int gossip_sock = -1;
static uint64_t local_incarnation = 0;
static uint32_t msg_sent = 0;
static uint32_t msg_received = 0;

/* Find or create node entry */
static gossip_node_t* gossip_find_node(uint32_t node_id) {
	gossip_node_t *node = nodes_head;
	while (node) {
		if (node->node_id == node_id) {
			return node;
		}
		node = node->next;
	}
	return NULL;
}

static gossip_node_t* gossip_add_node(uint32_t node_id, uint32_t ip, uint16_t port) {
	gossip_node_t *node = gossip_find_node(node_id);
	if (!node) {
		node = malloc(sizeof(gossip_node_t));
		if (node) {
			node->node_id = node_id;
			node->ip = ip;
			node->port = port;
			node->state = NODE_STATE_ALIVE;
			node->last_seen = monotonic_seconds();
			node->incarnation = 0;
			node->current_version = 0;
			node->highest_version = 0;
			node->next = nodes_head;
			nodes_head = node;
			known_nodes++;
		}
	}
	return node;
}

int gossip_init(void) {
	struct sockaddr_in addr;
	int optval = 1;
	
	gossip_interval = cfg_getdouble("HA_GOSSIP_INTERVAL", 5.0);
	local_incarnation = monotonic_useconds();
	
	/* Create UDP socket for gossip */
	gossip_sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (gossip_sock < 0) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "gossip_init: failed to create UDP socket: %s", strerror(errno));
		return -1;
	}
	
	/* Set socket options */
	setsockopt(gossip_sock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
	
	/* Bind to gossip port */
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(GOSSIP_PORT);
	
	if (bind(gossip_sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "gossip_init: failed to bind UDP socket: %s", strerror(errno));
		close(gossip_sock);
		gossip_sock = -1;
		return -1;
	}
	
	/* Make socket non-blocking */
	fcntl(gossip_sock, F_SETFL, O_NONBLOCK);
	
	/* Add self to nodes list */
	gossip_add_node(ha_get_node_id(), 0, GOSSIP_PORT);
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "gossip_init: initialized with gossip interval %.1fs on port %d", 
	        gossip_interval, GOSSIP_PORT);
	return 0;
}

void gossip_term(void) {
	gossip_node_t *node, *next;
	
	/* Close socket */
	if (gossip_sock >= 0) {
		close(gossip_sock);
		gossip_sock = -1;
	}
	
	/* Free nodes list */
	node = nodes_head;
	while (node) {
		next = node->next;
		free(node);
		node = next;
	}
	nodes_head = NULL;
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "gossip_term: terminated");
}

/* Send gossip message */
static void gossip_send_message(uint8_t msg_type, uint32_t target_ip, uint16_t target_port) {
	uint8_t buffer[1024];
	uint8_t *ptr = buffer;
	struct sockaddr_in addr;
	ssize_t sent;
	
	/* Get current version info */
	extern uint64_t meta_version(void);
	uint64_t my_version = meta_version();
	
	/* Build message header */
	put32bit(&ptr, GOSSIP_MAGIC);
	put8bit(&ptr, msg_type);
	put32bit(&ptr, ha_get_node_id());
	put64bit(&ptr, local_incarnation);
	put64bit(&ptr, my_version); /* Add current version to all messages */
	
	switch (msg_type) {
		case GOSSIP_MSG_PING:
		case GOSSIP_MSG_HEARTBEAT:
			/* Just header + version */
			break;
			
		case GOSSIP_MSG_NODE_LIST:
			/* Add known nodes */
			put32bit(&ptr, known_nodes);
			gossip_node_t *node = nodes_head;
			while (node) {
				put32bit(&ptr, node->node_id);
				put32bit(&ptr, node->ip);
				put16bit(&ptr, node->port);
				put8bit(&ptr, node->state);
				put64bit(&ptr, node->incarnation);
				put64bit(&ptr, node->current_version);
				put64bit(&ptr, node->highest_version);
				node = node->next;
			}
			break;
	}
	
	/* Send message */
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = target_ip;
	addr.sin_port = htons(target_port);
	
	sent = sendto(gossip_sock, buffer, ptr - buffer, 0, 
	              (struct sockaddr*)&addr, sizeof(addr));
	if (sent > 0) {
		msg_sent++;
	}
}

/* Select random node for gossip */
static gossip_node_t* gossip_select_random_node(void) {
	uint32_t count = 0;
	gossip_node_t *node = nodes_head;
	
	/* Count alive nodes (excluding self) */
	while (node) {
		if (node->node_id != ha_get_node_id() && node->state != NODE_STATE_DEAD) {
			count++;
		}
		node = node->next;
	}
	
	if (count == 0) return NULL;
	
	/* Select random index */
	uint32_t idx = rndu32_ranged(count);
	node = nodes_head;
	count = 0;
	
	while (node) {
		if (node->node_id != ha_get_node_id() && node->state != NODE_STATE_DEAD) {
			if (count == idx) {
				return node;
			}
			count++;
		}
		node = node->next;
	}
	
	return NULL;
}

/* Periodic gossip protocol maintenance */
void gossip_tick(double now) {
	gossip_node_t *node;
	double timeout = 30.0; /* Node timeout in seconds */
	
	/* Check for dead nodes */
	node = nodes_head;
	while (node) {
		if (node->node_id != ha_get_node_id()) {
			double age = now - node->last_seen;
			
			if (node->state == NODE_STATE_ALIVE && age > timeout / 2) {
				node->state = NODE_STATE_SUSPECTED;
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "gossip: node %u suspected (not seen for %.1fs)",
				        node->node_id, age);
			} else if (node->state == NODE_STATE_SUSPECTED && age > timeout) {
				node->state = NODE_STATE_DEAD;
				known_nodes--;
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "gossip: node %u marked dead (not seen for %.1fs)",
				        node->node_id, age);
			}
		}
		node = node->next;
	}
	
	/* Send periodic gossip */
	if (now - last_gossip_time >= gossip_interval) {
		/* Select random node to gossip with */
		node = gossip_select_random_node();
		if (node && node->ip > 0) {
			mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "gossip_tick: sending heartbeat to node %u", node->node_id);
			gossip_send_message(GOSSIP_MSG_HEARTBEAT, node->ip, node->port);
			
			/* Occasionally send full node list */
			if (rndu32_ranged(10) == 0) {
				gossip_send_message(GOSSIP_MSG_NODE_LIST, node->ip, node->port);
			}
		}
		
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
	if (gossip_sock >= 0) {
		pdesc[*ndesc].fd = gossip_sock;
		pdesc[*ndesc].events = POLLIN;
		(*ndesc)++;
	}
}

/* Process incoming gossip message */
static void gossip_process_message(uint8_t *buffer, ssize_t len, struct sockaddr_in *from) {
	const uint8_t *ptr = buffer;
	uint32_t magic, sender_id;
	uint64_t sender_incarnation;
	uint64_t sender_version;
	uint8_t msg_type;
	gossip_node_t *sender_node;
	
	if (len < 25) return; /* Too small - now includes version */
	
	/* Parse header */
	magic = get32bit(&ptr);
	if (magic != GOSSIP_MAGIC) return;
	
	msg_type = get8bit(&ptr);
	sender_id = get32bit(&ptr);
	sender_incarnation = get64bit(&ptr);
	sender_version = get64bit(&ptr); /* Extract version from message */
	
	/* Update sender info */
	sender_node = gossip_find_node(sender_id);
	if (!sender_node) {
		sender_node = gossip_add_node(sender_id, from->sin_addr.s_addr, ntohs(from->sin_port));
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "gossip: discovered new node %u at %s:%d",
		        sender_id, inet_ntoa(from->sin_addr), ntohs(from->sin_port));
	}
	
	if (sender_node) {
		sender_node->last_seen = monotonic_seconds();
		sender_node->incarnation = sender_incarnation;
		sender_node->ip = from->sin_addr.s_addr;
		sender_node->port = ntohs(from->sin_port);
		
		/* Update version information */
		sender_node->current_version = sender_version;
		if (sender_version > sender_node->highest_version) {
			sender_node->highest_version = sender_version;
		}
		
		/* Check if we need to synchronize */
		extern uint64_t meta_version(void);
		uint64_t my_version = meta_version();
		
		if (sender_version > my_version) {
			/* This node has newer data - trigger synchronization */
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "gossip: node %u has version %"PRIu64", we have %"PRIu64" - triggering sync",
			        sender_id, sender_version, my_version);
			
			/* Request missing versions from this node */
			extern void ringrepl_request_range(uint64_t from_version, uint64_t to_version);
			ringrepl_request_range(my_version + 1, sender_version);
		}
		
		if (sender_node->state != NODE_STATE_ALIVE) {
			sender_node->state = NODE_STATE_ALIVE;
			mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "gossip: node %u is now alive", sender_id);
		}
	}
	
	msg_received++;
	
	/* Process message type */
	switch (msg_type) {
		case GOSSIP_MSG_PING:
			/* Reply with pong */
			gossip_send_message(GOSSIP_MSG_PONG, from->sin_addr.s_addr, ntohs(from->sin_port));
			break;
			
		case GOSSIP_MSG_NODE_LIST:
			/* Process node list */
			if (len >= 29) { /* Updated size check for version info */
				uint32_t count = get32bit(&ptr);
				uint32_t i;
				
				for (i = 0; i < count && (ptr - buffer + 35) <= len; i++) { /* 35 bytes per node entry now */
					uint32_t node_id = get32bit(&ptr);
					uint32_t node_ip = get32bit(&ptr);
					uint16_t node_port = get16bit(&ptr);
					uint8_t node_state = get8bit(&ptr);
					uint64_t node_incarnation = get64bit(&ptr);
					uint64_t node_current_version = get64bit(&ptr);
					uint64_t node_highest_version = get64bit(&ptr);
					
					if (node_id != ha_get_node_id()) {
						gossip_node_t *node = gossip_find_node(node_id);
						if (!node && node_state != NODE_STATE_DEAD) {
							node = gossip_add_node(node_id, node_ip, node_port);
							if (node) {
								node->incarnation = node_incarnation;
								node->state = node_state;
								node->current_version = node_current_version;
								node->highest_version = node_highest_version;
							}
						} else if (node && node->incarnation < node_incarnation) {
							/* Update with newer information */
							node->ip = node_ip;
							node->port = node_port;
							node->incarnation = node_incarnation;
							node->state = node_state;
							node->current_version = node_current_version;
							node->highest_version = node_highest_version;
						}
						
						/* Check if any node has newer data */
						if (node && node_current_version > 0) {
							extern uint64_t meta_version(void);
							uint64_t my_version = meta_version();
							
							if (node_current_version > my_version) {
								mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
								        "gossip: discovered node %u has version %"PRIu64", we have %"PRIu64,
								        node_id, node_current_version, my_version);
								
								/* Request missing versions */
								extern void ringrepl_request_range(uint64_t from_version, uint64_t to_version);
								ringrepl_request_range(my_version + 1, node_current_version);
							}
						}
					}
				}
			}
			break;
	}
}

/* Service events from polling */
void gossip_serve(struct pollfd *pdesc) {
	uint32_t i;
	
	for (i = 0; pdesc[i].fd >= 0; i++) {
		if (pdesc[i].fd == gossip_sock && (pdesc[i].revents & POLLIN)) {
			uint8_t buffer[GOSSIP_MAX_PACKET];
			struct sockaddr_in from;
			socklen_t fromlen = sizeof(from);
			ssize_t len;
			
			while ((len = recvfrom(gossip_sock, buffer, sizeof(buffer), 0,
			                       (struct sockaddr*)&from, &fromlen)) > 0) {
				gossip_process_message(buffer, len, &from);
			}
		}
	}
}

/* Display status information */
void gossip_info(FILE *fd) {
	gossip_node_t *node;
	double now = monotonic_seconds();
	
	fprintf(fd, "[gossip status]\n");
	fprintf(fd, "gossip_interval: %.1f seconds\n", gossip_interval);
	fprintf(fd, "last_gossip_time: %.2f\n", last_gossip_time);
	fprintf(fd, "known_nodes: %"PRIu32"\n", known_nodes);
	fprintf(fd, "next_gossip_in: %.1f seconds\n", 
	        gossip_interval - (now - last_gossip_time));
	fprintf(fd, "messages_sent: %"PRIu32"\n", msg_sent);
	fprintf(fd, "messages_received: %"PRIu32"\n", msg_received);
	fprintf(fd, "\n");
	
	/* Display cluster membership */
	fprintf(fd, "[cluster nodes]\n");
	node = nodes_head;
	while (node) {
		const char *state_str = "unknown";
		switch (node->state) {
			case NODE_STATE_ALIVE: state_str = "alive"; break;
			case NODE_STATE_SUSPECTED: state_str = "suspected"; break;
			case NODE_STATE_DEAD: state_str = "dead"; break;
		}
		
		struct in_addr addr;
		addr.s_addr = node->ip;
		
		fprintf(fd, "node %"PRIu32": %s:%d state=%s last_seen=%.1fs ago incarnation=%"PRIu64"\n",
		        node->node_id, 
		        node->ip ? inet_ntoa(addr) : "<unknown>",
		        node->port,
		        state_str,
		        now - node->last_seen,
		        node->incarnation);
		node = node->next;
	}
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
