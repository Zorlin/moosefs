/*
 * Copyright (C) 2025 MooseFS High Availability Extension
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <poll.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <inttypes.h>
#include <stddef.h>

#include "haconn.h"
#include "mfslog.h"
#include "sockets.h"
#include "clocks.h"
#include "cfg.h"
#include "datapack.h"
#include "crc.h"
#include "massert.h"
#include "main.h"
#include "gvc.h"
#include <pthread.h>
#include "metasync.h"
#include "raftconsensus.h"
#include "main.h"

/* MFS HA Protocol message types */
#define MFSHA_NOP             0x1000
#define MFSHA_CRDT_DELTA      0x1001
#define MFSHA_RAFT_REQUEST    0x1002
#define MFSHA_RAFT_RESPONSE   0x1003
#define MFSHA_GOSSIP_PING     0x1004
#define MFSHA_GOSSIP_PONG     0x1005
#define MFSHA_SHARD_MIGRATE   0x1006
#define MFSHA_META_SYNC       0x1007
#define MFSHA_CHANGELOG_ENTRY 0x8000  /* High number to avoid MooseFS protocol conflicts */

/* Connection states */
enum {
	HACONN_FREE,
	HACONN_CONNECTING,
	HACONN_HANDSHAKE,
	HACONN_CONNECTED,
	HACONN_KILL
};

/* Handshake message types */
#define MFSHA_HANDSHAKE_REQ   0x1100
#define MFSHA_HANDSHAKE_RESP  0x1101

/* GVC RPC message types */
#define MFSHA_GVC_VERSION_REQ  0x3000
#define MFSHA_GVC_VERSION_RESP 0x3001

/* Packet structures */
typedef struct out_packet {
	struct out_packet *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t data[1];
} out_packet_t;

typedef struct in_packet {
	struct in_packet *next;
	uint32_t type, length;
	uint8_t data[1];
} in_packet_t;

/* HA Connection structure */
typedef struct haconn {
	struct haconn *next;
	uint8_t mode;
	int sock;
	int32_t pdescpos;
	double lastread, lastwrite, conntime;
	
	uint32_t peerip;
	uint16_t peerport;
	uint32_t peerid;
	
	/* Input packet processing */
	uint8_t input_hdr[8];
	uint8_t *input_startptr;
	uint32_t input_bytesleft;
	uint8_t input_end;
	in_packet_t *input_packet;
	in_packet_t *inputhead, **inputtail;
	
	/* Output packet queue */
	out_packet_t *outputhead, **outputtail;
} haconn_t;

/* Global state */
static haconn_t *haconn_head = NULL;
static int listen_sock = -1;
static uint16_t listen_port = 9430;
static uint32_t my_nodeid = 0;
static char *peers_config = NULL;

/* Statistics */
static uint64_t stats_bytesout = 0;
static uint64_t stats_bytesin = 0;
static uint64_t stats_packetsin = 0;
static uint64_t stats_packetsout = 0;


/* Function prototypes */
static haconn_t* haconn_new(int sock);
static void haconn_delete(haconn_t *conn);
static uint8_t* haconn_createpacket(haconn_t *conn, uint32_t type, uint32_t size);
static void haconn_gotpacket(haconn_t *conn, uint32_t type, const uint8_t *data, uint32_t length);
static void haconn_read(haconn_t *conn, double now);
static void haconn_write(haconn_t *conn, double now);
static void haconn_parse(haconn_t *conn);

/* Send handshake request */
static void haconn_send_handshake(haconn_t *conn) {
	uint8_t *ptr;
	
	ptr = haconn_createpacket(conn, MFSHA_HANDSHAKE_REQ, 4);
	if (ptr) {
		put32bit(&ptr, my_nodeid);
		mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: sent handshake request with node_id %u", my_nodeid);
	}
}

/* Create a new HA connection */
static haconn_t* haconn_new(int sock) {
	haconn_t *conn;
	
	conn = malloc(sizeof(haconn_t));
	if (!conn) {
		return NULL;
	}
	
	conn->next = haconn_head;
	haconn_head = conn;
	
	conn->mode = HACONN_HANDSHAKE;  /* Default for incoming connections */
	conn->sock = sock;
	conn->pdescpos = -1;
	conn->lastread = conn->lastwrite = conn->conntime = monotonic_seconds();
	
	conn->peerip = 0;
	conn->peerport = 0;
	conn->peerid = 0;
	
	conn->input_startptr = conn->input_hdr;
	conn->input_bytesleft = 8;
	conn->input_end = 0;
	conn->input_packet = NULL;
	conn->inputhead = NULL;
	conn->inputtail = &(conn->inputhead);
	
	conn->outputhead = NULL;
	conn->outputtail = &(conn->outputhead);
	
	/* Don't send handshake automatically - let caller decide */
	
	return conn;
}

/* Create new outgoing connection */
static haconn_t* haconn_new_outgoing(int sock) {
	haconn_t *conn = haconn_new(sock);
	if (conn) {
		conn->mode = HACONN_CONNECTING;  /* Mark as connecting for outgoing */
	}
	return conn;
}

/* Create new incoming connection */
static haconn_t* haconn_new_incoming(int sock) {
	haconn_t *conn = haconn_new(sock);
	if (conn) {
		conn->mode = HACONN_HANDSHAKE;  /* Ready for handshake on incoming */
		/* Send handshake immediately for incoming connections */
		haconn_send_handshake(conn);
	}
	return conn;
}

/* Delete an HA connection */
static void haconn_delete(haconn_t *conn) {
	haconn_t **connptr, *aconn;
	in_packet_t *ipptr, *ipaptr;
	out_packet_t *opptr, *opaptr;
	
	if (conn->sock >= 0) {
		tcpclose(conn->sock);
	}
	
	if (conn->input_packet) {
		free(conn->input_packet);
	}
	
	ipptr = conn->inputhead;
	while (ipptr) {
		ipaptr = ipptr;
		ipptr = ipptr->next;
		free(ipaptr);
	}
	
	opptr = conn->outputhead;
	while (opptr) {
		opaptr = opptr;
		opptr = opptr->next;
		free(opaptr);
	}
	
	connptr = &haconn_head;
	while ((aconn = *connptr)) {
		if (aconn == conn) {
			*connptr = aconn->next;
			break;
		}
		connptr = &(aconn->next);
	}
	
	free(conn);
}

/* Create an outgoing packet */
static uint8_t* haconn_createpacket(haconn_t *conn, uint32_t type, uint32_t size) {
	out_packet_t *outpacket;
	uint8_t *ptr;
	uint32_t psize;
	
	psize = size + 8;
	outpacket = malloc(offsetof(out_packet_t, data) + psize);
	if (!outpacket) {
		return NULL;
	}
	
	outpacket->bytesleft = psize;
	ptr = outpacket->data;
	put32bit(&ptr, type);
	put32bit(&ptr, size);
	outpacket->startptr = outpacket->data;
	outpacket->next = NULL;
	
	*(conn->outputtail) = outpacket;
	conn->outputtail = &(outpacket->next);
	
	stats_packetsout++;
	
	return ptr;
}

/* Handle received packets */
static void haconn_gotpacket(haconn_t *conn, uint32_t type, const uint8_t *data, uint32_t length) {
	stats_packetsin++;
	
	switch (type) {
		case MFSHA_HANDSHAKE_REQ: {
			/* Received handshake request - send response */
			if (length >= 4) {
				const uint8_t *ptr = data;
				uint8_t *resp;
				conn->peerid = get32bit(&ptr);
				
				resp = haconn_createpacket(conn, MFSHA_HANDSHAKE_RESP, 4);
				if (resp) {
					put32bit(&resp, my_nodeid);
				}
				
				conn->mode = HACONN_CONNECTED;
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: handshake complete with peer %u (received request)", conn->peerid);
				
				/* Add peer to Raft */
				extern int raft_add_peer(uint32_t node_id, const char *host, uint16_t port);
				struct in_addr addr;
				addr.s_addr = conn->peerip;
				char ip_str[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &addr, ip_str, sizeof(ip_str));
				raft_add_peer(conn->peerid, ip_str, conn->peerport);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: invalid handshake request");
				conn->mode = HACONN_KILL;
			}
			break;
		}
		
		case MFSHA_HANDSHAKE_RESP: {
			/* Received handshake response */
			if (length >= 4) {
				const uint8_t *ptr = data;
				conn->peerid = get32bit(&ptr);
				conn->mode = HACONN_CONNECTED;
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: handshake complete with peer %u (received response)", conn->peerid);
				
				/* Add peer to Raft */
				extern int raft_add_peer(uint32_t node_id, const char *host, uint16_t port);
				struct in_addr addr;
				addr.s_addr = conn->peerip;
				char ip_str[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &addr, ip_str, sizeof(ip_str));
				raft_add_peer(conn->peerid, ip_str, conn->peerport);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: invalid handshake response");
				conn->mode = HACONN_KILL;
			}
			break;
		}
		
		case MFSHA_NOP:
			/* Heartbeat - no action needed */
			break;
			
		case MFSHA_CRDT_DELTA:
			/* CRDT support removed - ignore these messages */
			if (conn->mode != HACONN_CONNECTED) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: received CRDT delta before handshake");
				conn->mode = HACONN_KILL;
				break;
			}
			mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: ignoring CRDT delta (CRDT support removed)");
			break;
			
		case MFSHA_RAFT_REQUEST:
			/* Forward to Raft consensus */
			if (conn->mode != HACONN_CONNECTED) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: received Raft request before handshake");
				conn->mode = HACONN_KILL;
				break;
			}
			if (length >= 1) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: received Raft request from peer %u, %u bytes", conn->peerid, length);
				raft_handle_incoming_message(conn->peerid, data, length);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: invalid Raft request size");
			}
			break;
			
		case MFSHA_RAFT_RESPONSE:
			/* Forward to Raft consensus */
			if (conn->mode != HACONN_CONNECTED) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: received Raft response before handshake");
				conn->mode = HACONN_KILL;
				break;
			}
			if (length >= 1) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: received Raft response from peer %u, %u bytes", conn->peerid, length);
				raft_handle_incoming_message(conn->peerid, data, length);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: invalid Raft response size");
			}
			break;
			
		case MFSHA_GOSSIP_PING:
			/* Respond with pong */
			haconn_createpacket(conn, MFSHA_GOSSIP_PONG, 4);
			mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: received gossip ping, sending pong");
			break;
			
		case MFSHA_GOSSIP_PONG:
			/* Update gossip state */
			/* TODO: Call gossip_handle_pong(conn->peerid) */
			mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: received gossip pong");
			break;
			
		case MFSHA_SHARD_MIGRATE:
			/* Forward to shard manager */
			if (length >= 8) {
				/* TODO: Call shardmgr_handle_migration(data, length) */
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: received shard migration, %u bytes", length);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: invalid shard migration size");
			}
			break;
			
		case MFSHA_META_SYNC:
			/* Forward to metadata syncer */
			if (length >= 1) {
				metasync_handle_message(conn->peerid, data, length);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: invalid metadata sync size");
			}
			break;
			
		case MFSHA_GVC_VERSION_REQ:
			/* GVC version requests are no longer supported - we don't allocate ranges */
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: received obsolete GVC version request");
			conn->mode = HACONN_KILL;
			break;
			
		case MFSHA_GVC_VERSION_RESP:
			/* GVC version responses are no longer used */
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: received obsolete GVC version response");
			break;
			
		case MFSHA_CHANGELOG_ENTRY:
			/* Handle changelog entry from peer */
			if (conn->mode != HACONN_CONNECTED) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: received changelog entry before handshake");
				conn->mode = HACONN_KILL;
				break;
			}
			if (length >= 12) { /* version:8 + length:4 + data */
				const uint8_t *ptr = data;
				uint64_t version = get64bit(&ptr);
				uint32_t data_len = get32bit(&ptr);
				
				if (length == 12 + data_len) {
					/* Forward to changelog replay system */
					extern int changelog_replay_entry(uint64_t version, const char *entry);
					changelog_replay_entry(version, (const char*)ptr);
					
					mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: received changelog v%"PRIu64" (%u bytes) from peer %u", 
						version, data_len, conn->peerid);
				} else {
					mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: invalid changelog entry size (expected %u, got %u)", 
						12 + data_len, length);
				}
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: changelog entry too small (%u < 12)", length);
			}
			break;
			
		default:
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: unknown packet type: 0x%08X", type);
			conn->mode = HACONN_KILL;
			break;
	}
}

/* Read data from connection */
static void haconn_read(haconn_t *conn, double now) {
	int32_t i;
	uint32_t type, length;
	const uint8_t *ptr;
	static uint8_t *readbuff = NULL;
	static uint32_t readbuffsize = 0;
	uint32_t rblength, rbpos;
	uint8_t err, hup;
	
	if (readbuffsize == 0) {
		readbuffsize = 65536;
		readbuff = malloc(readbuffsize);
		if (!readbuff) {
			mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "haconn: out of memory");
			conn->mode = HACONN_KILL;
			return;
		}
	}
	
	rblength = 0;
	err = 0;
	hup = 0;
	
	for (;;) {
		i = read(conn->sock, readbuff + rblength, readbuffsize - rblength);
		if (i == 0) {
			hup = 1;
			break;
		} else if (i < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				err = 1;
			}
			break;
		} else {
			stats_bytesin += i;
			rblength += i;
			if (rblength == readbuffsize) {
				readbuffsize *= 2;
				readbuff = realloc(readbuff, readbuffsize);
				if (!readbuff) {
					mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "haconn: out of memory");
					conn->mode = HACONN_KILL;
					return;
				}
			} else {
				break;
			}
		}
	}
	
	if (rblength > 0) {
		conn->lastread = now;
	}
	
	rbpos = 0;
	while (rbpos < rblength) {
		if ((rblength - rbpos) >= conn->input_bytesleft) {
			memcpy(conn->input_startptr, readbuff + rbpos, conn->input_bytesleft);
			i = conn->input_bytesleft;
		} else {
			memcpy(conn->input_startptr, readbuff + rbpos, rblength - rbpos);
			i = rblength - rbpos;
		}
		rbpos += i;
		conn->input_startptr += i;
		conn->input_bytesleft -= i;
		
		if (conn->input_bytesleft > 0) {
			break;
		}
		
		if (conn->input_packet == NULL) {
			ptr = conn->input_hdr;
			type = get32bit(&ptr);
			length = get32bit(&ptr);
			
			if (length > 100000000) {  /* 100MB limit */
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: packet too long (%u)", length);
				conn->input_end = 1;
				return;
			}
			
			conn->input_packet = malloc(offsetof(in_packet_t, data) + length);
			if (!conn->input_packet) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "haconn: out of memory");
				conn->mode = HACONN_KILL;
				return;
			}
			conn->input_packet->next = NULL;
			conn->input_packet->type = type;
			conn->input_packet->length = length;
			
			conn->input_startptr = conn->input_packet->data;
			conn->input_bytesleft = length;
		}
		
		if (conn->input_bytesleft > 0) {
			continue;
		}
		
		if (conn->input_packet != NULL) {
			*(conn->inputtail) = conn->input_packet;
			conn->inputtail = &(conn->input_packet->next);
			conn->input_packet = NULL;
			conn->input_bytesleft = 8;
			conn->input_startptr = conn->input_hdr;
		}
	}
	
	if (hup) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, "haconn: connection closed by peer");
		conn->input_end = 1;
	} else if (err) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: read error: %s", strerror(errno));
		conn->input_end = 1;
	}
}

/* Write data to connection */
static void haconn_write(haconn_t *conn, double now) {
	out_packet_t *opack;
	int32_t i;
	
	while ((opack = conn->outputhead) != NULL) {
		i = write(conn->sock, opack->startptr, opack->bytesleft);
		if (i < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: write error: %s", strerror(errno));
				conn->mode = HACONN_KILL;
			}
			return;
		}
		if (i > 0) {
			conn->lastwrite = now;
		}
		stats_bytesout += i;
		opack->startptr += i;
		opack->bytesleft -= i;
		if (opack->bytesleft > 0) {
			return;
		}
		conn->outputhead = opack->next;
		if (conn->outputhead == NULL) {
			conn->outputtail = &(conn->outputhead);
		}
		free(opack);
	}
}

/* Parse incoming packets */
static void haconn_parse(haconn_t *conn) {
	in_packet_t *ipack;
	
	while ((conn->mode == HACONN_CONNECTED || conn->mode == HACONN_HANDSHAKE) && (ipack = conn->inputhead) != NULL) {
		haconn_gotpacket(conn, ipack->type, ipack->data, ipack->length);
		conn->inputhead = ipack->next;
		free(ipack);
		if (conn->inputhead == NULL) {
			conn->inputtail = &(conn->inputhead);
		}
	}
	
	if (conn->mode == HACONN_CONNECTED && conn->inputhead == NULL && conn->input_end) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_parse: killing connection due to input_end");
		conn->mode = HACONN_KILL;
	}
}

/* Public interface functions */

/* Forward declaration */
static void haconn_retry_peer_connections(void);

int haconn_init(void) {
	int sock;
	
	listen_port = cfg_getuint16("HA_CONN_LISTEN_PORT", 9430);
	
	/* Read node ID from environment first, then config file */
	char *node_id_env = getenv("MFSHA_NODE_ID");
	if (node_id_env && *node_id_env) {
		my_nodeid = (uint32_t)atol(node_id_env);
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: using node ID %u from environment", my_nodeid);
	} else {
		my_nodeid = cfg_getuint32("MFSHA_NODE_ID", 1);
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: using node ID %u from config", my_nodeid);
	}
	
	/* Read peers from environment first, then config file */
	char *peers_env = getenv("MFSHA_PEERS");
	if (peers_env && *peers_env) {
		peers_config = strdup(peers_env);
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: using peers from environment: %s", peers_config);
	} else {
		peers_config = cfg_getstr("MFSHA_PEERS", "");
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: using peers from config: %s", peers_config);
	}
	
	sock = tcpsocket();
	if (sock < 0) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "haconn: cannot create socket");
		return -1;
	}
	
	tcpreuseaddr(sock);
	if (tcpnumlisten(sock, 0, listen_port, 64) < 0) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "haconn: cannot listen on port %u", listen_port);
		tcpclose(sock);
		return -1;
	}
	
	tcpnonblock(sock);
	listen_sock = sock;
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: listening on port %u, node ID %u", listen_port, my_nodeid);
	
	/* Register with main poll loop */
	main_poll_register(haconn_desc, haconn_serve);
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: registered with main poll loop");
	
	/* Register periodic retry of peer connections */
	main_time_register(5, 0, haconn_retry_peer_connections);  /* Check every 5 seconds */
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: registered periodic peer connection retry");
	
	/* Parse peers_config and establish connections */
	if (peers_config && strlen(peers_config) > 0) {
		char *peers_copy = strdup(peers_config);
		char *peer = strtok(peers_copy, ",");
		uint32_t peer_position = 1; /* Track position in peer list */
		
		while (peer) {
			char *colon = strchr(peer, ':');
			if (colon) {
				*colon = '\0';
				/* Always use HA port for peer connections, not the port from config */
				uint16_t port = listen_port; /* Use same port we're listening on */
				
				/* Skip self based on node ID matching peer position */
				int is_self = (peer_position == my_nodeid);
				
				/* Debug: Log peer connection decision */
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: peer %s:%u position=%u is_self=%d (my_nodeid=%u)", 
				        peer, port, peer_position, is_self, my_nodeid);
				
				if (!is_self) {
					/* Connect to peer */
					int csock = tcpsocket();
					if (csock >= 0) {
						uint32_t ip = 0;
						uint16_t resolved_port = 0;
						if (tcpresolve(peer, NULL, &ip, &resolved_port, 0) >= 0 && ip > 0) {
							mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: resolved %s to IP %u.%u.%u.%u", peer, 
							       (ip >> 24) & 0xFF, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);
							tcpnonblock(csock);
							int connect_result = tcpnumconnect(csock, ip, port);
							mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: connect result for %s:%u = %d (errno=%d:%s)", 
							       peer, port, connect_result, errno, strerror(errno));
							if (connect_result >= 0 || errno == EINPROGRESS) {
								/* Connection successful or in progress */
								tcpnodelay(csock);
								haconn_t *new_conn = haconn_new_outgoing(csock);
								if (new_conn != NULL) {
									/* Set peer IP address for outgoing connection */
									new_conn->peerip = ip;
									new_conn->peerport = port;
									mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: initiated connection to peer %s:%u (fd=%d)", peer, port, csock);
								} else {
									tcpclose(csock);
									mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: failed to create connection structure for %s:%u", peer, port);
								}
							} else {
								tcpclose(csock);
								mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: failed to connect to peer %s:%u - %s", peer, port, strerror(errno));
							}
						} else {
							tcpclose(csock);
							mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: failed to resolve peer %s", peer);
						}
					}
				}
			}
			peer = strtok(NULL, ",");
			peer_position++; /* Increment position for next peer */
		}
		
		free(peers_copy);
	}
	
	return 0;
}

/* Periodic retry of peer connections */
static void haconn_retry_peer_connections(void) {
	haconn_t *conn;
	uint32_t connected_count = 0;
	uint32_t expected_peers = 0;
	
	if (!peers_config || strlen(peers_config) == 0) {
		return;
	}
	
	/* Count connected peers */
	for (conn = haconn_head; conn; conn = conn->next) {
		if (conn->mode == HACONN_CONNECTED) {
			connected_count++;
		}
	}
	
	/* Count expected peers (total nodes - 1 for self) */
	char *peers_copy = strdup(peers_config);
	char *peer = strtok(peers_copy, ",");
	while (peer) {
		expected_peers++;
		peer = strtok(NULL, ",");
	}
	free(peers_copy);
	
	/* Adjust for self */
	if (expected_peers > 0) {
		expected_peers--;
	}
	
	/* If we have all expected peers, nothing to do */
	if (connected_count >= expected_peers) {
		return;
	}
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: retrying peer connections (connected=%u, expected=%u)", 
	        connected_count, expected_peers);
	
	/* Try to connect to missing peers */
	peers_copy = strdup(peers_config);
	peer = strtok(peers_copy, ",");
	uint32_t peer_position = 1;
	
	while (peer) {
		char *colon = strchr(peer, ':');
		if (colon) {
			*colon = '\0';
		}
		
		uint16_t port = listen_port;
		int is_self = (peer_position == my_nodeid);
		
		if (!is_self) {
			/* Check if we already have a connection to this peer */
			uint32_t ip = 0;
			uint16_t resolved_port = 0;
			if (tcpresolve(peer, NULL, &ip, &resolved_port, 0) >= 0 && ip > 0) {
				/* Check if already connected */
				int already_connected = 0;
				for (conn = haconn_head; conn; conn = conn->next) {
					if (conn->peerip == ip && conn->peerport == port && 
					    (conn->mode == HACONN_CONNECTED || conn->mode == HACONN_CONNECTING || 
					     conn->mode == HACONN_HANDSHAKE)) {
						already_connected = 1;
						break;
					}
				}
				
				if (!already_connected) {
					/* Try to connect */
					int csock = tcpsocket();
					if (csock >= 0) {
						mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: retrying connection to peer %s:%u", peer, port);
						tcpnonblock(csock);
						int connect_result = tcpnumconnect(csock, ip, port);
						if (connect_result >= 0 || errno == EINPROGRESS) {
							tcpnodelay(csock);
							haconn_t *new_conn = haconn_new_outgoing(csock);
							if (new_conn != NULL) {
								new_conn->peerip = ip;
								new_conn->peerport = port;
								mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: retry initiated connection to peer %s:%u", peer, port);
							} else {
								tcpclose(csock);
							}
						} else {
							tcpclose(csock);
							mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: retry failed to connect to peer %s:%u - %s", 
							        peer, port, strerror(errno));
						}
					}
				}
			}
		}
		
		peer = strtok(NULL, ",");
		peer_position++;
	}
	
	free(peers_copy);
}

void haconn_term(void) {
	haconn_t *conn, *next_conn;
	
	if (listen_sock >= 0) {
		tcpclose(listen_sock);
		listen_sock = -1;
	}
	
	conn = haconn_head;
	while (conn) {
		next_conn = conn->next;
		haconn_delete(conn);
		conn = next_conn;
	}
	
	if (peers_config) {
		free(peers_config);
		peers_config = NULL;
	}
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: terminated");
}

void haconn_desc(struct pollfd *pdesc, uint32_t *ndesc) {
	haconn_t *conn;
	uint32_t pos = *ndesc;
	static uint32_t desc_call_count = 0;
	
	desc_call_count++;
	if ((desc_call_count % 100) == 1) { /* Log every 100th call */
		mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_desc: called %u times, starting pos=%u", desc_call_count, pos);
	}
	
	/* Listen socket */
	if (listen_sock >= 0) {
		pdesc[pos].fd = listen_sock;
		pdesc[pos].events = POLLIN;
		pos++;
	}
	
	/* Connection sockets */
	for (conn = haconn_head; conn; conn = conn->next) {
		conn->pdescpos = -1;
		if (conn->mode == HACONN_FREE || conn->sock < 0) {
			continue;
		}
		
		pdesc[pos].events = 0;
		if ((conn->mode == HACONN_CONNECTED || conn->mode == HACONN_HANDSHAKE) && conn->input_end == 0) {
			pdesc[pos].events |= POLLIN;
		}
		if (conn->mode == HACONN_CONNECTING) {
			pdesc[pos].events |= POLLOUT; /* Wait for connection completion */
		}
		if (conn->outputhead != NULL) {
			pdesc[pos].events |= POLLOUT;
		}
		if (pdesc[pos].events != 0) {
			pdesc[pos].fd = conn->sock;
			conn->pdescpos = pos;
			/* Debug: Log poll setup for connecting connections */
			if (conn->mode == HACONN_CONNECTING) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_desc: setup POLLOUT for fd=%d at pos=%u", conn->sock, pos);
			}
			pos++;
		}
	}
	
	*ndesc = pos;
}

void haconn_serve(struct pollfd *pdesc) {
	haconn_t *conn, *next_conn;
	double now;
	int newfd;
	uint32_t i;
	static uint32_t serve_call_count = 0;
	
	serve_call_count++;
	if ((serve_call_count % 100) == 1) { /* Log every 100th call */
		mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_serve: called %u times", serve_call_count);
	}
	
	now = monotonic_seconds();
	
	/* Handle new connections */
	if (listen_sock >= 0) {
		/* Find listen socket in pdesc array */
		for (i = 0; i < 1000 && pdesc[i].fd != listen_sock; i++) {
			/* Search for listen socket */
		}
		if (i < 1000 && pdesc[i].fd == listen_sock && (pdesc[i].revents & POLLIN)) {
			newfd = tcpaccept(listen_sock);
			if (newfd >= 0) {
				tcpnonblock(newfd);
				tcpnodelay(newfd);
				haconn_t *new_conn = haconn_new_incoming(newfd);
				if (new_conn == NULL) {
					mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: cannot accept connection");
					tcpclose(newfd);
				} else {
					/* Get peer IP address for incoming connection */
					uint32_t peer_ip = 0;
					uint16_t peer_port = 0;
					if (tcpgetpeer(newfd, &peer_ip, &peer_port) == 0) {
						new_conn->peerip = peer_ip;
						new_conn->peerport = peer_port;
						mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: accepted new connection from %u.%u.%u.%u:%u", 
						        (peer_ip >> 24) & 0xFF, (peer_ip >> 16) & 0xFF, 
						        (peer_ip >> 8) & 0xFF, peer_ip & 0xFF, peer_port);
					} else {
						mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: accepted new connection (could not get peer address)");
					}
				}
			}
		}
	}
	
	/* Handle existing connections */
	conn = haconn_head;
	while (conn) {
		next_conn = conn->next;
		
		if (conn->pdescpos >= 0) {
			/* Debug: Log events for connecting connections */
			if (conn->mode == HACONN_CONNECTING && pdesc[conn->pdescpos].revents != 0) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: fd=%d revents=0x%x (POLLOUT=0x%x POLLERR=0x%x POLLHUP=0x%x)", 
				       conn->sock, pdesc[conn->pdescpos].revents, POLLOUT, POLLERR, POLLHUP);
			}
			
			/* Handle connection completion for outgoing connections */
			if (conn->mode == HACONN_CONNECTING && (pdesc[conn->pdescpos].revents & POLLOUT)) {
				int sockstatus = tcpgetstatus(conn->sock);
				if (sockstatus == 0) {
					/* Connection successful */
					conn->mode = HACONN_HANDSHAKE;
					mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: connection established (fd=%d), sending handshake", conn->sock);
					haconn_send_handshake(conn);
				} else {
					/* Connection failed */
					mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: connection failed (fd=%d) - %s", conn->sock, strerror(sockstatus));
					conn->mode = HACONN_KILL;
				}
			}
			
			/* Handle connection errors */
			if (conn->mode == HACONN_CONNECTING && (pdesc[conn->pdescpos].revents & (POLLERR | POLLHUP))) {
				int err = 0;
				socklen_t errlen = sizeof(err);
				getsockopt(conn->sock, SOL_SOCKET, SO_ERROR, &err, &errlen);
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: connection error/hangup (fd=%d) revents=0x%x SO_ERROR=%d:%s", 
				       conn->sock, pdesc[conn->pdescpos].revents, err, strerror(err));
				conn->mode = HACONN_KILL;
			}
			
			if ((pdesc[conn->pdescpos].revents & POLLIN) && (conn->mode == HACONN_CONNECTED || conn->mode == HACONN_HANDSHAKE)) {
				haconn_read(conn, now);
			}
			if (pdesc[conn->pdescpos].revents & (POLLERR | POLLHUP)) {
				conn->input_end = 1;
			}
			haconn_parse(conn);
		}
		
		/* Send heartbeat if needed */
		if (conn->mode == HACONN_CONNECTED && 
		    conn->lastwrite + 30.0 < now && 
		    conn->outputhead == NULL) {
			haconn_createpacket(conn, MFSHA_NOP, 0);
		}
		
		if (conn->pdescpos >= 0) {
			if (((pdesc[conn->pdescpos].events & POLLOUT) == 0 && conn->outputhead) ||
			    (pdesc[conn->pdescpos].revents & POLLOUT)) {
				haconn_write(conn, now);
			}
		}
		
		/* Timeout check */
		if (conn->mode == HACONN_CONNECTED && conn->lastread + 120.0 < now) {
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: connection timeout (fd=%d)", conn->sock);
			conn->mode = HACONN_KILL;
		}
		
		/* Timeout connecting connections after 30 seconds */
		if (conn->mode == HACONN_CONNECTING && conn->conntime + 30.0 < now) {
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: connection timeout during connect (fd=%d)", conn->sock);
			conn->mode = HACONN_KILL;
		}
		
		/* Timeout handshake after 10 seconds */
		if (conn->mode == HACONN_HANDSHAKE && conn->conntime + 10.0 < now) {
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: handshake timeout (fd=%d)", conn->sock);
			conn->mode = HACONN_KILL;
		}
		
		/* Delete killed connections */
		if (conn->mode == HACONN_KILL) {
			haconn_delete(conn);
		}
		
		conn = next_conn;
	}
}

void haconn_reload(void) {
	/* TODO: Reload peer configuration */
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: configuration reloaded");
}

void haconn_info(FILE *fd) {
	haconn_t *conn;
	uint32_t conn_count = 0;
	
	for (conn = haconn_head; conn; conn = conn->next) {
		conn_count++;
	}
	
	fprintf(fd, "[haconn status]\n");
	fprintf(fd, "listen port: %u\n", listen_port);
	fprintf(fd, "node ID: %u\n", my_nodeid);
	fprintf(fd, "active connections: %u\n", conn_count);
	fprintf(fd, "bytes in: %"PRIu64"\n", stats_bytesin);
	fprintf(fd, "bytes out: %"PRIu64"\n", stats_bytesout);
	fprintf(fd, "packets in: %"PRIu64"\n", stats_packetsin);
	fprintf(fd, "packets out: %"PRIu64"\n", stats_packetsout);
}

/* Send CRDT delta to peers */
void haconn_send_crdt_delta(const uint8_t *data, uint32_t length) {
	haconn_t *conn;
	uint8_t *ptr;
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_send_crdt_delta: broadcasting %u bytes", length);
	
	/* Debug: Hex dump first 40 bytes of outgoing data */
	if (length > 0) {
		char hex_dump[512];
		uint32_t dump_len = (length > 40) ? 40 : length;
		char *hex_ptr = hex_dump;
		for (uint32_t i = 0; i < dump_len; i++) {
			sprintf(hex_ptr, "%02X ", data[i]);
			hex_ptr += 3;
		}
		*hex_ptr = '\0';
		mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_send_crdt_delta: sending hex dump (%u bytes): %s", dump_len, hex_dump);
		
		/* Parse and verify the outgoing header too */
		if (length >= 32) {
			const uint8_t *ptr = data;
			uint64_t key = get64bit(&ptr);
			uint32_t type = get32bit(&ptr);
			uint64_t timestamp = get64bit(&ptr);
			uint32_t node_id = get32bit(&ptr);
			uint32_t counter = get32bit(&ptr);
			uint32_t value_size = get32bit(&ptr);
			
			mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_send_crdt_delta: OUTGOING header: key=%"PRIu64" type=%u timestamp=%"PRIu64" node_id=%u counter=%u value_size=%u", 
				key, type, timestamp, node_id, counter, value_size);
				
			if (value_size > 10000000) {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "haconn_send_crdt_delta: BUG - OUTGOING data already corrupt! value_size=%u (0x%08X)", 
					value_size, value_size);
			}
		}
	}
	
	for (conn = haconn_head; conn; conn = conn->next) {
		if (conn->mode == HACONN_CONNECTED) {
			ptr = haconn_createpacket(conn, MFSHA_CRDT_DELTA, length);
			if (ptr) {
				memcpy(ptr, data, length);
				
				/* Verify the data wasn't corrupted during packet creation */
				if (memcmp(ptr, data, length) != 0) {
					mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "haconn_send_crdt_delta: DATA CORRUPTION during packet creation for peer %u", conn->peerid);
				}
				
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_send_crdt_delta: sent %u bytes to peer %u", length, conn->peerid);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn_send_crdt_delta: failed to create packet for peer %u", conn->peerid);
			}
		}
	}
}

/* Send Raft request to specific peer */
void haconn_send_raft_request(uint32_t peerid, const uint8_t *data, uint32_t length) {
	haconn_t *conn;
	uint8_t *ptr;
	
	for (conn = haconn_head; conn; conn = conn->next) {
		if (conn->mode == HACONN_CONNECTED && conn->peerid == peerid) {
			ptr = haconn_createpacket(conn, MFSHA_RAFT_REQUEST, length);
			if (ptr) {
				memcpy(ptr, data, length);
			}
			break;
		}
	}
}

/* Send Raft response to specific peer */
void haconn_send_raft_response(uint32_t peerid, const uint8_t *data, uint32_t length) {
	haconn_t *conn;
	uint8_t *ptr;
	int found = 0;
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn_send_raft_response: looking for peer %u to send %u bytes", peerid, length);
	
	for (conn = haconn_head; conn; conn = conn->next) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn_send_raft_response: checking conn peerid=%u mode=%d", conn->peerid, conn->mode);
		if (conn->mode == HACONN_CONNECTED && conn->peerid == peerid) {
			ptr = haconn_createpacket(conn, MFSHA_RAFT_RESPONSE, length);
			if (ptr) {
				memcpy(ptr, data, length);
				mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn_send_raft_response: sent %u bytes to peer %u", length, peerid);
				found = 1;
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "haconn_send_raft_response: failed to create packet for peer %u", peerid);
			}
			break;
		}
	}
	
	if (!found) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn_send_raft_response: no connected peer found with id %u", peerid);
	}
}

/* Send metadata sync message to all peers */
void haconn_send_meta_sync(const uint8_t *data, uint32_t length) {
	haconn_t *conn;
	uint8_t *ptr;
	
	for (conn = haconn_head; conn; conn = conn->next) {
		if (conn->mode == HACONN_CONNECTED) {
			ptr = haconn_createpacket(conn, MFSHA_META_SYNC, length);
			if (ptr) {
				memcpy(ptr, data, length);
			}
		}
	}
}

/* Send metadata sync message to specific peer */
void haconn_send_meta_sync_to_peer(uint32_t peerid, const uint8_t *data, uint32_t length) {
	haconn_t *conn;
	uint8_t *ptr;
	
	for (conn = haconn_head; conn; conn = conn->next) {
		if (conn->mode == HACONN_CONNECTED && conn->peerid == peerid) {
			ptr = haconn_createpacket(conn, MFSHA_META_SYNC, length);
			if (ptr) {
				memcpy(ptr, data, length);
			}
			break;
		}
	}
}

/* Send changelog entry to all peers */
void haconn_send_changelog_entry(uint64_t version, const uint8_t *data, uint32_t length) {
	haconn_t *conn;
	uint8_t *ptr;
	uint32_t packet_size = 12 + length; /* version:8 + length:4 + data */
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_send_changelog_entry: broadcasting v%"PRIu64" (%u bytes)", version, length);
	
	for (conn = haconn_head; conn; conn = conn->next) {
		if (conn->mode == HACONN_CONNECTED) {
			ptr = haconn_createpacket(conn, MFSHA_CHANGELOG_ENTRY, packet_size);
			if (ptr) {
				put64bit(&ptr, version);
				put32bit(&ptr, length);
				if (length > 0) {
					memcpy(ptr, data, length);
				}
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_send_changelog_entry: sent to peer %u", conn->peerid);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn_send_changelog_entry: failed to create packet for peer %u", conn->peerid);
			}
		}
	}
}

/* Send Raft message to all peers */
void haconn_send_raft_broadcast(const uint8_t *data, uint32_t length) {
	haconn_t *conn;
	uint8_t *ptr;
	uint32_t sent_count = 0;
	uint32_t total_count = 0;
	
	for (conn = haconn_head; conn; conn = conn->next) {
		total_count++;
		if (conn->mode == HACONN_CONNECTED) {
			ptr = haconn_createpacket(conn, MFSHA_RAFT_REQUEST, length);
			if (ptr) {
				memcpy(ptr, data, length);
				sent_count++;
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_send_raft_broadcast: sent %u bytes to peer %u", length, conn->peerid);
			}
		}
	}
	
	if (sent_count == 0) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn_send_raft_broadcast: no connected peers to send to (total connections=%"PRIu32")", total_count);
		/* Debug: Show connection states */
		uint32_t connecting = 0, handshake = 0, connected = 0, killed = 0, free_count = 0;
		for (conn = haconn_head; conn; conn = conn->next) {
			switch (conn->mode) {
				case HACONN_FREE: free_count++; break;
				case HACONN_CONNECTING: connecting++; break;
				case HACONN_HANDSHAKE: handshake++; break;
				case HACONN_CONNECTED: connected++; break;
				case HACONN_KILL: killed++; break;
			}
		}
		mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn_send_raft_broadcast: connection states - free:%"PRIu32" connecting:%"PRIu32" handshake:%"PRIu32" connected:%"PRIu32" killed:%"PRIu32, 
		       free_count, connecting, handshake, connected, killed);
	}
}

/* Get leader connection information for client redirection */
int haconn_get_leader_info(uint32_t leader_id, uint32_t *leader_ip, uint16_t *leader_port) {
	haconn_t *conn;
	
	if (!leader_ip || !leader_port) {
		return -1;
	}
	
	/* Initialize to invalid values */
	*leader_ip = 0;
	*leader_port = 0;
	
	if (leader_id == my_nodeid) {
		/* We are the leader - this should not happen if we're redirecting! */
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn_get_leader_info: asked for leader info but we ARE the leader (leader_id=%u, my_nodeid=%u)", leader_id, my_nodeid);
		*leader_ip = 0;
		*leader_port = 9420; /* Standard MooseFS chunkserver port */
		return 0;
	}
	
	/* Find the connection to the leader peer */
	for (conn = haconn_head; conn != NULL; conn = conn->next) {
		if (conn->peerid == leader_id && conn->mode == HACONN_CONNECTED) {
			if (conn->peerip != 0) {
				*leader_ip = conn->peerip;
				*leader_port = 9421; /* Standard MooseFS client port */
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn_get_leader_info: found leader %u at %u.%u.%u.%u:%u", 
				        leader_id, (conn->peerip >> 24) & 0xFF, (conn->peerip >> 16) & 0xFF, 
				        (conn->peerip >> 8) & 0xFF, conn->peerip & 0xFF, *leader_port);
				return 0;
			}
		}
	}
	
	/* If we can't find it in active connections, try to parse from peer config */
	if (peers_config && strlen(peers_config) > 0) {
		char *peers_copy = strdup(peers_config);
		char *peer = strtok(peers_copy, ",");
		uint32_t peer_position = 1;
		
		while (peer) {
			if (peer_position == leader_id) {
				/* Found the leader in config */
				char *colon = strchr(peer, ':');
				if (colon) {
					*colon = '\0'; /* Terminate hostname part */
				}
				
				/* Resolve the hostname to IP */
				uint32_t ip = 0;
				uint16_t resolved_port = 0;
				if (tcpresolve(peer, NULL, &ip, &resolved_port, 0) >= 0 && ip > 0) {
					*leader_ip = ip;
					*leader_port = 9420; /* Standard MooseFS chunkserver port */
					mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn_get_leader_info: resolved leader %u from config: %s -> %u.%u.%u.%u:%u", 
					        leader_id, peer, (ip >> 24) & 0xFF, (ip >> 16) & 0xFF, 
					        (ip >> 8) & 0xFF, ip & 0xFF, *leader_port);
					free(peers_copy);
					return 0;
				} else {
					mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn_get_leader_info: failed to resolve leader %u hostname: %s", leader_id, peer);
				}
				break;
			}
			peer = strtok(NULL, ",");
			peer_position++;
		}
		
		free(peers_copy);
	}
	
	/* Leader not found in connections or config */
	mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn_get_leader_info: leader %u not found in connections or config", leader_id);
	return -1;
}

