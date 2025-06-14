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

/* MFS HA Protocol message types */
#define MFSHA_NOP             0x1000
#define MFSHA_CRDT_DELTA      0x1001
#define MFSHA_RAFT_REQUEST    0x1002
#define MFSHA_RAFT_RESPONSE   0x1003
#define MFSHA_GOSSIP_PING     0x1004
#define MFSHA_GOSSIP_PONG     0x1005
#define MFSHA_SHARD_MIGRATE   0x1006
#define MFSHA_META_SYNC       0x1007

/* Connection states */
enum {
	HACONN_FREE,
	HACONN_CONNECTING,
	HACONN_CONNECTED,
	HACONN_KILL
};

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

/* Create a new HA connection */
static haconn_t* haconn_new(int sock) {
	haconn_t *conn;
	
	conn = malloc(sizeof(haconn_t));
	if (!conn) {
		return NULL;
	}
	
	conn->next = haconn_head;
	haconn_head = conn;
	
	conn->mode = HACONN_CONNECTED;
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
		case MFSHA_NOP:
			/* Heartbeat - no action needed */
			break;
			
		case MFSHA_CRDT_DELTA:
			/* Forward to CRDT store */
			if (length >= 8) {
				/* TODO: Call crdtstore_apply_delta(data, length) */
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: received CRDT delta, %u bytes", length);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: invalid CRDT delta size");
			}
			break;
			
		case MFSHA_RAFT_REQUEST:
			/* Forward to Raft consensus */
			if (length >= 4) {
				/* TODO: Call raftconsensus_handle_request(conn->peerid, data, length) */
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: received Raft request, %u bytes", length);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: invalid Raft request size");
			}
			break;
			
		case MFSHA_RAFT_RESPONSE:
			/* Forward to Raft consensus */
			if (length >= 4) {
				/* TODO: Call raftconsensus_handle_response(conn->peerid, data, length) */
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: received Raft response, %u bytes", length);
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
			if (length >= 4) {
				/* TODO: Call metasyncer_handle_sync(data, length) */
				mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "haconn: received metadata sync, %u bytes", length);
			} else {
				mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: invalid metadata sync size");
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
	
	while (conn->mode == HACONN_CONNECTED && (ipack = conn->inputhead) != NULL) {
		haconn_gotpacket(conn, ipack->type, ipack->data, ipack->length);
		conn->inputhead = ipack->next;
		free(ipack);
		if (conn->inputhead == NULL) {
			conn->inputtail = &(conn->inputhead);
		}
	}
	
	if (conn->mode == HACONN_CONNECTED && conn->inputhead == NULL && conn->input_end) {
		conn->mode = HACONN_KILL;
	}
}

/* Public interface functions */

int haconn_init(void) {
	int sock;
	
	listen_port = cfg_getuint16("MFSHA_PORT", 9430);
	my_nodeid = cfg_getuint32("MFSHA_NODE_ID", 1);
	peers_config = cfg_getstr("MFSHA_PEERS", "");
	
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
	return 0;
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
		if (conn->mode == HACONN_CONNECTED && conn->input_end == 0) {
			pdesc[pos].events |= POLLIN;
		}
		if (conn->outputhead != NULL) {
			pdesc[pos].events |= POLLOUT;
		}
		if (pdesc[pos].events != 0) {
			pdesc[pos].fd = conn->sock;
			conn->pdescpos = pos;
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
				if (haconn_new(newfd) == NULL) {
					mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: cannot accept connection");
					tcpclose(newfd);
				} else {
					mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "haconn: accepted new connection");
				}
			}
		}
	}
	
	/* Handle existing connections */
	conn = haconn_head;
	while (conn) {
		next_conn = conn->next;
		
		if (conn->pdescpos >= 0) {
			if ((pdesc[conn->pdescpos].revents & POLLIN) && conn->mode == HACONN_CONNECTED) {
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
			mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "haconn: connection timeout");
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
	
	for (conn = haconn_head; conn; conn = conn->next) {
		if (conn->mode == HACONN_CONNECTED) {
			ptr = haconn_createpacket(conn, MFSHA_CRDT_DELTA, length);
			if (ptr) {
				memcpy(ptr, data, length);
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
	
	for (conn = haconn_head; conn; conn = conn->next) {
		if (conn->mode == HACONN_CONNECTED && conn->peerid == peerid) {
			ptr = haconn_createpacket(conn, MFSHA_RAFT_RESPONSE, length);
			if (ptr) {
				memcpy(ptr, data, length);
			}
			break;
		}
	}
}
