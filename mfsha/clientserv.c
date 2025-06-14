/*
 * MooseFS HA Client Server Module
 * Handles client requests and shard routing for HA cluster
 */

#include "clientserv.h"
#include "shardmgr.h"
#include "haconn.h" 
#include "mfslog.h"
#include "sockets.h"
#include "datapack.h"
#include "MFSCommunication.h"
#include <sys/poll.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#define CLIENTSERV_LISTEN_PORT 9421
#define MAX_CLIENT_CONNECTIONS 1000

typedef struct client_conn {
    int fd;
    uint32_t version;
    uint32_t sesflags;
    uint32_t sessionid;
    char *info;
    struct client_conn *next;
} client_conn_t;

static client_conn_t *client_connections = NULL;
static int listen_fd = -1;
static uint32_t next_sessionid = 1;

int clientserv_init(void) {
    listen_fd = tcpsocket();
    if (listen_fd < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "clientserv_init: socket failed");
        return -1;
    }

    if (tcpsetnonblock(listen_fd) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "clientserv_init: nonblock failed");
        close(listen_fd);
        return -1;
    }

    if (tcpreuseaddr(listen_fd) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "clientserv_init: reuseaddr failed");
        close(listen_fd);
        return -1;
    }

    if (tcpstrlisten(listen_fd, "*", CLIENTSERV_LISTEN_PORT, 100) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR, "clientserv_init: listen failed");
        close(listen_fd);
        return -1;
    }

    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "clientserv_init: listening on port %d", CLIENTSERV_LISTEN_PORT);
    return 0;
}

void clientserv_term(void) {
    client_conn_t *conn, *next;
    
    for (conn = client_connections; conn; conn = next) {
        next = conn->next;
        close(conn->fd);
        if (conn->info) {
            free(conn->info);
        }
        free(conn);
    }
    
    if (listen_fd >= 0) {
        close(listen_fd);
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "clientserv_term: terminated");
}

static void clientserv_accept_connection(void) {
    int fd = tcpaccept(listen_fd);
    if (fd < 0) {
        return;
    }
    
    client_conn_t *conn = malloc(sizeof(client_conn_t));
    if (!conn) {
        close(fd);
        return;
    }
    
    conn->fd = fd;
    conn->version = 0;
    conn->sesflags = 0;
    conn->sessionid = next_sessionid++;
    conn->info = NULL;
    conn->next = client_connections;
    client_connections = conn;
    
    tcpsetnonblock(fd);
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "clientserv: accepted client connection (session %"PRIu32")", conn->sessionid);
}

static int clientserv_send_response(client_conn_t *conn, uint32_t cmd, uint32_t msgid, const uint8_t *data, uint32_t length) {
    uint8_t *buff, *wptr;
    uint32_t total_size = 8 + length;
    
    buff = malloc(total_size);
    if (!buff) {
        return -1;
    }
    
    wptr = buff;
    put32bit(&wptr, cmd);
    put32bit(&wptr, length);
    if (length > 0 && data) {
        memcpy(wptr, data, length);
    }
    
    if (tcptowrite(conn->fd, buff, total_size, 1000) != (int32_t)total_size) {
        free(buff);
        return -1;
    }
    
    free(buff);
    return 0;
}

static int clientserv_handle_ha_cluster_info(client_conn_t *conn, uint32_t msgid, const uint8_t *data, uint32_t length) {
    uint8_t *response, *wptr;
    uint32_t response_size;
    uint32_t node_count = 1; // For now, just report this node
    uint32_t shard_count = shardmgr_get_shard_count();
    uint32_t i;
    
    // Calculate response size: msgid + nodecount + shardcount + nodeinfo + shardmappings
    response_size = 4 + 4 + 4; // msgid + nodecount + shardcount
    response_size += 4 + 4 + 9 + 2 + 1; // nodeid + hostnamelen + "localhost" + port + status
    response_size += shard_count * 4; // shard mappings
    
    response = malloc(response_size);
    if (!response) {
        return -1;
    }
    
    wptr = response;
    put32bit(&wptr, msgid);
    put32bit(&wptr, node_count);
    put32bit(&wptr, shard_count);
    
    // Node information
    put32bit(&wptr, 1); // node_id
    put32bit(&wptr, 9); // hostname length
    memcpy(wptr, "localhost", 9);
    wptr += 9;
    put16bit(&wptr, CLIENTSERV_LISTEN_PORT);
    put8bit(&wptr, 1); // status: up
    
    // Shard mappings (all shards map to node 0 for now)
    for (i = 0; i < shard_count; i++) {
        put32bit(&wptr, 0); // All shards owned by this node
    }
    
    if (clientserv_send_response(conn, MATOCL_HA_CLUSTER_INFO, msgid, response + 4, response_size - 4) < 0) {
        free(response);
        return -1;
    }
    
    free(response);
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "clientserv: sent cluster info to session %"PRIu32, conn->sessionid);
    return 0;
}

static int clientserv_handle_fuse_lookup(client_conn_t *conn, uint32_t msgid, const uint8_t *data, uint32_t length) {
    const uint8_t *rptr = data;
    uint32_t inode, shard_id;
    uint8_t *response, *wptr;
    uint32_t response_size = 8; // msgid + status
    
    if (length < 4) {
        return -1;
    }
    
    inode = get32bit(&rptr);
    shard_id = shardmgr_get_shard(inode);
    
    // Check if we own this shard
    if (!shardmgr_is_local_shard(shard_id)) {
        // Send redirect response
        response = malloc(response_size + 6); // + redirect info
        wptr = response;
        put32bit(&wptr, msgid);
        put8bit(&wptr, 2); // Status: redirect
        put32bit(&wptr, 0x7f000001); // localhost for now
        put16bit(&wptr, CLIENTSERV_LISTEN_PORT);
        
        clientserv_send_response(conn, MATOCL_HA_SHARD_REDIRECT, msgid, response + 4, 7);
        free(response);
        return 0;
    }
    
    // Handle locally - for now send a stub response
    response = malloc(response_size);
    wptr = response;
    put32bit(&wptr, msgid);
    put8bit(&wptr, 1); // Status: success
    put8bit(&wptr, 0); // Placeholder for actual filesystem response
    put16bit(&wptr, 0);
    
    clientserv_send_response(conn, MATOCL_FUSE_LOOKUP, msgid, response + 4, 4);
    free(response);
    return 0;
}

static int clientserv_handle_request(client_conn_t *conn, const uint8_t *data, uint32_t length) {
    const uint8_t *rptr;
    uint32_t cmd, msgid, packet_length;
    
    if (length < 8) {
        return -1;
    }
    
    rptr = data;
    cmd = get32bit(&rptr);
    packet_length = get32bit(&rptr);
    
    if (packet_length + 8 != length) {
        return -1;
    }
    
    if (packet_length >= 4) {
        msgid = get32bit(&rptr);
    } else {
        msgid = 0;
    }
    
    switch (cmd) {
        case CLTOMA_HA_CLUSTER_INFO:
            return clientserv_handle_ha_cluster_info(conn, msgid, rptr, packet_length - 4);
            
        case CLTOMA_FUSE_LOOKUP:
            return clientserv_handle_fuse_lookup(conn, msgid, rptr, packet_length - 4);
            
        // Add other FUSE operations here
        case CLTOMA_FUSE_GETATTR:
        case CLTOMA_FUSE_SETATTR:
        case CLTOMA_FUSE_READDIR:
        case CLTOMA_FUSE_ACCESS:
        case CLTOMA_FUSE_OPEN:
            // Stub implementations for now
            {
                uint8_t response[8];
                uint8_t *wptr = response;
                put32bit(&wptr, msgid);
                put8bit(&wptr, 0); // Status: OK
                put8bit(&wptr, 0);
                put16bit(&wptr, 0);
                clientserv_send_response(conn, cmd + 1, msgid, response + 4, 4);
            }
            break;
            
        default:
            // Unknown command
            {
                uint8_t response[8];
                uint8_t *wptr = response;
                put32bit(&wptr, msgid);
                put8bit(&wptr, EINVAL); // Status: invalid
                put8bit(&wptr, 0);
                put16bit(&wptr, 0);
                clientserv_send_response(conn, cmd + 1000, msgid, response + 4, 4);
            }
            break;
    }
    
    return 0;
}

void clientserv_desc(struct pollfd *pdesc, uint32_t *ndesc) {
    client_conn_t *conn;
    
    // Add listen socket
    if (listen_fd >= 0 && *ndesc < 1000) {
        pdesc[*ndesc].fd = listen_fd;
        pdesc[*ndesc].events = POLLIN;
        (*ndesc)++;
    }
    
    // Add client connection sockets
    for (conn = client_connections; conn && *ndesc < 1000; conn = conn->next) {
        pdesc[*ndesc].fd = conn->fd;
        pdesc[*ndesc].events = POLLIN;
        (*ndesc)++;
    }
}

void clientserv_serve(struct pollfd *pdesc, uint32_t ndesc) {
    client_conn_t *conn, *prev, *next;
    uint8_t buffer[65536];
    int ret;
    uint32_t i;
    
    // Check listen socket
    if (listen_fd >= 0) {
        for (i = 0; i < ndesc; i++) {
            if (pdesc[i].fd == listen_fd && (pdesc[i].revents & POLLIN)) {
                clientserv_accept_connection();
                break;
            }
        }
    }
    
    // Check client connections
    prev = NULL;
    for (conn = client_connections; conn; conn = next) {
        next = conn->next;
        
        for (i = 0; i < ndesc; i++) {
            if (pdesc[i].fd == conn->fd && (pdesc[i].revents & POLLIN)) {
                ret = tcptoread(conn->fd, buffer, 65536, 0);
                if (ret > 0) {
                    if (clientserv_handle_request(conn, buffer, ret) < 0) {
                        // Close connection on error
                        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "clientserv: closing session %"PRIu32" due to error", conn->sessionid);
                        close(conn->fd);
                        if (prev) {
                            prev->next = next;
                        } else {
                            client_connections = next;
                        }
                        if (conn->info) {
                            free(conn->info);
                        }
                        free(conn);
                        conn = NULL;
                    }
                } else if (ret == 0) {
                    // Client disconnected
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "clientserv: session %"PRIu32" disconnected", conn->sessionid);
                    close(conn->fd);
                    if (prev) {
                        prev->next = next;
                    } else {
                        client_connections = next;
                    }
                    if (conn->info) {
                        free(conn->info);
                    }
                    free(conn);
                    conn = NULL;
                } else if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    // Error
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "clientserv: session %"PRIu32" error: %s", conn->sessionid, strerr(errno));
                    close(conn->fd);
                    if (prev) {
                        prev->next = next;
                    } else {
                        client_connections = next;
                    }
                    if (conn->info) {
                        free(conn->info);
                    }
                    free(conn);
                    conn = NULL;
                }
                break;
            }
        }
        
        if (conn) {
            prev = conn;
        }
    }
}