/*
 * Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
 * 
 * This file is part of MooseFS.
 * 
 * MooseFS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2 (only).
 * 
 * MooseFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MooseFS; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

#ifdef HAVE_CONFIG_H
# include "config.h"
#else
#  define VERSION2INT(maj,med,min) ((maj)*0x10000+(med)*0x100+(((maj)>1)?((min)*2):(min)))
#endif

#ifdef HAVE_ATOMICS
#include <stdatomic.h>
#undef HAVE_ATOMICS
#define HAVE_ATOMICS 1
#else
#define HAVE_ATOMICS 0
#endif

#if defined(HAVE___SYNC_OP_AND_FETCH)
#define HAVE_SYNCS 1
#else
#define HAVE_SYNCS 0
#endif

#ifdef WIN32
# include "portable.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef WIN32
# ifdef HAVE_POLL_H
#  include <poll.h>
# else
#  include <sys/poll.h>
# endif
#endif
#include <time.h>
#include <limits.h>
#include <errno.h>
#include <pthread.h>
#ifndef WIN32
# include <unistd.h>
# include <sys/time.h>
# include <pwd.h>
# include <grp.h>
#endif

#include "MFSCommunication.h"
#ifndef WIN32
# include "stats.h"
#endif
#include "sockets.h"
#include "strerr.h"
#include "md5.h"
#include "datapack.h"
#include "clocks.h"
#include "portable.h"
#include "heapsorter.h"
#include "extrapackets.h"
#include "massert.h"
#ifdef MFSMOUNT
#include "mfs_fuse.h"
#include "mfsmount.h"
#endif
#include "chunksdatacache.h"
#include "readdata.h"
#include "writedata.h"
// #include "dircache.h"

#define CONNECT_TIMEOUT 2000

typedef struct _threc {
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	uint8_t *obuff;
	uint32_t obuffsize;
	uint32_t odataleng;
	uint8_t *ibuff;
	uint32_t ibuffsize;
	uint32_t idataleng;

	uint8_t sent;		// packet was sent
	uint8_t status;		// receive status
	uint8_t rcvd;		// packet was received
//	uint8_t waiting;	// thread is waiting for answer
	uint8_t receiving;

	uint32_t rcvd_cmd;

	uint32_t packetid;
	struct _threc *next;
} threc;

#define THRECHASHSIZE 256

static threc *threchash[THRECHASHSIZE];
static threc *threcfree = NULL;
static uint16_t threcnextid = 0;


/*
typedef struct _threc {
	pthread_t thid;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	uint8_t *buff;
	uint32_t buffsize;
	uint8_t sent;
	uint8_t status;
	uint8_t release;	// cond variable
	uint8_t waiting;
	uint32_t size;
	uint32_t cmd;
	uint32_t packetid;
	struct _threc *next;
} threc;
*/

#define AMTIME_HASH_SIZE 4096
#define AMTIME_MAX_AGE 10

typedef struct _amtime_file {
	uint32_t inode;
	uint16_t atimeage;
	uint16_t mtimeage;
	uint64_t atime;
	uint64_t mtime;
	struct _amtime_file *next;
} amtime_file;

static amtime_file *amtime_hash[AMTIME_HASH_SIZE];

#define ACQFILES_HASH_SIZE 4096
#define ACQFILES_LRU_LIMIT 5000
#define ACQFILES_MAX_AGE 10

typedef struct _acquired_file {
	uint32_t inode;
	uint16_t cnt;
	uint8_t age;
	uint8_t dentry;
	struct _acquired_file *next;
	struct _acquired_file *lrunext,**lruprev;
} acquired_file;

static acquired_file *af_hash[ACQFILES_HASH_SIZE];
static acquired_file *af_lruhead,**af_lrutail;
static uint32_t af_lru_cnt;
static int64_t timediffusec = 0;

#define DEFAULT_OUTPUT_BUFFSIZE 0x1000
#define DEFAULT_INPUT_BUFFSIZE 0x10000

static int sock_timeout = 10;
static int recv_timeout = 300;
static int send_timeout = 10;

static int fd;
static int disconnect;
static int donotsendsustainedinodes;
static double lastwrite;
static int sessionlost;
static uint64_t lastsyncsend = 0;

/* Forward declarations for HA functions */
threc* fs_get_my_threc(void);
const uint8_t* fs_sendandreceive(threc *rec,uint32_t expected_cmd,uint32_t *answer_leng);
static const uint8_t* fs_raw_sendandreceive(const uint8_t *buff, uint32_t size, uint32_t expected_cmd, uint32_t *length);

/* HA Cluster Management */
typedef struct _ha_node {
    char *hostname;
    uint16_t port;
    uint32_t node_id;
    int fd;
    uint8_t status; // 0=down, 1=up, 2=connecting
    double last_check;
} ha_node_t;

typedef struct _ha_cluster {
    ha_node_t *nodes;
    uint32_t node_count;
    uint32_t shard_count;
    uint32_t *shard_to_node; // shard_id -> node_index mapping
    uint8_t ha_mode; // 0=single master, 1=HA cluster
    uint32_t current_node; // Currently connected node index
} ha_cluster_t;

static ha_cluster_t ha_cluster = {NULL, 0, 8, NULL, 0, 0};

#define FUSE_ROOT_ID 1

static uint64_t usectimeout;
static uint32_t maxretries;

#if HAVE_ATOMICS
static _Atomic uint32_t rcnt,wcnt,fcnt;
static _Atomic uint64_t rbyt,wbyt;
#elif HAVE_SYNCS
static volatile uint32_t rcnt,wcnt,fcnt;
static volatile uint64_t rbyt,wbyt;
#else
static volatile uint32_t rcnt,wcnt,fcnt;
static volatile uint64_t rbyt,wbyt;
#define OPDATA_USE_LOCK 1
static pthread_mutex_t opdatalock;
#endif

static pthread_t rpthid,npthid;
static pthread_mutex_t fdlock,reclock,aflock,amtimelock;
static pthread_key_t reckey;
static threc *mainrec;

static uint32_t sessionid;
static uint64_t metaid;
static uint32_t masterversion;
static uint64_t masterprocessid;
static uint8_t attrsize;

static char masterstrip[STRIPSIZE];
static uint32_t masterip=0;
static uint16_t masterport=0;
static char srcstrip[STRIPSIZE];
static uint32_t srcip=0;

static uint8_t fterm;

static uint8_t working_flags = 0;


void fs_getmasterlocation(uint8_t loc[22]) {
	pthread_mutex_lock(&fdlock);
	put32bit(&loc,masterip);
	put16bit(&loc,masterport);
	put32bit(&loc,sessionid);
	put32bit(&loc,masterversion);
	put64bit(&loc,masterprocessid);
	pthread_mutex_unlock(&fdlock);
}

void fs_getmasterparams(uint32_t *mip,uint16_t *mport,uint32_t *sid,uint32_t *mver,uint64_t *mprocid) {
	pthread_mutex_lock(&fdlock);
	if (mip!=NULL) {
		*mip = masterip;
	}
	if (mport!=NULL) {
		*mport = masterport;
	}
	if (sid!=NULL) {
		*sid = sessionid;
	}
	if (mver!=NULL) {
		*mver = masterversion;
	}
	if (mprocid!=NULL) {
		*mprocid = masterprocessid;
	}
	pthread_mutex_unlock(&fdlock);
}

uint32_t master_version(void) {
	uint32_t mver;
	pthread_mutex_lock(&fdlock);
	mver = masterversion;
	pthread_mutex_unlock(&fdlock);
	return mver;
}

uint8_t master_attrsize(void) {
	uint8_t asize;
	pthread_mutex_lock(&fdlock);
	asize = attrsize;
	pthread_mutex_unlock(&fdlock);
	return asize;
}

uint32_t fs_getsrcip(void) {
	uint32_t sip;
	pthread_mutex_lock(&fdlock);
	sip = srcip;
	pthread_mutex_unlock(&fdlock);
	return sip;
}

/* HA Cluster Functions */
static uint32_t ha_get_shard_for_inode(uint32_t inode) {
	return inode % ha_cluster.shard_count;
}

static int ha_discover_cluster(void) {
	uint8_t *wptr, *buff;
	const uint8_t *rptr;
	uint32_t cmd, length, i;
	
	if (!ha_cluster.ha_mode) {
		return 0; // Not in HA mode
	}
	
	// Send cluster discovery request
	buff = malloc(12);
	wptr = buff;
	put32bit(&wptr, CLTOMA_HA_CLUSTER_INFO);
	put32bit(&wptr, 4);
	put32bit(&wptr, 0); // Reserved
	
	rptr = fs_raw_sendandreceive(buff, 12, MATOCL_HA_CLUSTER_INFO, &length);
	free(buff);
	
	if (rptr == NULL) {
		return -1;
	}
	
	if (length < 16) {
		return -1;
	}
	
	cmd = get32bit(&rptr);
	length = get32bit(&rptr);
	
	if (cmd != MATOCL_HA_CLUSTER_INFO) {
		return -1;
	}
	
	// Parse cluster topology
	ha_cluster.node_count = get32bit(&rptr);
	ha_cluster.shard_count = get32bit(&rptr);
	
	// Allocate space for nodes and shard mapping
	if (ha_cluster.nodes) {
		for (i = 0; i < ha_cluster.node_count; i++) {
			if (ha_cluster.nodes[i].hostname) {
				free(ha_cluster.nodes[i].hostname);
			}
		}
		free(ha_cluster.nodes);
	}
	if (ha_cluster.shard_to_node) {
		free(ha_cluster.shard_to_node);
	}
	
	ha_cluster.nodes = malloc(ha_cluster.node_count * sizeof(ha_node_t));
	ha_cluster.shard_to_node = malloc(ha_cluster.shard_count * sizeof(uint32_t));
	
	// Parse node information
	for (i = 0; i < ha_cluster.node_count; i++) {
		uint32_t hostname_len;
		ha_cluster.nodes[i].node_id = get32bit(&rptr);
		hostname_len = get32bit(&rptr);
		
		ha_cluster.nodes[i].hostname = malloc(hostname_len + 1);
		memcpy(ha_cluster.nodes[i].hostname, rptr, hostname_len);
		ha_cluster.nodes[i].hostname[hostname_len] = 0;
		rptr += hostname_len;
		
		ha_cluster.nodes[i].port = get16bit(&rptr);
		ha_cluster.nodes[i].status = get8bit(&rptr);
		ha_cluster.nodes[i].fd = -1;
		ha_cluster.nodes[i].last_check = 0.0;
	}
	
	// Parse shard mappings
	for (i = 0; i < ha_cluster.shard_count; i++) {
		ha_cluster.shard_to_node[i] = get32bit(&rptr);
	}
	
	return 0;
}

// Raw send/receive that bypasses thread management (for HA routing)
static const uint8_t* fs_raw_sendandreceive(const uint8_t *buff, uint32_t size, uint32_t expected_cmd, uint32_t *length) {
	// For now, this is a simplified implementation that uses the current connection
	// In a full HA implementation, this would handle routing to specific nodes
	threc *rec = fs_get_my_threc();
	if (rec == NULL) {
		return NULL;
	}
	
	// Copy the data to the thread record output buffer
	if (rec->obuffsize < size) {
		if (rec->obuff != NULL) {
			free(rec->obuff);
		}
		rec->obuff = malloc(size);
		rec->obuffsize = size;
	}
	
	if (rec->obuff == NULL) {
		return NULL;
	}
	
	memcpy(rec->obuff, buff, size);
	rec->odataleng = size;
	
	// Use existing sendandreceive mechanism
	return fs_sendandreceive(rec, expected_cmd, length);
}

// HA-aware sendandreceive that routes to correct shard owner
static const uint8_t* fs_sendandreceive_ha(threc *rec, uint32_t inode, uint32_t cmd, uint32_t *length) {
	uint32_t shard_id, target_node;
	
	if (!ha_cluster.ha_mode) {
		// Fall back to regular single master communication  
		return fs_sendandreceive(rec, cmd, length);
	}
	
	shard_id = ha_get_shard_for_inode(inode);
	
	if (shard_id >= ha_cluster.shard_count) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"HA: invalid shard %u for inode %u", shard_id, inode);
		return NULL;
	}
	
	target_node = ha_cluster.shard_to_node[shard_id];
	
	if (target_node >= ha_cluster.node_count) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"HA: invalid target node %u for shard %u", target_node, shard_id);
		return NULL;
	}
	
	// Check if this is the current connection's node
	if (target_node == ha_cluster.current_node) {
		// Route through current connection
		return fs_sendandreceive(rec, cmd, length);
	}
	
	// For now, route through current connection with a warning
	// TODO: Implement proper multi-connection support
	mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"HA: routing inode %u to node %u (shard %u) via current connection", inode, target_node, shard_id);
	return fs_sendandreceive(rec, cmd, length);
}

// Wrapper for operations that don't have a specific inode (use root inode)
static const uint8_t* fs_sendandreceive_ha_root(threc *rec, uint32_t cmd, uint32_t *length) {
	return fs_sendandreceive_ha(rec, FUSE_ROOT_ID, cmd, length);
}

static inline void copy_attr(const uint8_t *rptr,uint8_t attr[ATTR_RECORD_SIZE],uint8_t asize) {
	if (asize>=ATTR_RECORD_SIZE) {
		memcpy(attr,rptr,ATTR_RECORD_SIZE);
	} else {
		memcpy(attr,rptr,asize);
		memset(attr+asize,0,(ATTR_RECORD_SIZE-asize));
	}
}

enum {
	MASTER_CONNECTS = 0,
	MASTER_BYTESSENT,
	MASTER_BYTESRCVD,
	MASTER_PACKETSSENT,
	MASTER_PACKETSRCVD,
	MASTER_PING,
	MASTER_TIMEDIFF,
	STATNODES
};

static void *statsptr[STATNODES];

struct connect_args_t {
	char *bindhostname;
	char *masterhostname;
	char *masterportname;
	uint8_t meta;
	uint8_t clearpassword;
	char *info;
	char *subfolder;
	uint8_t *passworddigest;
	uint32_t minversion;
};

static struct connect_args_t connect_args;

#ifndef WIN32
void master_statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"master",0,0);
	statsptr[MASTER_PACKETSRCVD] = stats_get_subnode(s,"packets_received",0,1);
	statsptr[MASTER_PACKETSSENT] = stats_get_subnode(s,"packets_sent",0,1);
	statsptr[MASTER_BYTESRCVD] = stats_get_subnode(s,"bytes_received",0,1);
	statsptr[MASTER_BYTESSENT] = stats_get_subnode(s,"bytes_sent",0,1);
	statsptr[MASTER_CONNECTS] = stats_get_subnode(s,"reconnects",0,1);
	statsptr[MASTER_PING] = stats_get_subnode(s,"usec_ping",1,1);
	statsptr[MASTER_TIMEDIFF] = stats_get_subnode(s,"usec_timediff",1,1);
}

void master_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		stats_counter_inc(statsptr[id]);
	}
}

void master_stats_add(uint8_t id,uint64_t s) {
	if (id<STATNODES) {
		stats_counter_add(statsptr[id],s);
	}
}

void master_stats_set(uint8_t id,uint64_t s) {
	if (id<STATNODES) {
		stats_counter_set(statsptr[id],s);
	}
}
#else
void master_statsptr_init(void) {
}

void master_stats_inc(uint8_t id) {
	(void)id;
}

void master_stats_add(uint8_t id,uint64_t s) {
	(void)id;
	(void)s;
}

void master_stats_set(uint8_t id,uint64_t s) {
	(void)id;
	(void)s;
}
#endif

const char* errtab[]={MFS_ERROR_STRINGS};

static inline const char* mfs_strerror(uint8_t status) {
	if (status>MFS_ERROR_MAX) {
		status=MFS_ERROR_MAX;
	}
	return errtab[status];
}

// read
void fs_atime(uint32_t inode) {
	amtime_file *amfptr;
	uint32_t amhash;
	pthread_mutex_lock(&amtimelock);
	amhash = inode % AMTIME_HASH_SIZE;
	for (amfptr = amtime_hash[amhash] ; amfptr ; amfptr = amfptr->next) {
		if (amfptr->inode == inode) {
			amfptr->atime = monotonic_useconds()+timediffusec;
			amfptr->atimeage = 0;
			pthread_mutex_unlock(&amtimelock);
			return;
		}
	}
	amfptr = malloc(sizeof(amtime_file));
	amfptr->inode = inode;
	amfptr->atimeage = 0;
	amfptr->mtimeage = 0;
	amfptr->atime = monotonic_useconds()+timediffusec;
	amfptr->mtime = 0;
	amfptr->next = amtime_hash[amhash];
	amtime_hash[amhash] = amfptr;
	pthread_mutex_unlock(&amtimelock);
}

// write
void fs_mtime(uint32_t inode) {
	amtime_file *amfptr;
	uint32_t amhash;
	pthread_mutex_lock(&amtimelock);
	amhash = inode % AMTIME_HASH_SIZE;
	for (amfptr = amtime_hash[amhash] ; amfptr ; amfptr = amfptr->next) {
		if (amfptr->inode == inode) {
			amfptr->mtime = monotonic_useconds()+timediffusec;
			amfptr->mtimeage = 0;
			pthread_mutex_unlock(&amtimelock);
			return;
		}
	}
	amfptr = malloc(sizeof(amtime_file));
	amfptr->inode = inode;
	amfptr->atimeage = 0;
	amfptr->mtimeage = 0;
	amfptr->mtime = monotonic_useconds()+timediffusec;
	amfptr->atime = 0;
	amfptr->next = amtime_hash[amhash];
	amtime_hash[amhash] = amfptr;
	pthread_mutex_unlock(&amtimelock);
}

// set atime (setattr)
void fs_no_atime(uint32_t inode) {
	amtime_file *amfptr;
	uint32_t amhash;
	pthread_mutex_lock(&amtimelock);
	amhash = inode % AMTIME_HASH_SIZE;
	for (amfptr = amtime_hash[amhash] ; amfptr ; amfptr = amfptr->next) {
		if (amfptr->inode == inode) {
			amfptr->atimeage = 0;
			amfptr->atime = 0;
			pthread_mutex_unlock(&amtimelock);
			return;
		}
	}
	pthread_mutex_unlock(&amtimelock);
}

// set mtime (setattr)
void fs_no_mtime(uint32_t inode) {
	amtime_file *amfptr;
	uint32_t amhash;
	pthread_mutex_lock(&amtimelock);
	amhash = inode % AMTIME_HASH_SIZE;
	for (amfptr = amtime_hash[amhash] ; amfptr ; amfptr = amfptr->next) {
		if (amfptr->inode == inode) {
			amfptr->mtimeage = 0;
			amfptr->mtime = 0;
			pthread_mutex_unlock(&amtimelock);
			return;
		}
	}
	pthread_mutex_unlock(&amtimelock);
}

void fs_fix_amtime(uint32_t inode,uint32_t *atime,uint32_t *mtime) {
	amtime_file *amfptr;
	uint32_t amhash;
	uint32_t ioatime,iomtime;
	pthread_mutex_lock(&amtimelock);
	amhash = inode % AMTIME_HASH_SIZE;
	for (amfptr = amtime_hash[amhash] ; amfptr ; amfptr = amfptr->next) {
		if (amfptr->inode == inode) {
			ioatime = amfptr->atime / 1000000;
			iomtime = amfptr->mtime / 1000000;
			if (ioatime > *atime) {
				*atime = ioatime;
			}
			if (iomtime > *mtime) {
				*mtime = iomtime;
			}
			pthread_mutex_unlock(&amtimelock);
			return;
		}
	}
	pthread_mutex_unlock(&amtimelock);
}

void fs_amtime_reference_clock(uint64_t localmonotonic,uint64_t remotewall) {
	pthread_mutex_lock(&amtimelock);
	timediffusec = remotewall - localmonotonic;
	pthread_mutex_unlock(&amtimelock);
}

static void fs_af_remove_from_lru(acquired_file *afptr) {
	if (afptr->lrunext) {
		afptr->lrunext->lruprev = afptr->lruprev;
	} else {
		af_lrutail = afptr->lruprev;
	}
	*(afptr->lruprev) = afptr->lrunext;
	af_lru_cnt--;
	afptr->lrunext = NULL;
	afptr->lruprev = NULL;
}

static void fs_af_add_to_lru(acquired_file *afptr) {
	acquired_file *iafptr,**afpptr;
	uint32_t hash;
	if (af_lru_cnt>ACQFILES_LRU_LIMIT) {
		hash = af_lruhead->inode % ACQFILES_HASH_SIZE;
		afpptr = af_hash + hash;
		while ((iafptr = *afpptr)) {
			if (iafptr==af_lruhead) {
				*afpptr = iafptr->next;
				chunksdatacache_clear_inode(iafptr->inode,0);
				fs_af_remove_from_lru(iafptr);
				free(iafptr);
			} else {
				afpptr = &(iafptr->next);
			}
		}
	}
	massert(af_lru_cnt<=ACQFILES_LRU_LIMIT,"open files lru data mismatch !!!");
	afptr->lruprev = af_lrutail;
	*af_lrutail = afptr;
	afptr->lrunext = NULL;
	af_lrutail = &(afptr->lrunext);
	af_lru_cnt++;
}

void fs_add_entry(uint32_t inode) {
	uint32_t afhash;
	acquired_file *afptr;
	pthread_mutex_lock(&aflock);
	afhash = inode % ACQFILES_HASH_SIZE;
	for (afptr = af_hash[afhash] ; afptr ; afptr = afptr->next) {
		if (afptr->inode == inode) {
			afptr->dentry = 1;
			if (afptr->lruprev!=NULL) {
				fs_af_remove_from_lru(afptr);
			}
			afptr->age = 0;
			pthread_mutex_unlock(&aflock);
			return;
		}
	}
	afptr = (acquired_file*)malloc(sizeof(acquired_file));
	afptr->inode = inode;
	afptr->cnt = 0;
	afptr->dentry = 1;
	afptr->age = 0;
	afptr->lrunext = NULL;
	afptr->lruprev = NULL;
	afptr->next = af_hash[afhash];
	af_hash[afhash] = afptr;
	pthread_mutex_unlock(&aflock);
}

void fs_forget_entry(uint32_t inode) {
	uint32_t afhash;
	acquired_file *afptr;
	pthread_mutex_lock(&aflock);
	afhash = inode % ACQFILES_HASH_SIZE;
	for (afptr = af_hash[afhash] ; afptr ; afptr = afptr->next) {
		if (afptr->inode == inode) {
			afptr->dentry = 0;
			if (afptr->cnt==0 && afptr->lruprev==NULL) {
				fs_af_add_to_lru(afptr);
			}
			afptr->age = 0;
			pthread_mutex_unlock(&aflock);
			return;
		}
	}
	pthread_mutex_unlock(&aflock);
}

int fs_isopen(uint32_t inode) {
	uint32_t afhash;
	acquired_file *afptr;
	pthread_mutex_lock(&aflock);
	afhash = inode % ACQFILES_HASH_SIZE;
	for (afptr = af_hash[afhash] ; afptr ; afptr = afptr->next) {
		if (afptr->inode == inode) {
			if (afptr->dentry || afptr->cnt) {
				pthread_mutex_unlock(&aflock);
				return 1;
			} else {
				pthread_mutex_unlock(&aflock);
				return 0;
			}
		}
	}
	pthread_mutex_unlock(&aflock);
	return 0;
}

void fs_inc_acnt(uint32_t inode) {
	uint32_t afhash;
	acquired_file *afptr;
	pthread_mutex_lock(&aflock);
	afhash = inode % ACQFILES_HASH_SIZE;
	for (afptr = af_hash[afhash] ; afptr ; afptr = afptr->next) {
		if (afptr->inode == inode) {
			afptr->cnt++;
			if (afptr->lruprev!=NULL) {
				fs_af_remove_from_lru(afptr);
			}
			afptr->age = 0;
			pthread_mutex_unlock(&aflock);
			return;
		}
	}
	afptr = (acquired_file*)malloc(sizeof(acquired_file));
	afptr->inode = inode;
	afptr->cnt = 1;
	afptr->dentry = 0;
	afptr->age = 0;
	afptr->lrunext = NULL;
	afptr->lruprev = NULL;
	afptr->next = af_hash[afhash];
	af_hash[afhash] = afptr;
	pthread_mutex_unlock(&aflock);
}

void fs_dec_acnt(uint32_t inode) {
	uint32_t afhash;
	acquired_file *afptr;
	pthread_mutex_lock(&aflock);
	afhash = inode % ACQFILES_HASH_SIZE;
	for (afptr = af_hash[afhash] ; afptr ; afptr = afptr->next) {
		if (afptr->inode == inode) {
			if (afptr->cnt>0) {
				afptr->cnt--;
			}
			if (afptr->cnt==0 && afptr->dentry==0 && afptr->lruprev==NULL) {
				fs_af_add_to_lru(afptr);
			}
			afptr->age = 0;
			pthread_mutex_unlock(&aflock);
			return;
		}
	}
	pthread_mutex_unlock(&aflock);
}

void fs_read_notify(uint64_t bytes) {
#if HAVE_ATOMICS
	atomic_fetch_add(&rbyt,bytes);
	atomic_fetch_add(&rcnt,1);
#elif HAVE_SYNCS
	__sync_add_and_fetch(&rbyt,bytes);
	__sync_add_and_fetch(&rcnt,1);
#else
	pthread_mutex_lock(&opdatalock);
	rbyt+=bytes;
	rcnt++;
	pthread_mutex_unlock(&opdatalock);
#endif
}

void fs_write_notify(uint64_t bytes) {
#if HAVE_ATOMICS
	atomic_fetch_add(&wbyt,bytes);
	atomic_fetch_add(&wcnt,1);
#elif HAVE_SYNCS
	__sync_add_and_fetch(&wbyt,bytes);
	__sync_add_and_fetch(&wcnt,1);
#else
	pthread_mutex_lock(&opdatalock);
	wbyt+=bytes;
	wcnt++;
	pthread_mutex_unlock(&opdatalock);
#endif
}

void fs_fsync_notify(void) {
#if HAVE_ATOMICS
	atomic_fetch_add(&fcnt,1);
#elif HAVE_SYNCS
	__sync_add_and_fetch(&fcnt,1);
#else
	pthread_mutex_lock(&opdatalock);
	fcnt++;
	pthread_mutex_unlock(&opdatalock);
#endif
}

void fs_init_counters(void) {
#if HAVE_ATOMICS
	atomic_fetch_and(&rbyt,0);
	atomic_fetch_and(&rcnt,0);
	atomic_fetch_and(&wbyt,0);
	atomic_fetch_and(&wcnt,0);
	atomic_fetch_and(&fcnt,0);
#elif HAVE_SYNCS
	__sync_fetch_and_and(&rbyt,0);
	__sync_fetch_and_and(&rcnt,0);
	__sync_fetch_and_and(&wbyt,0);
	__sync_fetch_and_and(&wcnt,0);
	__sync_fetch_and_and(&fcnt,0);
#else
	pthread_mutex_lock(&opdatalock);
	rbyt = 0;
	rcnt = 0;
	wbyt = 0;
	wcnt = 0;
	fcnt = 0;
	pthread_mutex_unlock(&opdatalock);
#endif
}

void fs_free_threc(void *vrec) {
	threc *drec = (threc*)vrec;
	threc *rec,**recp;
	uint32_t rechash;

	pthread_mutex_lock(&reclock);
	rechash = drec->packetid % THRECHASHSIZE;
	recp = threchash + rechash;
	while ((rec = *recp)) {
		if (rec==drec) {
			*recp = rec->next;
			rec->next = threcfree;
			threcfree = rec;
			pthread_mutex_lock(&(rec->mutex));
			if (rec->obuff!=NULL) {
				free(rec->obuff);
				rec->obuff = NULL;
				rec->obuffsize = 0;
			}
			if (rec->ibuff!=NULL) {
				free(rec->ibuff);
				rec->ibuff = NULL;
				rec->ibuffsize = 0;
			}
			pthread_mutex_unlock(&(rec->mutex));
			pthread_mutex_unlock(&reclock);
			return;
		} else {
			recp = &(rec->next);
		}
	}
	pthread_mutex_unlock(&reclock);
	mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"threc not found in data structures !!!");
}

threc* fs_get_my_threc(void) {
	threc *rec;
	uint32_t rechash;

	rec = pthread_getspecific(reckey);
	if (rec!=NULL) {
		return rec;
	}
	pthread_mutex_lock(&reclock);
	if (threcfree!=NULL) {
		rec = threcfree;
		threcfree = rec->next;
	} else {
		rec = malloc(sizeof(threc));
		rec->packetid = ++threcnextid;
		pthread_mutex_init(&(rec->mutex),NULL);
		pthread_cond_init(&(rec->cond),NULL);
	}
	rechash = rec->packetid % THRECHASHSIZE;
	rec->next = threchash[rechash];
	threchash[rechash] = rec;
	rec->obuff = NULL;
	rec->ibuff = NULL;
	rec->obuffsize = 0;
	rec->ibuffsize = 0;
	rec->odataleng = 0;
	rec->idataleng = 0;
	rec->sent = 0;
	rec->status = 0;
	rec->rcvd = 0;
	rec->receiving = 0;
	rec->rcvd_cmd = 0;
	pthread_mutex_unlock(&reclock);
	pthread_setspecific(reckey,rec);
	return rec;
}

threc* fs_get_threc_by_id(uint32_t packetid) {
	threc *rec;
	uint32_t rechash;
	rechash = packetid % THRECHASHSIZE;
	pthread_mutex_lock(&reclock);
	for (rec = threchash[rechash] ; rec!=NULL ; rec=rec->next) {
		if (rec->packetid==packetid) {
			pthread_mutex_unlock(&reclock);
			return rec;
		}
	}
	pthread_mutex_unlock(&reclock);
	mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"packet: %"PRIu32" - record not found !!!",packetid);
	return NULL;
}

void fs_output_buffer_init(threc *rec,uint32_t size) {
	if (size>DEFAULT_OUTPUT_BUFFSIZE) {
		if (rec->obuff) {
			free(rec->obuff);
		}
		rec->obuff = malloc(size);
		passert(rec->obuff);
		rec->obuffsize = size;
	} else if (rec->obuffsize!=DEFAULT_OUTPUT_BUFFSIZE) {
		if (rec->obuff) {
			free(rec->obuff);
		}
		rec->obuff = malloc(DEFAULT_OUTPUT_BUFFSIZE);
		passert(rec->obuff);
		rec->obuffsize = DEFAULT_OUTPUT_BUFFSIZE;
	}
//	if (rec->obuff==NULL) {
//		rec->obuffsize = 0;
//	}
}

void fs_input_buffer_init(threc *rec,uint32_t size) {
	if (size>DEFAULT_INPUT_BUFFSIZE) {
		if (rec->ibuff) {
			free(rec->ibuff);
		}
		rec->ibuff = malloc(size);
		passert(rec->ibuff);
		rec->ibuffsize = size;
	} else if (rec->ibuffsize!=DEFAULT_INPUT_BUFFSIZE) {
		if (rec->ibuff) {
			free(rec->ibuff);
		}
		rec->ibuff = malloc(DEFAULT_INPUT_BUFFSIZE);
		passert(rec->ibuff);
		rec->ibuffsize = DEFAULT_INPUT_BUFFSIZE;
	}
//	if (rec->ibuff==NULL) {
//		rec->ibuffsize = 0;
//	}
}

uint8_t* fs_createpacket(threc *rec,uint32_t cmd,uint32_t size) {
	uint8_t *ptr;
	uint32_t hdrsize = size+4;
	pthread_mutex_lock(&(rec->mutex));	// make helgrind happy
	fs_output_buffer_init(rec,size+12);
	if (rec->obuff==NULL) {
		return NULL;
	}
	ptr = rec->obuff;
	put32bit(&ptr,cmd);
	put32bit(&ptr,hdrsize);
	put32bit(&ptr,rec->packetid);
	rec->odataleng = size+12;
	pthread_mutex_unlock(&(rec->mutex));	// make helgrind happy
	return ptr;
}

static inline void fs_disconnect(void) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	(void)__sync_fetch_and_or(&disconnect,1);
#else
	pthread_mutex_lock(&fdlock);
	disconnect = 1;
	pthread_mutex_unlock(&fdlock);
#endif
}

const uint8_t* fs_sendandreceive(threc *rec,uint32_t expected_cmd,uint32_t *answer_leng) {
	uint32_t cnt;
	static uint8_t notsup = MFS_ERROR_ENOTSUP;
	uint64_t start,period,usecto;
//	uint32_t size = rec->size;

	start = 0; // make static code analysers happy
	if (usectimeout>0) {
		start = monotonic_useconds();
	}
	for (cnt=1 ; cnt<=maxretries ; cnt++) {
		pthread_mutex_lock(&fdlock);
		if (sessionlost==1) {
			pthread_mutex_unlock(&fdlock);
			return NULL;
		}
		if (fd==-1) {
			pthread_mutex_unlock(&fdlock);
			usecto = 1000+((cnt<30)?((cnt-1)*300000):10000000);
			if (usectimeout>0) {
				period = monotonic_useconds() - start;
				if (period >= usectimeout) {
					return NULL;
				}
				if (usecto > usectimeout - period) {
					usecto = usectimeout - period;
				}
			}
			portable_usleep(usecto);
			continue;
		}
		//mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"threc(%"PRIu32") - sending ...",rec->packetid);
		pthread_mutex_lock(&(rec->mutex));	// make helgrind happy
		if (tcptowrite(fd,rec->obuff,rec->odataleng,1000,send_timeout*10)!=(int32_t)(rec->odataleng)) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"tcp send error: %s",strerr(errno));
#ifdef HAVE___SYNC_FETCH_AND_OP
			(void)__sync_fetch_and_or(&disconnect,1);
#else
			disconnect = 1;
#endif
			pthread_mutex_unlock(&(rec->mutex));
			pthread_mutex_unlock(&fdlock);
			usecto = 1000+((cnt<30)?((cnt-1)*300000):10000000);
			if (usectimeout>0) {
				period = monotonic_useconds() - start;
				if (period >= usectimeout) {
					return NULL;
				}
				if (usecto > usectimeout - period) {
					usecto = usectimeout - period;
				}
			}
			portable_usleep(usecto);
			continue;
		}
		rec->rcvd = 0;
		rec->sent = 1;
		pthread_mutex_unlock(&(rec->mutex));	// make helgrind happy
		master_stats_add(MASTER_BYTESSENT,rec->odataleng);
		master_stats_inc(MASTER_PACKETSSENT);
		lastwrite = monotonic_seconds();
		pthread_mutex_unlock(&fdlock);
		// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: lock: %"PRIu32,rec->packetid);
		pthread_mutex_lock(&(rec->mutex));
		while (rec->rcvd==0) {
//			rec->waiting = 1;
			if (usectimeout>0) {
				struct timespec ts;
				struct timeval tv;
				period = monotonic_useconds() - start;
				if (period >= usectimeout) {
					pthread_mutex_unlock(&(rec->mutex));
					return NULL;
				}
				period = usectimeout - period;
				gettimeofday(&tv, NULL);
				usecto = tv.tv_sec;
				usecto *= 1000000;
				usecto += tv.tv_usec;
				usecto += period;
				ts.tv_sec = usecto / 1000000;
				ts.tv_nsec = (usecto % 1000000) * 1000;
				if (pthread_cond_timedwait(&(rec->cond),&(rec->mutex),&ts)==ETIMEDOUT) {
//					rec->waiting = 0;
					pthread_mutex_unlock(&(rec->mutex));
					return NULL;
				}
			} else {
				pthread_cond_wait(&(rec->cond),&(rec->mutex));
			}
//			rec->waiting = 0;
		}
		*answer_leng = rec->idataleng;
		// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: unlocked: %"PRIu32,rec->packetid);
		// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: command_info: %"PRIu32" ; reccmd: %"PRIu32,command_info,rec->cmd);
		if (rec->status!=0) {
			pthread_mutex_unlock(&(rec->mutex));
			usecto = 1000+((cnt<30)?((cnt-1)*300000):10000000);
			if (usectimeout>0) {
				period = monotonic_useconds() - start;
				if (period >= usectimeout) {
					return NULL;
				}
				if (usecto > usectimeout - period) {
					usecto = usectimeout - period;
				}
			}
			portable_usleep(usecto);
			continue;
		}
		if (rec->rcvd_cmd==ANTOAN_UNKNOWN_COMMAND || rec->rcvd_cmd==ANTOAN_BAD_COMMAND_SIZE) {
			pthread_mutex_unlock(&(rec->mutex));
			*answer_leng = 1; // simulate error
			return &notsup; // return MFS_ERROR_ENOTSUP in this case
		}
		if (rec->rcvd_cmd!=expected_cmd) {
			pthread_mutex_unlock(&(rec->mutex));
			fs_disconnect();
			usecto = 1000+((cnt<30)?((cnt-1)*300000):10000000);
			if (usectimeout>0) {
				period = monotonic_useconds() - start;
				if (period >= usectimeout) {
					return NULL;
				}
				if (usecto > usectimeout - period) {
					usecto = usectimeout - period;
				}
			}
			portable_usleep(usecto);
			continue;
		}
		pthread_mutex_unlock(&(rec->mutex));
		//mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"threc(%"PRIu32") - received",rec->packetid);
		return rec->ibuff;
	}
	return NULL;
}

const uint8_t* fs_sendandreceive_any(threc *rec,uint32_t *received_cmd,uint32_t *answer_leng) {
	uint32_t cnt;
	uint64_t start,period,usecto;
//	uint32_t size = rec->size;

	start = 0; // make static code analysers happy
	if (usectimeout>0) {
		start = monotonic_useconds();
	}
	for (cnt=1 ; cnt<=maxretries ; cnt++) {
		pthread_mutex_lock(&fdlock);
		if (sessionlost==1) {
			pthread_mutex_unlock(&fdlock);
			return NULL;
		}
		if (fd==-1) {
			pthread_mutex_unlock(&fdlock);
			usecto = 1000+((cnt<30)?((cnt-1)*300000):10000000);
			if (usectimeout>0) {
				period = monotonic_useconds() - start;
				if (period >= usectimeout) {
					return NULL;
				}
				if (usecto > usectimeout - period) {
					usecto = usectimeout - period;
				}
			}
			portable_usleep(usecto);
			continue;
		}
		//mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"threc(%"PRIu32") - sending ...",rec->packetid);
		pthread_mutex_lock(&(rec->mutex));	// make helgrind happy
		if (tcptowrite(fd,rec->obuff,rec->odataleng,1000,send_timeout*1000)!=(int32_t)(rec->odataleng)) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"tcp send error: %s",strerr(errno));
#ifdef HAVE___SYNC_FETCH_AND_OP
			(void)__sync_fetch_and_or(&disconnect,1);
#else
			disconnect = 1;
#endif
			pthread_mutex_unlock(&(rec->mutex));
			pthread_mutex_unlock(&fdlock);
			usecto = 1000+((cnt<30)?((cnt-1)*300000):10000000);
			if (usectimeout>0) {
				period = monotonic_useconds() - start;
				if (period >= usectimeout) {
					return NULL;
				}
				if (usecto > usectimeout - period) {
					usecto = usectimeout - period;
				}
			}
			portable_usleep(usecto);
			continue;
		}
		rec->rcvd = 0;
		rec->sent = 1;
		pthread_mutex_unlock(&(rec->mutex));	// make helgrind happy
		master_stats_add(MASTER_BYTESSENT,rec->odataleng);
		master_stats_inc(MASTER_PACKETSSENT);
		lastwrite = monotonic_seconds();
		pthread_mutex_unlock(&fdlock);
		// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: lock: %"PRIu32,rec->packetid);
		pthread_mutex_lock(&(rec->mutex));
		while (rec->rcvd==0) {
//			rec->waiting = 1;
			if (usectimeout>0) {
				struct timespec ts;
				struct timeval tv;
				period = monotonic_useconds() - start;
				if (period >= usectimeout) {
					pthread_mutex_unlock(&(rec->mutex));
					return NULL;
				}
				period = usectimeout - period;
				gettimeofday(&tv, NULL);
				usecto = tv.tv_sec;
				usecto *= 1000000;
				usecto += tv.tv_usec;
				usecto += period;
				ts.tv_sec = usecto / 1000000;
				ts.tv_nsec = (usecto % 1000000) * 1000;
				if (pthread_cond_timedwait(&(rec->cond),&(rec->mutex),&ts)==ETIMEDOUT) {
//					rec->waiting = 0;
					pthread_mutex_unlock(&(rec->mutex));
					return NULL;
				}
			} else {
				pthread_cond_wait(&(rec->cond),&(rec->mutex));
			}
//			rec->waiting = 0;
		}
		*answer_leng = rec->idataleng;
		// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: unlocked: %"PRIu32,rec->packetid);
		// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: command_info: %"PRIu32" ; reccmd: %"PRIu32,command_info,rec->cmd);
		if (rec->status!=0) {
			pthread_mutex_unlock(&(rec->mutex));
			usecto = 1000+((cnt<30)?((cnt-1)*300000):10000000);
			if (usectimeout>0) {
				period = monotonic_useconds() - start;
				if (period >= usectimeout) {
					return NULL;
				}
				if (usecto > usectimeout - period) {
					usecto = usectimeout - period;
				}
			}
			portable_usleep(usecto);
			continue;
		}
		*received_cmd = rec->rcvd_cmd;
		pthread_mutex_unlock(&(rec->mutex));
		//mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"threc(%"PRIu32") - received",rec->packetid);
		return rec->ibuff;
	}
	return NULL;
}

int fs_resolve(uint8_t oninit,const char *bindhostname,const char *masterhostname,const char *masterportname) {
	if (bindhostname) {
		if (tcpresolve(bindhostname,NULL,&srcip,NULL,1)<0) {
			if (oninit) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"can't resolve source hostname (%s)",bindhostname);
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't resolve source hostname (%s)",bindhostname);
			}
			return -1;
		}
	} else {
		srcip=0;
	}
	univmakestrip(srcstrip,srcip);

	if (tcpresolve(masterhostname,masterportname,&masterip,&masterport,0)<0) {
		if (oninit) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"can't resolve master hostname and/or portname (%s:%s)",masterhostname,masterportname);
		} else {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't resolve master hostname and/or portname (%s:%s)",masterhostname,masterportname);
		}
		return -1;
	}
	univmakestrip(masterstrip,masterip);

	return 0;
}

const char* fs_get_current_srcstrip(void) {
	return srcstrip;
}

const char* fs_get_current_masterstrip(void) {
	return masterstrip;
}

uint16_t fs_get_current_masterport(void) {
	return masterport;
}


// int fs_connect(uint8_t oninit,const char *bindhostname,const char *masterhostname,const char *masterportname,uint8_t meta,const char *info,const char *subfolder,const uint8_t passworddigest[16],uint8_t *sesflags,uint32_t *rootuid,uint32_t *rootgid,uint32_t *mapalluid,uint32_t *mapallgid,uint8_t *mingoal,uint8_t *maxgoal,uint32_t *mintrashretention,uint32_t *maxtrashretention) {
int fs_connect(uint8_t oninit,struct connect_args_t *cargs) {
	uint32_t i,j;
	uint8_t *wptr,*regbuff;
	md5ctx ctx;
	uint8_t digest[16];
	const uint8_t *rptr;
	uint32_t newmasterip;
	uint8_t havepassword;
	uint32_t pleng,ileng;
	uint8_t sesflags;
	uint16_t umaskval;
	uint32_t rootuid,rootgid,mapalluid,mapallgid;
	uint8_t mingoal,maxgoal;
	int32_t sclassgroups;
	uint32_t mintrashretention,maxtrashretention;
	uint32_t disables;
	int32_t rleng;
	const char* disablestr[]={DISABLE_STRINGS};
	const char *sesflagposstrtab[]={SESFLAG_POS_STRINGS};
	const char *sesflagnegstrtab[]={SESFLAG_NEG_STRINGS};
	char *infobuff;
	uint32_t ibleng;
#ifndef WIN32
	struct passwd pwd,*pw;
	struct group grp,*gr;
	char pwdgrpbuff[16384];
#endif
	static uint32_t trycnt=0;
	static uint32_t redirect_count=0;

	if (fs_resolve(oninit,cargs->bindhostname,cargs->masterhostname,cargs->masterportname)<0) {
		return -1;
	}

	havepassword = (cargs->passworddigest==NULL)?0:1;
	ileng = strlen(cargs->info)+1;
	if (cargs->meta) {
		pleng = 0;
		rleng = 9;
	} else {
		pleng = strlen(cargs->subfolder)+1;
		rleng = 13;
	}
	rleng += 8+64+pleng+ileng;
	if (havepassword) {
		rleng += 16;
	}
	if (sessionlost==2) {
		rleng += 4;
		if (metaid!=0) {
			rleng += 8;
		}
	}
	regbuff = malloc(rleng);

	do {
		fd = tcpsocket();
		if (fd<0) {
			free(regbuff);
			return -1;
		}
		if (tcpnodelay(fd)<0) {
			if (oninit) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"can't set TCP_NODELAY");
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"can't set TCP_NODELAY");
			}
		}
		if (srcip>0) {
			if (tcpnumbind(fd,srcip,0)<0) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"can't bind socket to given ip (\"%s\")",srcstrip);
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't bind socket to given ip (\"%s\")",srcstrip);
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
		}
		if (tcpnumtoconnect(fd,masterip,masterport,CONNECT_TIMEOUT)<0) {
			tcpclose(fd);
			fd=-1;
			if (oninit) {
				if (trycnt<10) {
					trycnt++;
					if (fs_resolve(oninit,cargs->bindhostname,cargs->masterhostname,cargs->masterportname)<0) {
						free(regbuff);
						return -1;
					}
					i=4;
					continue;
				} else {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"can't connect to mfsmaster (\"%s\":\"%"PRIu16"\")",masterstrip,masterport);
					free(regbuff);
					return -1;
				}
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't connect to mfsmaster (\"%s\":\"%"PRIu16"\")",masterstrip,masterport);
				free(regbuff);
				return -1;
			}
		}
		if (havepassword) {
			wptr = regbuff;
			put32bit(&wptr,CLTOMA_FUSE_REGISTER);
			put32bit(&wptr,65);
			memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
			wptr+=64;
			put8bit(&wptr,REGISTER_GETRANDOM);
			if (tcptowrite(fd,regbuff,8+65,1000,send_timeout*1000)!=8+65) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error sending data to mfsmaster");
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error sending data to mfsmaster");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			if (tcptoread(fd,regbuff,8,1000,recv_timeout*1000)!=8) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error receiving data from mfsmaster");
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error receiving data from mfsmaster");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			rptr = regbuff;
			i = get32bit(&rptr);
			if (i!=MATOCL_FUSE_REGISTER) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"got incorrect answer from mfsmaster");
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"got incorrect answer from mfsmaster");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			i = get32bit(&rptr);
			if (i!=32) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"got incorrect answer from mfsmaster");
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"got incorrect answer from mfsmaster");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			if (tcptoread(fd,regbuff,32,1000,recv_timeout*1000)!=32) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error receiving data from mfsmaster");
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error receiving data from mfsmaster");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			md5_init(&ctx);
			md5_update(&ctx,regbuff,16);
			md5_update(&ctx,cargs->passworddigest,16);
			md5_update(&ctx,regbuff+16,16);
			md5_final(digest,&ctx);
		}
		wptr = regbuff;
		put32bit(&wptr,CLTOMA_FUSE_REGISTER);
		put32bit(&wptr,rleng-8);
		memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
		wptr+=64;
		put8bit(&wptr,(cargs->meta)?REGISTER_NEWMETASESSION:REGISTER_NEWSESSION);
		put16bit(&wptr,VERSMAJ);
		put8bit(&wptr,VERSMID);
		put8bit(&wptr,VERSMIN);
		put32bit(&wptr,ileng);
		memcpy(wptr,cargs->info,ileng);
		wptr+=ileng;
		if (!cargs->meta) {
			put32bit(&wptr,pleng);
			memcpy(wptr,cargs->subfolder,pleng);
			wptr+=pleng;
		}
		if (sessionlost==2) {
			put32bit(&wptr,sessionid);
			if (metaid!=0) {
				put64bit(&wptr,metaid);
			}
		}
		if (havepassword) {
			memcpy(wptr,digest,16);
		}
		if (tcptowrite(fd,regbuff,rleng,1000,send_timeout*1000)!=rleng) {
			if (oninit) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error sending data to mfsmaster: %s",strerr(errno));
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error sending data to mfsmaster: %s",strerr(errno));
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		if (tcptoread(fd,regbuff,8,1000,recv_timeout*1000)!=8) {
			if (oninit) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error receiving data from mfsmaster: %s",strerr(errno));
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error receiving data from mfsmaster: %s",strerr(errno));
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		rptr = regbuff;
		i = get32bit(&rptr);
		if (i==MATOCL_HA_LEADER_REDIRECT) {
			/* Handle HA leader redirection */
			redirect_count++;
			if (redirect_count > 5) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"too many redirects (%u) - giving up",redirect_count);
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"too many redirects (%u) - giving up",redirect_count);
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			i = get32bit(&rptr);  /* Get data length */
			if (i != 6) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"invalid HA redirect response length");
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"invalid HA redirect response length");
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			if (tcptoread(fd,regbuff,6,1000,recv_timeout*1000)!=6) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error receiving redirect data from mfsmaster: %s",strerr(errno));
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error receiving redirect data from mfsmaster: %s",strerr(errno));
				}
				tcpclose(fd);
				fd=-1;
				free(regbuff);
				return -1;
			}
			rptr = regbuff;
			uint32_t leader_ip = get32bit(&rptr);
			uint16_t leader_port = get16bit(&rptr);
			
			if (oninit) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"mfsmaster redirected to leader at %u.%u.%u.%u:%u",
				        (leader_ip >> 24) & 0xFF, (leader_ip >> 16) & 0xFF, (leader_ip >> 8) & 0xFF, leader_ip & 0xFF, leader_port);
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"mfsmaster redirected to leader at %u.%u.%u.%u:%u",
				        (leader_ip >> 24) & 0xFF, (leader_ip >> 16) & 0xFF, (leader_ip >> 8) & 0xFF, leader_ip & 0xFF, leader_port);
			}
			
			/* Close current connection */
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			
			/* Try to connect to the leader */
			if (leader_ip == 0) {
				/* leader_ip=0 means use same IP we connected to */
				leader_ip = srcip;
			}
			
			/* Prevent redirect loops - don't redirect to the same server */
			if (oninit) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_DEBUG,"redirect check: leader=%u.%u.%u.%u:%u current=%u.%u.%u.%u:%u",
				        (leader_ip >> 24) & 0xFF, (leader_ip >> 16) & 0xFF, (leader_ip >> 8) & 0xFF, leader_ip & 0xFF, leader_port,
				        (masterip >> 24) & 0xFF, (masterip >> 16) & 0xFF, (masterip >> 8) & 0xFF, masterip & 0xFF, masterport);
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"redirect check: leader=%u.%u.%u.%u:%u current=%u.%u.%u.%u:%u",
				        (leader_ip >> 24) & 0xFF, (leader_ip >> 16) & 0xFF, (leader_ip >> 8) & 0xFF, leader_ip & 0xFF, leader_port,
				        (masterip >> 24) & 0xFF, (masterip >> 16) & 0xFF, (masterip >> 8) & 0xFF, masterip & 0xFF, masterport);
			}
			if (leader_ip == masterip && leader_port == masterport) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"redirect loop detected - leader points to same server");
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"redirect loop detected - leader points to same server");
				}
				return -1;
			}
			
			/* Update master connection info to point to leader */
			masterip = leader_ip;
			masterport = leader_port;
			univmakestrip(masterstrip, masterip);
			
			/* Retry connection with leader */
			return fs_connect(oninit, cargs);
		} else if (i!=MATOCL_FUSE_REGISTER) {
			if (oninit) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"got incorrect answer from mfsmaster");
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"got incorrect answer from mfsmaster");
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		i = get32bit(&rptr);
		if (!(i==1 || i==4 || (cargs->meta && (i==19 || i==27 || i==35)) || (cargs->meta==0 && (i==35 || i==43 || i==45 || i==49 || i==57)))) {
			if (oninit) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"got incorrect answer from mfsmaster");
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"got incorrect answer from mfsmaster");
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		if (tcptoread(fd,regbuff,i,1000,recv_timeout*1000)!=(int32_t)i) {
			if (oninit) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"error receiving data from mfsmaster: %s",strerr(errno));
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"error receiving data from mfsmaster: %s",strerr(errno));
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		rptr = regbuff;
		if (i==1) {
			if (oninit) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"mfsmaster register error: %s",mfs_strerror(rptr[0]));
			} else {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"mfsmaster register error: %s",mfs_strerror(rptr[0]));
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		if (i==4) {
			// redirect
			newmasterip = get32bit(&rptr);
			if (newmasterip==0 || newmasterip==masterip) {
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"mfsmaster %s - got empty redirect - retrying",masterstrip);
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"mfsmaster %s - got empty redirect - retrying",masterstrip);
				}
				tcpclose(fd);
				fd = -1;
				if (oninit) {
					portable_sleep(2);
				} else {
					free(regbuff);
					return -1;
				}
			} else {
				char newmasterstrip[STRIPSIZE];
				univmakestrip(newmasterstrip,newmasterip);
				if (oninit) {
					mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"mfsmaster %s - got redirect to %s",masterstrip,newmasterstrip);
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"mfsmaster %s - got redirect to %s",masterstrip,newmasterstrip);
				}
				masterip = newmasterip;
				strcpy(masterstrip,newmasterstrip);
				tcpclose(fd);
				fd = -1;
			}
		}
	} while (i==4);
	masterversion = get32bit(&rptr);
	if (masterversion < VERSION2INT(2,1,7) || masterversion < cargs->minversion) {
		if (oninit) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"incompatible mfsmaster version");
		} else {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"incompatible mfsmaster version");
		}
		tcpclose(fd);
		fd=-1;
		free(regbuff);
		return -1;
	}
	attrsize = (masterversion>=VERSION2INT(3,0,93)&&masterversion!=VERSION2INT(4,0,0)&&masterversion!=VERSION2INT(4,0,1))?ATTR_RECORD_SIZE:35;
	sessionid = get32bit(&rptr);
	if ((cargs->meta && (i==27 || i==35)) || (cargs->meta==0 && (i==43 || i==45 || i==49 || i==57))) {
		metaid = get64bit(&rptr);
	}
	sesflags = get8bit(&rptr);
	if (!cargs->meta) {
		if (i==45 || i==49 || i==57) {
			umaskval = get16bit(&rptr);
		} else {
			umaskval = 0;
		}
		rootuid = get32bit(&rptr);
		rootgid = get32bit(&rptr);
		mapalluid = get32bit(&rptr);
		mapallgid = get32bit(&rptr);
	} else {
		umaskval = 0;
		rootuid = 0;
		rootgid = 0;
		mapalluid = 0;
		mapallgid = 0;
	}
	if (masterversion < VERSION2INT(4,57,0)) {
		mingoal = get8bit(&rptr);
		maxgoal = get8bit(&rptr);
		sclassgroups = -1;
	} else {
		mingoal = 1;
		maxgoal = 9;
		sclassgroups = get16bit(&rptr);
	}
	mintrashretention = get32bit(&rptr);
	maxtrashretention = get32bit(&rptr);
	if (!cargs->meta) {
		if (i==49 || i==57) {
			disables = get32bit(&rptr);
		} else {
			disables = 0;
		}
	} else {
		disables = 0;
	}
	if ((cargs->meta && i==35) || (cargs->meta==0 && i==57)) {
		masterprocessid = get64bit(&rptr);
	} else { // this is old master - use meta id as a processid (usually it is good enough - as long as you do not have two instances with the same metaid)
		masterprocessid = metaid;
	}
	free(regbuff);
	lastwrite = monotonic_seconds();
#ifdef MFSMOUNT
	mfs_setdisables(disables);
	main_setparams(sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mingoal,maxgoal,mintrashretention,maxtrashretention,disables);
#endif
	if (oninit==0) {
		if (sessionlost==2) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"registered to master using previous session");
		} else {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"registered to master with new session");
		}
	}
	if (cargs->clearpassword && cargs->passworddigest!=NULL) {
		memset(cargs->passworddigest,0,16);
		free(cargs->passworddigest);
		cargs->passworddigest = NULL;
	}
#define INFOBUFF_SIZE 4096
	infobuff = malloc(INFOBUFF_SIZE);
	ibleng = 0;
#define buffprintf(...) if (ibleng<INFOBUFF_SIZE) ibleng+=snprintf(infobuff+ibleng,INFOBUFF_SIZE-ibleng,__VA_ARGS__)
	j=0;
	for (i=0 ; i<8 ; i++) {
		if (sesflags&(1<<i)) {
			buffprintf("%s%s",j?",":"",sesflagposstrtab[i]);
			j=1;
		} else if (sesflagnegstrtab[i]) {
			buffprintf("%s%s",j?",":"",sesflagnegstrtab[i]);
			j=1;
		}
	}
	if (j==0) {
		buffprintf("-");
	}
	if (!cargs->meta) {
#ifdef WIN32
		buffprintf(" ; root mapped to %"PRIu32":%"PRIu32,rootuid,rootgid);
		if (sesflags&SESFLAG_MAPALL) {
			buffprintf(" ; users mapped to %"PRIu32":%"PRIu32,mapalluid,mapallgid);
		}
#else
		if (umaskval!=0) {
			buffprintf(" ; global umask set to 0%03"PRIo16,umaskval);
		}
		buffprintf(" ; root mapped to ");
		getpwuid_r(rootuid,&pwd,pwdgrpbuff,16384,&pw);
//		pw = getpwuid(rootuid);
		if (pw) {
			buffprintf("%s:",pw->pw_name);
		} else {
			buffprintf("%"PRIu32":",rootuid);
		}
		getgrgid_r(rootgid,&grp,pwdgrpbuff,16384,&gr);
//		gr = getgrgid(rootgid);
		if (gr) {
			buffprintf("%s",gr->gr_name);
		} else {
			buffprintf("%"PRIu32,rootgid);
		}
		if (sesflags&SESFLAG_MAPALL) {
			buffprintf(" ; users mapped to ");
			pw = getpwuid(mapalluid);
			if (pw) {
				buffprintf("%s:",pw->pw_name);
			} else {
				buffprintf("%"PRIu32":",mapalluid);
			}
			gr = getgrgid(mapallgid);
			if (gr) {
				buffprintf("%s",gr->gr_name);
			} else {
				buffprintf("%"PRIu32,mapallgid);
			}
		}
#endif
	}
	if ((mingoal>0 && maxgoal>0)) {
		if (mingoal>1 || maxgoal<9) {
			buffprintf(" ; setgoal limited to (%u:%u)",mingoal,maxgoal);
		}
		if (sclassgroups>=0) {
			buffprintf(" ; sclass groups allowed: ");
			if (sclassgroups==0xFFFF) {
				buffprintf("ALL");
			} else {
				j = 0;
				for (i=0 ; i<EXPORT_GROUPS ; i++) {
					if ((1<<i) & sclassgroups) {
						if (j) {
							buffprintf(",%u",i);
						} else {
							buffprintf("%u",i);
							j = 1;
						}
					}
				}
				if (j==0) {
					buffprintf("-");
				}
			}
		}
		if (mintrashretention>0 || maxtrashretention<UINT32_C(0xFFFFFFFF)) {
			buffprintf(" ; settrashretention limited to (");
			if (mintrashretention>0) {
				if (mintrashretention>604800) {
					buffprintf("%uw",mintrashretention/604800);
					mintrashretention %= 604800;
				}
				if (mintrashretention>86400) {
					buffprintf("%ud",mintrashretention/86400);
					mintrashretention %= 86400;
				}
				if (mintrashretention>3600) {
					buffprintf("%uh",mintrashretention/3600);
					mintrashretention %= 3600;
				}
				if (mintrashretention>60) {
					buffprintf("%um",mintrashretention/60);
					mintrashretention %= 60;
				}
				if (mintrashretention>0) {
					buffprintf("%us",mintrashretention);
				}
			} else {
				buffprintf("0s");
			}
			buffprintf(":");
			if (maxtrashretention>0) {
				if (maxtrashretention>604800) {
					buffprintf("%uw",maxtrashretention/604800);
					maxtrashretention %= 604800;
				}
				if (maxtrashretention>86400) {
					buffprintf("%ud",maxtrashretention/86400);
					maxtrashretention %= 86400;
				}
				if (maxtrashretention>3600) {
					buffprintf("%uh",maxtrashretention/3600);
					maxtrashretention %= 3600;
				}
				if (maxtrashretention>60) {
					buffprintf("%um",maxtrashretention/60);
					maxtrashretention %= 60;
				}
				if (maxtrashretention>0) {
					buffprintf("%us",maxtrashretention);
				}
			} else {
				buffprintf("0s");
			}
			buffprintf(")");
		}
	}
	if (disables>0) {
		int s;
		buffprintf(" ; disabled commands: ");
		s = 0;
		for (i=0,j=1 ; disablestr[i]!=NULL ; i++,j<<=1) {
			if (disables&j) {
				buffprintf("%s%s",s?",":"",disablestr[i]);
				s = 1;
			}
		}
	}
	buffprintf("\n");
	if (ibleng<INFOBUFF_SIZE) {
		infobuff[ibleng]='\0';
	} else {
		infobuff[INFOBUFF_SIZE-1]='\0';
	}
	if (oninit) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"mfsmaster accepted connection with parameters: %s",infobuff);
	} else {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"mfsmaster accepted connection with parameters: %s",infobuff);
	}
	free(infobuff);
	
	// Try HA cluster discovery after successful connection
	if (ha_discover_cluster() < 0) {
		// HA discovery failed, but continue with single master mode
		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"HA cluster discovery failed, continuing in single master mode");
	} else if (ha_cluster.ha_mode) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"Connected to HA cluster with %"PRIu32" nodes, %"PRIu32" shards", 
			ha_cluster.node_count, ha_cluster.shard_count);
	}
	
	/* Reset redirect counter on successful connection */
	redirect_count = 0;
	
	return 0;
}

void fs_reconnect(uint32_t minversion) {
	uint32_t newmasterip;
	uint32_t i;
	uint8_t *wptr,regbuff[8+64+17];
	int32_t rleng;
	const uint8_t *rptr;

	if (sessionid==0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't register using previous sessionid");
		return;
	}

	univmakestrip(masterstrip,masterip);

	do {
		fd = tcpsocket();
		if (fd<0) {
			return;
		}
		if (tcpnodelay(fd)<0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"can't set TCP_NODELAY: %s",strerr(errno));
		}
		if (srcip>0) {
			if (tcpnumbind(fd,srcip,0)<0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't bind socket to given ip (\"%s\")",srcstrip);
				tcpclose(fd);
				fd=-1;
				return;
			}
		}
		if (tcpnumtoconnect(fd,masterip,masterport,CONNECT_TIMEOUT)<0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't connect to master (\"%s\":\"%"PRIu16"\")",masterstrip,masterport);
			tcpclose(fd);
			fd=-1;
			return;
		}
		master_stats_inc(MASTER_CONNECTS);
		wptr = regbuff;
		put32bit(&wptr,CLTOMA_FUSE_REGISTER);
		if (masterversion>=VERSION2INT(3,0,11) && metaid!=0) {
			put32bit(&wptr,81);
			rleng = 8+81;
		} else {
			put32bit(&wptr,73);
			rleng = 8+73;
		}
		memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
		wptr+=64;
		put8bit(&wptr,REGISTER_RECONNECT);
		put32bit(&wptr,sessionid);
		put16bit(&wptr,VERSMAJ);
		put8bit(&wptr,VERSMID);
		put8bit(&wptr,VERSMIN);
		put64bit(&wptr,metaid);
		if (tcptowrite(fd,regbuff,rleng,1000,send_timeout*1000)!=rleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: register error (write: %s)",strerr(errno));
			tcpclose(fd);
			fd=-1;
			return;
		}
		master_stats_add(MASTER_BYTESSENT,rleng);
		master_stats_inc(MASTER_PACKETSSENT);
		if (tcptoread(fd,regbuff,8,1000,recv_timeout*1000)!=8) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: register error (read header: %s)",strerr(errno));
			tcpclose(fd);
			fd=-1;
			return;
		}
		master_stats_add(MASTER_BYTESRCVD,8);
		rptr = regbuff;
		i = get32bit(&rptr);
		if (i!=MATOCL_FUSE_REGISTER) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: register error (bad answer: %"PRIu32")",i);
			tcpclose(fd);
			fd=-1;
			return;
		}
		i = get32bit(&rptr);
		if (i!=1 && i!=4 && i!=5 && i!=13) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: register error (bad length: %"PRIu32")",i);
			tcpclose(fd);
			fd=-1;
			return;
		}
		if (tcptoread(fd,regbuff,i,1000,recv_timeout*1000)!=(int32_t)i) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: register error (read data: %s)",strerr(errno));
			tcpclose(fd);
			fd=-1;
			return;
		}
		master_stats_add(MASTER_BYTESRCVD,i);
		master_stats_inc(MASTER_PACKETSRCVD);
		rptr = regbuff;
		if (i==4) {
			// redirect
			newmasterip = get32bit(&rptr);
			if (newmasterip==0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"mfsmaster %s - got empty redirect",masterstrip);
				tcpclose(fd);
				fd = -1;
				return;
			} else {
				if (newmasterip==masterip) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"mfsmaster %s - got self redirect",masterstrip);
					tcpclose(fd);
					fd = -1;
					return;
				} else {
					masterip = newmasterip;
					univmakestrip(masterstrip,masterip);
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"mfsmaster sent redirect to: %s",masterstrip);
					tcpclose(fd);
					fd = -1;
				}
			}
		}
	} while (i==4);
	if (i>=5) {
		masterversion = get32bit(&rptr);
		if (masterversion < VERSION2INT(2,1,7) || masterversion < minversion) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"incompatible mfsmaster version");
			tcpclose(fd);
			fd=-1;
			return;
		}
		attrsize = (masterversion>=VERSION2INT(3,0,93)&&masterversion!=VERSION2INT(4,0,0)&&masterversion!=VERSION2INT(4,0,1))?ATTR_RECORD_SIZE:35;
		if (i>=13) {
			masterprocessid = get64bit(&rptr);
		} else {
			masterprocessid = metaid;
		}
	}
	if (rptr[0]!=0) {
		sessionlost=(rptr[0]==MFS_ERROR_EPERM)?2:1;
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: register status: %s",mfs_strerror(rptr[0]));
		tcpclose(fd);
		fd=-1;
		return;
	}
	lastwrite = monotonic_seconds();
	lastsyncsend = 0;
	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"registered to master");
}

void fs_close_session(void) {
	uint8_t *wptr,regbuff[8+64+5+8];
	int32_t rleng;

	if (sessionid==0) {
		return;
	}

	wptr = regbuff;
	put32bit(&wptr,CLTOMA_FUSE_REGISTER);
	if (masterversion>=VERSION2INT(3,0,11) && metaid!=0) {
		put32bit(&wptr,77);
		rleng = 8+77;
	} else {
		put32bit(&wptr,69);
		rleng = 8+69;
	}
	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr+=64;
	put8bit(&wptr,REGISTER_CLOSESESSION);
	put32bit(&wptr,sessionid);
	put64bit(&wptr,metaid);
	if (tcptowrite(fd,regbuff,rleng,1000,1000)!=rleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: close session error (write: %s)",strerr(errno));
	}
	if (masterversion>=VERSION2INT(1,7,29)) {
		if (tcptoread(fd,regbuff,9,500,500)!=9) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: close session error (read: %s)",strerr(errno));
		} else if (regbuff[8]!=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: closes session error: %s",mfs_strerror(regbuff[8]));
		}
	}
}

void fs_send_amtime_inodes(void) {
	uint8_t *ptr,*inodespacket;
	int32_t inodesleng;
	amtime_file *amfptr,**amfpptr;
	uint32_t amhash;

	pthread_mutex_lock(&amtimelock);
	//inodesleng=24;
	if (masterversion>=VERSION2INT(3,0,74)) {
		inodesleng=0;
		for (amhash=0 ; amhash < AMTIME_HASH_SIZE ; amhash++) {
			for (amfptr = amtime_hash[amhash] ; amfptr ; amfptr = amfptr->next) {
				if (amfptr->atime > 0 || amfptr->mtime > 0) {
					inodesleng+=12;
				}
			}
		}
		if (inodesleng>0) {
			inodesleng+=8;
			inodespacket = malloc(inodesleng);
			ptr = inodespacket;
			put32bit(&ptr,CLTOMA_FUSE_AMTIME_INODES);
			put32bit(&ptr,inodesleng-8);
			for (amhash=0 ; amhash < AMTIME_HASH_SIZE ; amhash++) {
				amfpptr = amtime_hash + amhash;
				while ((amfptr = *amfpptr)) {
					if (amfptr->atime > 0 || amfptr->mtime > 0) {
						put32bit(&ptr,amfptr->inode);
						put32bit(&ptr,amfptr->atime/1000000);
						put32bit(&ptr,amfptr->mtime/1000000);
					}
					if (amfptr->atimeage>=1) { // sending second time - clear it
						amfptr->atime = 0;
					}
					if (amfptr->mtimeage>=1) { // sending second time - clear it
						amfptr->mtime = 0;
					}
					if (amfptr->atimeage < AMTIME_MAX_AGE || amfptr->mtimeage < AMTIME_MAX_AGE) {
						if (amfptr->atimeage < AMTIME_MAX_AGE) {
							amfptr->atimeage++;
						}
						if (amfptr->mtimeage < AMTIME_MAX_AGE) {
							amfptr->mtimeage++;
						}
						amfpptr = &(amfptr->next);
					} else {
						*amfpptr = amfptr->next;
						free(amfptr);
					}
				}
			}
			pthread_mutex_unlock(&amtimelock);
			if (tcptowrite(fd,inodespacket,inodesleng,1000,send_timeout*1000)!=inodesleng) {
#ifdef HAVE___SYNC_FETCH_AND_OP
				(void)__sync_fetch_and_or(&disconnect,1);
#else
				disconnect = 1;
#endif
			} else {
				master_stats_add(MASTER_BYTESSENT,inodesleng);
				master_stats_inc(MASTER_PACKETSSENT);
			}
			free(inodespacket);
			return;
		}
	}
	for (amhash=0 ; amhash < AMTIME_HASH_SIZE ; amhash++) {
		amfpptr = amtime_hash + amhash;
		while ((amfptr = *amfpptr)) {
			if (amfptr->atimeage < AMTIME_MAX_AGE || amfptr->mtimeage < AMTIME_MAX_AGE) {
				if (amfptr->atimeage < AMTIME_MAX_AGE) {
					amfptr->atimeage++;
				}
				if (amfptr->mtimeage < AMTIME_MAX_AGE) {
					amfptr->mtimeage++;
				}
				amfpptr = &(amfptr->next);
			} else {
				*amfpptr = amfptr->next;
				free(amfptr);
			}
		}
	}
	pthread_mutex_unlock(&amtimelock);
}

void fs_send_open_inodes(void) {
	uint8_t *ptr,*inodespacket;
	uint32_t i,inodes;
	uint32_t hash;
	acquired_file *afptr,**afpptr;
#ifdef MFSDEBUG
	uint32_t inode;
#endif

	pthread_mutex_lock(&aflock);
	//inodesleng=24;
	heap_cleanup();
	for (hash=0 ; hash<ACQFILES_HASH_SIZE ; hash++) {
		afpptr = af_hash + hash;
		while ((afptr = *afpptr)) {
			if (afptr->cnt==0 && afptr->dentry==0) {
				afptr->age++;
				if (afptr->age>ACQFILES_MAX_AGE) {
					*afpptr = afptr->next;
					chunksdatacache_clear_inode(afptr->inode,0);
					fs_af_remove_from_lru(afptr);
					free(afptr);
					continue;
				}
			}
			//mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"sustained inode: %"PRIu32,afptr->inode);
			afpptr = &(afptr->next);
			heap_push(afptr->inode);
		}
	}
	inodes = heap_elements();

	inodespacket = malloc(inodes*4+8);
	ptr = inodespacket;
	if (masterversion>=VERSION2INT(3,0,74)) {
		put32bit(&ptr,CLTOMA_FUSE_SUSTAINED_INODES);
	} else {
		put32bit(&ptr,CLTOMA_FUSE_SUSTAINED_INODES_DEPRECATED);
	}
	put32bit(&ptr,inodes*4);
	//put32bit(&ptr,inodesleng-24);
	//put64bit(&ptr,0);	// readbytes
	//put64bit(&ptr,0);	// writebytes
	// readbytes = 0;
	// writebytes = 0;
	for (i=0 ; i<inodes ; i++) {
#ifdef MFSDEBUG
		inode = heap_pop();
		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"open inode(%"PRIu32"): %"PRIu32,i,inode);
		put32bit(&ptr,inode);
#else
		put32bit(&ptr,heap_pop());
#endif
	}
	pthread_mutex_unlock(&aflock);
	i = inodes * 4 + 8;
	if (tcptowrite(fd,inodespacket,i,1000,send_timeout*1000)!=(int32_t)i) {
#ifdef HAVE___SYNC_FETCH_AND_OP
		(void)__sync_fetch_and_or(&disconnect,1);
#else
		disconnect = 1;
#endif
	} else {
		master_stats_add(MASTER_BYTESSENT,i);
		master_stats_inc(MASTER_PACKETSSENT);
	}
	free(inodespacket);
}

void fs_send_opdata(void) {
	uint8_t packetdata[8+44];
	int32_t senddata;
	uint8_t *wptr;
	uint64_t rbyt_copy,wbyt_copy;
	uint64_t sentbyt_copy,rcvdbyt_copy;
	uint32_t rcnt_copy,wcnt_copy,fcnt_copy;

	senddata = 0;
#if HAVE_ATOMICS
	rbyt_copy = atomic_fetch_and(&rbyt,0);
	rcnt_copy = atomic_fetch_and(&rcnt,0);
	wbyt_copy = atomic_fetch_and(&wbyt,0);
	wcnt_copy = atomic_fetch_and(&wcnt,0);
	fcnt_copy = atomic_fetch_and(&fcnt,0);
#elif HAVE_SYNCS
	rbyt_copy = __sync_fetch_and_and(&rbyt,0);
	rcnt_copy = __sync_fetch_and_and(&rcnt,0);
	wbyt_copy = __sync_fetch_and_and(&wbyt,0);
	wcnt_copy = __sync_fetch_and_and(&wcnt,0);
	fcnt_copy = __sync_fetch_and_and(&fcnt,0);
#else
	pthread_mutex_lock(&opdatalock);
	rbyt_copy = rbyt;
	rcnt_copy = rcnt;
	wbyt_copy = wbyt;
	wcnt_copy = wcnt;
	fcnt_copy = fcnt;
	rbyt = 0;
	rcnt = 0;
	wbyt = 0;
	wcnt = 0;
	fcnt = 0;
	pthread_mutex_unlock(&opdatalock);
#endif
	sentbyt_copy = write_get_total_bytes();
	rcvdbyt_copy = read_get_total_bytes();
	if (masterversion>=VERSION2INT(4,57,0) && (rbyt_copy|wbyt_copy|sentbyt_copy|rcvdbyt_copy|rcnt_copy|wcnt_copy|fcnt_copy)!=0) {
		wptr = packetdata;
		put32bit(&wptr,CLTOMA_FUSE_OPDATA);
		put32bit(&wptr,44);
		put64bit(&wptr,rbyt_copy);
		put64bit(&wptr,wbyt_copy);
		put32bit(&wptr,rcnt_copy);
		put32bit(&wptr,wcnt_copy);
		put32bit(&wptr,fcnt_copy);
		put64bit(&wptr,rcvdbyt_copy);
		put64bit(&wptr,sentbyt_copy);
		senddata = 44+8;
	} else if (masterversion>=VERSION2INT(4,27,0) && (rbyt_copy|wbyt_copy|rcnt_copy|wcnt_copy|fcnt_copy)!=0) {
		wptr = packetdata;
		put32bit(&wptr,CLTOMA_FUSE_OPDATA);
		put32bit(&wptr,28);
		put64bit(&wptr,rbyt_copy);
		put64bit(&wptr,wbyt_copy);
		put32bit(&wptr,rcnt_copy);
		put32bit(&wptr,wcnt_copy);
		put32bit(&wptr,fcnt_copy);
		senddata = 28+8;
	}
	if (senddata) {
		if (tcptowrite(fd,packetdata,senddata,1000,send_timeout*1000)!=senddata) {
#ifdef HAVE___SYNC_FETCH_AND_OP
			(void)__sync_fetch_and_or(&disconnect,1);
#else
			disconnect = 1;
#endif
		} else {
			master_stats_add(MASTER_BYTESSENT,senddata);
			master_stats_inc(MASTER_PACKETSSENT);
		}
	}
}

void fs_set_working_flags(uint8_t sflags) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	(void)__sync_fetch_and_or(&working_flags,sflags);
#else
	working_flags |= sflags;
#endif
}

void fs_clr_working_flags(uint8_t cflags) {
#ifdef HAVE___SYNC_FETCH_AND_OP
	(void)__sync_fetch_and_and(&working_flags,~cflags);
#else
	working_flags &= ~cflags;
#endif
}

// send dynamic bits - things that can change after registration
void fs_send_working_flags(void) {
	uint8_t packetdata[9];
	uint8_t *wptr;

	if (masterversion>=VERSION2INT(4,40,0)) {
		wptr = packetdata;
		put32bit(&wptr,CLTOMA_FUSE_WFLAGS);
		put32bit(&wptr,1);
		put8bit(&wptr,working_flags);
		if (tcptowrite(fd,packetdata,8+1,1000,send_timeout*1000)!=(8+1)) {
#ifdef HAVE___SYNC_FETCH_AND_OP
			(void)__sync_fetch_and_or(&disconnect,1);
#else
			disconnect = 1;
#endif
		} else {
			master_stats_add(MASTER_BYTESSENT,8+1);
			master_stats_inc(MASTER_PACKETSSENT);
		}
	}
}

void* fs_nop_thread(void *arg) {
	uint8_t *ptr,hdr[12];
	uint64_t usec;
	int now;
	int inodeswritecnt=0;
	(void)arg;
	for (;;) {
		pthread_mutex_lock(&fdlock);
		if (fterm==2 && donotsendsustainedinodes==0) {
			if (fd>=0) {
				fs_send_opdata();
				fs_send_amtime_inodes();
				fs_send_open_inodes();
				fs_close_session();
				tcpclose(fd);
				fd = -1;
			}
			pthread_mutex_unlock(&fdlock);
			return NULL;
		}
#ifdef HAVE___SYNC_FETCH_AND_OP
		if (__sync_fetch_and_or(&disconnect,0)==0 && fd>=0) {
#else
		if (disconnect==0 && fd>=0) {
#endif
			now = monotonic_seconds();
			if (lastwrite+2.0<now) {	// NOP
				ptr = hdr;
				put32bit(&ptr,ANTOAN_NOP);
				put32bit(&ptr,4);
				put32bit(&ptr,0);
				if (tcptowrite(fd,hdr,12,1000,send_timeout*1000)!=12) {
#ifdef HAVE___SYNC_FETCH_AND_OP
					(void)__sync_fetch_and_or(&disconnect,1);
#else
					disconnect=1;
#endif
				} else {
					master_stats_add(MASTER_BYTESSENT,12);
					master_stats_inc(MASTER_PACKETSSENT);
				}
				lastwrite = now;
			}
			usec = monotonic_useconds();
			if (masterversion>=VERSION2INT(3,0,74) && (lastsyncsend==0 || lastsyncsend+60000000<usec)) { // time sync
				ptr = hdr;
				put32bit(&ptr,CLTOMA_FUSE_TIME_SYNC);
				put32bit(&ptr,4);
				put32bit(&ptr,0);
				if (tcptowrite(fd,hdr,12,1000,send_timeout*1000)!=12) {
#ifdef HAVE___SYNC_FETCH_AND_OP
					(void)__sync_fetch_and_or(&disconnect,1);
#else
					disconnect=1;
#endif
				} else {
					master_stats_add(MASTER_BYTESSENT,12);
					master_stats_inc(MASTER_PACKETSSENT);
				}
				lastsyncsend = usec;
			}
			if (inodeswritecnt<=0 || inodeswritecnt>60) {
				inodeswritecnt=60;
			} else {
				inodeswritecnt--;
			}
			if (inodeswritecnt==0) {	// HELD INODES
				if (donotsendsustainedinodes) {
					inodeswritecnt=1;
				} else {
					fs_send_open_inodes();
				}
			}
			fs_send_opdata();
			fs_send_amtime_inodes();
			fs_send_working_flags();
		}
		pthread_mutex_unlock(&fdlock);
		portable_sleep(1);
	}
}

void* fs_receive_thread(void *arg) {
	const uint8_t *ptr;
	uint8_t hdr[12];
	uint8_t msgbuff[37];
	uint8_t internal;
	threc *rec;
	uint32_t cmd,size,packetid;
	uint32_t rcvd,toread;
	uint32_t rechash;
//	static uint8_t *notify_buff=NULL;
//	static uint32_t notify_buff_size=0;
	int32_t r;

	(void)arg;
	for (;;) {
		pthread_mutex_lock(&fdlock);
		if (fterm) {
			fterm=2;
			pthread_mutex_unlock(&fdlock);
			return NULL;
		}
#ifdef HAVE___SYNC_FETCH_AND_OP
		if (__sync_fetch_and_and(&disconnect,0)) {
#else
		if (disconnect) {
			disconnect = 0;
#endif
//			dir_cache_remove_all();
			chunksdatacache_cleanup();
			tcpclose(fd);
			fd = -1;
			// send to any threc status error and unlock them
			pthread_mutex_lock(&reclock);
			for (rechash=0 ; rechash<THRECHASHSIZE ; rechash++) {
				for (rec=threchash[rechash] ; rec ; rec=rec->next) {
					pthread_mutex_lock(&(rec->mutex));
					if (rec->sent) {
						rec->status = 1;
						rec->rcvd = 1;
						pthread_cond_signal(&(rec->cond));
					}
					pthread_mutex_unlock(&(rec->mutex));
				}
			}
			pthread_mutex_unlock(&reclock);
		}
		if (fd==-1 && sessionid!=0) {
			fs_reconnect(connect_args.minversion);		// try to register using the same session id
		}
		if (fd==-1) {	// still not connected
			if (sessionlost || sessionid==0) {	// if previous session is lost then try to register as a new session
				if (fs_connect(0,&connect_args)==0) {
					sessionlost=0;
				}
			} else {	// if other problem occurred then try to resolve hostname and portname then try to reconnect using the same session id
				if (fs_resolve(0,connect_args.bindhostname,connect_args.masterhostname,connect_args.masterportname)==0) {
					fs_reconnect(connect_args.minversion);
				}
			}
		}
		if (fd==-1) {
			pthread_mutex_unlock(&fdlock);
			portable_sleep(2);	// reconnect every 2 seconds
			continue;
		}
		pthread_mutex_unlock(&fdlock);
		r = tcptoread(fd,hdr,12,sock_timeout*1000,recv_timeout*1000);
		// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: header size: %d",r);
		if (r==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: connection lost (header)");
			fs_disconnect();
			continue;
		}
		if (r!=12) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: tcp recv error: %s (header)",strerr(errno));
			fs_disconnect();
			continue;
		}
		master_stats_add(MASTER_BYTESRCVD,12);
		master_stats_inc(MASTER_PACKETSRCVD);

		ptr = hdr;
		cmd = get32bit(&ptr);
		size = get32bit(&ptr);
		packetid = get32bit(&ptr);
		if (size<4) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: packet too small");
			fs_disconnect();
			continue;
		}
//		printf("got packet from master: cmd:%"PRIu32" ; size:%"PRIu32" ; packetid:%"PRIu32"\n",cmd,size,packetid);
		size -= 4;
		if (packetid==0) {
			if (cmd==ANTOAN_NOP && size==0) {
				// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: got nop");
				continue;
			}
			if (cmd==ANTOAN_UNKNOWN_COMMAND || cmd==ANTOAN_BAD_COMMAND_SIZE) { // just ignore these packets with packetid==0
				continue;
			}
			internal = 0;
//			if (cmd==MATOCL_FUSE_INVALIDATE_DATA_CACHE) {
//				if (size==4) {
//					internal = 1;
//				} else {
//					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: unexpected msg size (msg:MATOCL_FUSE_INVALIDATE_DATA_CACHE ; size:%"PRIu32"/4)",size);
//					fs_disconnect();
//					continue;
//				}
//			}
			if (cmd==MATOCL_FUSE_CHUNK_HAS_CHANGED) {
				if (size==29 || size==37) {
					internal = 1;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: unexpected msg size (msg:MATOCL_FUSE_CHUNK_HAS_CHANGED ; size:%"PRIu32"/33|41)",size+4);
					fs_disconnect();
					continue;
				}
			}
			if (cmd==MATOCL_FUSE_FLENG_HAS_CHANGED) {
				if (size==12) {
					internal = 1;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: unexpected msg size (msg:MATOCL_FUSE_FLENG_HAS_CHANGED ; size:%"PRIu32"/16)",size+4);
					fs_disconnect();
					continue;
				}
			}
			if (cmd==MATOCL_FUSE_TIME_SYNC) {
				if (size==8) {
					internal = 1;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: unexpected msg size (msg:MATOCL_FUSE_TIME_SYNC ; size:%"PRIu32"/12)",size+4);
					fs_disconnect();
					continue;
				}
			}
			if (cmd==MATOCL_FUSE_INVALIDATE_CHUNK_CACHE) {
				if (size==0) {
					internal = 1;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: unexpected msg size (msg:MATOCL_FUSE_INVALIDATE_CHUNK_CACHE ; size:%"PRIu32"/4)",size+4);
					fs_disconnect();
					continue;
				}
			}
			if (cmd==ANTOAN_FORCE_TIMEOUT) {
				if (size==2) {
					internal = 1;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: unexpected msg size (msg:ANTOAN_FORCE_TIMEOUT ; size:%"PRIu32"/6)",size+4);
					fs_disconnect();
					continue;
				}
			}
			if (internal) {
				if (size>0) {
					r = tcptoread(fd,msgbuff,size,sock_timeout*1000,recv_timeout*1000);
					if (r==0) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: connection lost (data/internal ; cmd:%"PRIu32" ; size:%"PRIu32")",cmd,size);
						fs_disconnect();
						continue;
					}
					if (r!=(int32_t)size) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: tcp recv error: %s (data/internal ; cmd:%"PRIu32" ; size:%"PRIu32")",strerr(errno),cmd,size);
						fs_disconnect();
						continue;
					}
					master_stats_add(MASTER_BYTESRCVD,size);
				}
				ptr = msgbuff;
//				if (cmd==MATOCL_FUSE_INVALIDATE_DATA_CACHE) {
//#ifndef WIN32
//					uint32_t inode;
//					inode = get32bit(&ptr);
//					mfs_invalidate_data_cache(inode);
//#endif
//					continue;
//				}
				if (cmd==MATOCL_FUSE_FLENG_HAS_CHANGED) {
					uint32_t inode;
					uint64_t fleng;
					inode = get32bit(&ptr);
					fleng = get64bit(&ptr);
//					fprintf(stderr,"FLENG_HAS_CHANGED inode:%"PRIu32" ; fleng:%"PRIu64"\n",inode,fleng);
					ep_fleng_has_changed(inode,fleng);
					continue;
				}
				if (cmd==MATOCL_FUSE_CHUNK_HAS_CHANGED) {
					uint32_t inode;
					uint32_t chindx;
					uint64_t chunkid;
					uint32_t version;
					uint64_t fleng;
					uint8_t truncflag;
					uint32_t choffset;
					uint32_t chsize;
					inode = get32bit(&ptr);
					chindx = get32bit(&ptr);
					chunkid = get64bit(&ptr);
					version = get32bit(&ptr);
					fleng = get64bit(&ptr);
					truncflag = get8bit(&ptr);
					if (size==37) {
						choffset = get32bit(&ptr);
						chsize = get32bit(&ptr);
					} else {
						choffset = 0;
						chsize = MFSCHUNKSIZE;
					}
//					fprintf(stderr,"CHUNK_HAS_CHANGED inode:%"PRIu32" ; chindx:%"PRIu32" ; chunkid:%"PRIu64" ; version:%"PRIu32" ; fleng:%"PRIu64" ; truncate:%"PRIu8" ; offset:%"PRIu32" ; size:%"PRIu32"\n",inode,chindx,chunkid,version,fleng,truncflag,choffset,chsize);
					ep_chunk_has_changed(inode,chindx,chunkid,version,fleng,truncflag,choffset,chsize);
					continue;
				}
				if (cmd==MATOCL_FUSE_TIME_SYNC) {
					uint64_t lusectime,rusectime,usec,usecping;
					struct timeval tv;
					usec = monotonic_useconds();
					pthread_mutex_lock(&fdlock);
					if (usec>=lastsyncsend) {
						usecping = usec - lastsyncsend;
						pthread_mutex_unlock(&fdlock);
						master_stats_set(MASTER_PING,usecping);
					} else {
						pthread_mutex_unlock(&fdlock);
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"negative packet travel time between client and master - ignoring in time sync");
						usecping = 0;
					}
					if (usecping>100000) { // ignore too high differences
						mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"high packet travel time between client and master (%u.%06us) - ignoring in time sync",(unsigned int)(usecping/1000000),(unsigned int)(usecping%1000000));
						usecping = 0;
					}
					rusectime = get64bit(&ptr);
					rusectime += usecping/2;
					// usectime here should have master's wall clock
					fs_amtime_reference_clock(usec,rusectime);
					gettimeofday(&tv,NULL);
					lusectime = tv.tv_sec;
					lusectime *= 1000000;
					lusectime += tv.tv_usec;
					if (rusectime + 1000000 < lusectime || lusectime + 1000000 < rusectime) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"time desync between client and master is higher than a second - it might lead to strange atime/mtime behaviour - consider time synchronization in your moosefs cluster");
					}
					if (rusectime > lusectime) {
						master_stats_set(MASTER_TIMEDIFF,rusectime-lusectime);
					} else {
						master_stats_set(MASTER_TIMEDIFF,lusectime-rusectime);
					}
#ifdef MFSDEBUG
					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"ping time: %u.%06u ; remote time: %u.%06u ; local time: %u.%06u ; monotonic time: %u.%06u",(unsigned int)(usecping/1000000),(unsigned int)(usecping%1000000),(unsigned int)(rusectime/1000000),(unsigned int)(rusectime%1000000),(unsigned int)(lusectime/1000000),(unsigned int)(lusectime%1000000),(unsigned int)(usec/1000000),(unsigned int)(usec%1000000));
#endif
					continue;
				}
				if (cmd==MATOCL_FUSE_INVALIDATE_CHUNK_CACHE) {
					chunksdatacache_cleanup();
					continue;
				}
				if (cmd==ANTOAN_FORCE_TIMEOUT) {
					sock_timeout = get16bit(&ptr);
					if (sock_timeout < 10) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"forced timeout too small (<10s) - set to 10s");
						sock_timeout = 10;
					}
					if (sock_timeout<100) {
						recv_timeout = 300;
					} else {
						recv_timeout = sock_timeout * 3;
					}
					continue;
				}
			}
		}
		rec = fs_get_threc_by_id(packetid);
		if (rec==NULL) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: got unexpected queryid (%"PRIu32" ; cmd:%"PRIu32" ; size:%"PRIu32")",packetid,cmd,size+4);
			fs_disconnect();
			continue;
		}
		pthread_mutex_lock(&(rec->mutex));	// make helgrind happy
		if (rec->receiving) {
			pthread_mutex_unlock(&(rec->mutex));
			fs_disconnect();
			continue;
		}
		fs_input_buffer_init(rec,size);
		if (rec->ibuff==NULL) {
			pthread_mutex_unlock(&(rec->mutex));
			fs_disconnect();
			continue;
		}
		rec->receiving = 1;
		pthread_mutex_unlock(&(rec->mutex));
		// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: expected data size: %"PRIu32,size);
		rcvd = 0;
		while (size-rcvd>0) {
			toread = size-rcvd;
			if (toread>65536) {
				toread = 65536;
			}
			r = tcptoread(fd,rec->ibuff+rcvd,toread,sock_timeout*1000,recv_timeout*1000);
			// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: data size: %d",r);
			if (r==0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: connection lost (data ; cmd:%"PRIu32" ; size:%"PRIu32")",cmd,size);
				break;
			}
			if (r!=(int32_t)toread) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"master: tcp recv error: %s (data ; cmd:%"PRIu32" ; size:%"PRIu32")",strerr(errno),cmd,size);
				break;
			}
			master_stats_add(MASTER_BYTESRCVD,toread);
			rcvd += toread;
		}
		if (size-rcvd>0) { // exit from previous loop by break = error
			pthread_mutex_lock(&(rec->mutex));
			rec->receiving = 0;
			pthread_mutex_unlock(&(rec->mutex));
			fs_disconnect();
			continue;
		}
		pthread_mutex_lock(&(rec->mutex));
		rec->sent = 0;
		rec->status = 0;
		rec->idataleng = size;
		rec->rcvd_cmd = cmd;
		rec->receiving = 0;
		// mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"master: unlock: %"PRIu32,rec->packetid);
		rec->rcvd = 1;
//		if (rec->waiting) {
		pthread_cond_signal(&(rec->cond));
//		}
		pthread_mutex_unlock(&(rec->mutex));
	}
}

// called before fork
int fs_init_master_connection(const char *bindhostname,const char *masterhostname,const char *masterportname,uint8_t meta,const char *info,const char *subfolder,const uint8_t passworddigest[16],uint8_t donotrememberpassword,uint8_t bgregister,uint32_t minversion) {
	master_statsptr_init();

	fd = -1;
	sessionlost = bgregister?1:0;
	lastsyncsend = 0;
	sessionid = 0;
	metaid = 0;
	masterversion = 0;
	masterprocessid = 0;
#ifdef HAVE___SYNC_FETCH_AND_OP
	(void)__sync_fetch_and_and(&disconnect,0);
#else
	disconnect = 0;
#endif
	donotsendsustainedinodes = 0;

	if (bindhostname) {
		connect_args.bindhostname = strdup(bindhostname);
	} else {
		connect_args.bindhostname = NULL;
	}
	connect_args.masterhostname = strdup(masterhostname);
	connect_args.masterportname = strdup(masterportname);
	connect_args.meta = meta;
	connect_args.clearpassword = donotrememberpassword;
	connect_args.info = strdup(info);
	connect_args.subfolder = strdup(subfolder);
	if (passworddigest==NULL) {
		connect_args.passworddigest = NULL;
	} else {
		connect_args.passworddigest = malloc(16);
		memcpy(connect_args.passworddigest,passworddigest,16);
	}
	connect_args.minversion = minversion;

	if (bgregister) {
		return 1;
	}
	return fs_connect(1,&connect_args);
}

// called after fork
void fs_init_threads(uint32_t retries,uint32_t timeout) {
	uint32_t i;
	pthread_attr_t thattr;
	struct timeval tv;
	uint64_t usectime;
	maxretries = retries;
	usectimeout = timeout;
	usectimeout *= 1000000; // sec -> usec
	fterm = 0;
	ep_init();
	for (i=0 ; i<AMTIME_HASH_SIZE ; i++) {
		amtime_hash[i] = NULL;
	}
	for (i=0 ; i<ACQFILES_HASH_SIZE ; i++) {
		af_hash[i] = NULL;
	}
	af_lruhead = NULL;
	af_lrutail = &(af_lruhead);
	af_lru_cnt = 0;
	gettimeofday(&tv,NULL);
	usectime = tv.tv_sec;
	usectime *= 1000000;
	usectime += tv.tv_usec;
	timediffusec = usectime - monotonic_useconds(); // before receiving packets from master start with own wall clock
	zassert(pthread_key_create(&reckey,fs_free_threc));
	zassert(pthread_mutex_init(&reclock,NULL));
	zassert(pthread_mutex_init(&fdlock,NULL));
	zassert(pthread_mutex_init(&aflock,NULL));
	zassert(pthread_mutex_init(&amtimelock,NULL));
#ifdef OPDATA_USE_LOCK
	zassert(pthread_mutex_init(&opdatalock,NULL));
#endif
	zassert(pthread_attr_init(&thattr));
	zassert(pthread_attr_setstacksize(&thattr,0x100000));
	zassert(pthread_create(&rpthid,&thattr,fs_receive_thread,NULL));
	zassert(pthread_create(&npthid,&thattr,fs_nop_thread,NULL));
	zassert(pthread_attr_destroy(&thattr));
	mainrec = fs_get_my_threc();
	fs_init_counters();
}

void fs_term(void) {
	threc *rec,*recn;
	uint32_t i;
	uint32_t rechash;
	amtime_file *amf,*amfn;
	acquired_file *af,*afn;

	zassert(pthread_mutex_lock(&fdlock));
	fterm = 1;
	zassert(pthread_mutex_unlock(&fdlock));
	zassert(pthread_join(npthid,NULL));
	zassert(pthread_join(rpthid,NULL));
#ifdef OPDATA_USE_LOCK
	zassert(pthread_mutex_destroy(&opdatalock));
#endif
	zassert(pthread_mutex_destroy(&amtimelock));
	zassert(pthread_mutex_destroy(&aflock));
	zassert(pthread_mutex_destroy(&fdlock));
	fs_free_threc(mainrec);
	zassert(pthread_mutex_lock(&reclock));
	for (rechash=0 ; rechash<THRECHASHSIZE ; rechash++) {
		for (rec = threchash[rechash] ; rec!=NULL ; rec=recn) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"thread specific memory (id:%"PRIu32") hasn't been freed",rec->packetid);
			recn = rec->next;
			if (rec->obuff) {
				free(rec->obuff);
			}
			if (rec->ibuff) {
				free(rec->ibuff);
			}
			pthread_mutex_destroy(&(rec->mutex));
			pthread_cond_destroy(&(rec->cond));
			free(rec);
		}
	}
	for (rec = threcfree ; rec ; rec = recn) {
		recn = rec->next;
		if (rec->obuff) {
			free(rec->obuff);
		}
		if (rec->ibuff) {
			free(rec->ibuff);
		}
		pthread_mutex_destroy(&(rec->mutex));
		pthread_cond_destroy(&(rec->cond));
		free(rec);
	}
	zassert(pthread_mutex_unlock(&reclock));
	zassert(pthread_mutex_destroy(&reclock));
	zassert(pthread_key_delete(reckey));
	for (i=0 ; i<ACQFILES_HASH_SIZE ; i++) {
		for (af = af_hash[i] ; af ; af = afn) {
			afn = af->next;
			free(af);
		}
	}
	for (i=0 ; i<AMTIME_HASH_SIZE ; i++) {
		for (amf = amtime_hash[i] ; amf ; amf = amfn) {
			amfn = amf->next;
			free(amf);
		}
	}
	if (fd>=0) {
		tcpclose(fd);
	}
	if (connect_args.bindhostname) {
		free(connect_args.bindhostname);
	}
	free(connect_args.masterhostname);
	free(connect_args.masterportname);
	free(connect_args.info);
	free(connect_args.subfolder);
	if (connect_args.passworddigest) {
		free(connect_args.passworddigest);
	}
	heap_term();
	ep_term();
}

uint8_t fs_get_cfg(const char *opt_name,uint8_t *oleng,const uint8_t **odata) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t nleng;
	threc *rec = fs_get_my_threc();

	nleng = strlen(opt_name);
	if (nleng>255) {
		return MFS_ERROR_EINVAL;
	}
	wptr = fs_createpacket(rec,ANTOAN_GET_CONFIG,1+nleng);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put8bit(&wptr,nleng);
	memcpy(wptr,opt_name,nleng);
	rptr = fs_sendandreceive(rec,ANTOAN_CONFIG_VALUE,&i);
	if (rptr==NULL) {
		return MFS_ERROR_IO;
	} else if (i==0 || i>255) {
		fs_disconnect();
		return MFS_ERROR_IO;
	}
	nleng = get8bit(&rptr);
	if (i!=(1U+nleng)) {
		fs_disconnect();
		return MFS_ERROR_IO;
	}
	*oleng = nleng;
	*odata = rptr;
	return MFS_STATUS_OK;
}

uint8_t fs_get_cfg_file(const char *opt_name,uint16_t *oleng,const uint8_t **odata) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t nleng;
	uint16_t cleng;
	threc *rec = fs_get_my_threc();

	if (master_version()<VERSION2INT(4,42,0)) {
		return MFS_ERROR_ENOTSUP;
	}
	nleng = strlen(opt_name);
	if (nleng>255) {
		return MFS_ERROR_EINVAL;
	}
	wptr = fs_createpacket(rec,ANTOAN_GET_CONFIG_FILE,1+nleng);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put8bit(&wptr,nleng);
	memcpy(wptr,opt_name,nleng);
	rptr = fs_sendandreceive(rec,ANTOAN_CONFIG_FILE_CONTENT,&i);
	if (rptr==NULL) {
		return MFS_ERROR_IO;
	} else if (i==1) {
		return rptr[0];
	} else if (i==0) {
		fs_disconnect();
		return MFS_ERROR_IO;
	}
	cleng = get16bit(&rptr);
	if (i!=(2U+cleng)) {
		fs_disconnect();
		return MFS_ERROR_IO;
	}
	*oleng = cleng;
	*odata = rptr;
	return MFS_STATUS_OK;
}

void fs_statfs(uint64_t *totalspace,uint64_t *availspace,uint64_t *freespace,uint64_t *trashspace,uint64_t *sustainedspace,uint32_t *inodes) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_STATFS,0);
	if (wptr==NULL) {
		*totalspace = 0;
		*availspace = 0;
		*freespace = 0;
		*trashspace = 0;
		*sustainedspace = 0;
		*inodes = 0;
		return;
	}
	rptr = fs_sendandreceive_ha_root(rec,MATOCL_FUSE_STATFS,&i);
	if (rptr==NULL || (i!=36 && i!=44)) {
		*totalspace = 0;
		*availspace = 0;
		*freespace = 0;
		*trashspace = 0;
		*sustainedspace = 0;
		*inodes = 0;
	} else {
		*totalspace = get64bit(&rptr);
		*availspace = get64bit(&rptr);
		if (i==44) {
			*freespace = get64bit(&rptr);
		} else {
			*freespace = *availspace;
		}
		*trashspace = get64bit(&rptr);
		*sustainedspace = get64bit(&rptr);
		*inodes = get32bit(&rptr);
	}
}

uint8_t fs_access(uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint16_t modemask) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (master_version()<VERSION2INT(2,0,0) || gids==0) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_ACCESS,13);
		if (wptr==NULL) {
			return MFS_ERROR_IO;
		}
		put32bit(&wptr,inode);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
		put8bit(&wptr,modemask);
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_ACCESS,14+4*gids);
		if (wptr==NULL) {
			return MFS_ERROR_IO;
		}
		put32bit(&wptr,inode);
		put32bit(&wptr,uid);
		put32bit(&wptr,gids);
		for (i=0 ; i<gids ; i++) {
			put32bit(&wptr,gid[i]);
		}
		put16bit(&wptr,modemask);
	}
	rptr = fs_sendandreceive_ha(rec,inode,MATOCL_FUSE_ACCESS,&i);
	if (!rptr || i!=1) {
		ret = MFS_ERROR_IO;
	} else {
		ret = rptr[0];
	}
	return ret;
}

uint8_t fs_path_lookup(uint32_t base_inode,uint32_t pleng,const uint8_t *path,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *parent_inode,uint32_t *last_inode,uint8_t *nleng,uint8_t name[256],uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t nlaux;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(4,14,0)) {
		return MFS_ERROR_ENOTSUP;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_PATH_LOOKUP,16+4*gids+pleng);
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,base_inode);
	put32bit(&wptr,pleng);
	memcpy(wptr,path,pleng);
	wptr+=pleng;
	put32bit(&wptr,uid);
	if (gids>0) {
		put32bit(&wptr,gids);
		for (i=0 ; i<gids ; i++) {
			put32bit(&wptr,gid[i]);
		}
	} else {
		put32bit(&wptr,0xFFFFFFFF);
	}
	rptr = fs_sendandreceive(rec,MATOCL_PATH_LOOKUP,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		if (i<(uint32_t)(9U+asize)) {
			fs_disconnect();
			ret = MFS_ERROR_IO;
		} else {
			*parent_inode = get32bit(&rptr);
			nlaux = get8bit(&rptr);
			if (i!=(uint32_t)(9U+nlaux+asize)) {
				fs_disconnect();
				ret = MFS_ERROR_IO;
			} else {
				if (nlaux>0) {
					memcpy(name,rptr,nlaux);
					rptr+=nlaux;
				}
				name[nlaux]='\0';
				*nleng = nlaux;
				*last_inode = get32bit(&rptr);
				copy_attr(rptr,attr,asize);
				ret = MFS_STATUS_OK;
			}
		}
	}
	return ret;
}

uint8_t fs_simple_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_LOOKUP,13+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_LOOKUP,13+4*gids+nleng);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive_ha(rec,parent,MATOCL_FUSE_LOOKUP,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i==(uint32_t)(4+asize) || i>=(uint32_t)(6+asize)) {
		*inode = get32bit(&rptr);
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE],uint16_t *lflags,uint8_t *csdataver,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_LOOKUP,13+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_LOOKUP,13+4*gids+nleng);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive_ha(rec,parent,MATOCL_FUSE_LOOKUP,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i==(uint32_t)(4+asize)) {
		*inode = get32bit(&rptr);
		copy_attr(rptr,attr,asize);
		*lflags = 0xFFFF;
		ret = MFS_STATUS_OK;
	} else if (i>=(uint32_t)(6+asize)) {
		*inode = get32bit(&rptr);
		copy_attr(rptr,attr,asize);
		rptr+=asize;
		*lflags = get16bit(&rptr);
		ret = MFS_STATUS_OK;
		if ((*lflags) & LOOKUP_CHUNK_ZERO_DATA) {
			if (i>=(uint32_t)(19+asize)) {
				*csdataver = get8bit(&rptr);
				*chunkid = get64bit(&rptr);
				*version = get32bit(&rptr);
				*csdata = rptr;
				*csdatasize = i-(19+asize);
				if (((*csdataver)!=2 && (*csdataver)!=3)) {
					ret = MFS_ERROR_IO;
				} else if ((*csdataver)==2 && ((i-(19+asize))%14)!=0) {
					ret = MFS_ERROR_IO;
				} else if ((*csdataver)==3 && i!=(uint32_t)(19+asize)+8*14 && i!=(uint32_t)(19+asize)+4*14) {
					ret = MFS_ERROR_IO;
				}
			} else {
				ret = MFS_ERROR_IO;
			}
		} else {
			*csdataver = 0;
			*chunkid = 0;
			*version = 0;
			*csdata = NULL;
			*csdatasize = 0;
		}
		if (ret == MFS_ERROR_IO) {
			fs_disconnect();
		}
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_getattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(1,6,28)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETATTR,12);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETATTR,13);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver>=1) {
		put8bit(&wptr,opened);
	}
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive_ha(rec,inode,MATOCL_FUSE_GETATTR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=asize) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	}
	return ret;
}

uint8_t fs_setattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t winattr,uint8_t sugidclearmode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();
	uint32_t mv = master_version();

	if (mv<VERSION2INT(1,6,25)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,31);
		packetver = 0;
	} else if (mv<VERSION2INT(1,6,28)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,32);
		packetver = 1;
	} else if (mv<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,33);
		packetver = 2;
	} else if (mv<VERSION2INT(3,0,93)||mv==VERSION2INT(4,0,0)||mv==VERSION2INT(4,0,1)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,33+gids*4);
		packetver = 3;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,34+gids*4);
		packetver = 4;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver>=2) {
		put8bit(&wptr,opened);
	}
	put32bit(&wptr,uid);
	if (packetver<=2) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	put8bit(&wptr,setmask);
	put16bit(&wptr,attrmode);
	put32bit(&wptr,attruid);
	put32bit(&wptr,attrgid);
	put32bit(&wptr,attratime);
	put32bit(&wptr,attrmtime);
	if (packetver>=4) {
		put8bit(&wptr,winattr);
	}
	if (packetver>=1) {
		put8bit(&wptr,sugidclearmode);
	}
	rptr = fs_sendandreceive_ha(rec,inode,MATOCL_FUSE_SETATTR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=asize) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	}
	return ret;
}

uint8_t fs_truncate(uint32_t inode,uint8_t flags,uint32_t uid,uint32_t gids,uint32_t *gid,uint64_t attrlength,uint8_t attr[ATTR_RECORD_SIZE],uint64_t *prevlength) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_TRUNCATE,21);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_TRUNCATE,21+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put8bit(&wptr,flags);
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	put64bit(&wptr,attrlength);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_TRUNCATE,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i==asize) {
		if (attr!=NULL) {
			copy_attr(rptr,attr,asize);
		}
		ret = MFS_STATUS_OK;
	} else if (i==(uint32_t)(asize+8)) {
		if (prevlength!=NULL) {
			*prevlength = get64bit(&rptr);
		} else {
			rptr+=8;
		}
		if (attr!=NULL) {
			copy_attr(rptr,attr,asize);
		}
		ret = MFS_STATUS_OK;
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_readlink(uint32_t inode,const uint8_t **path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_READLINK,4);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_READLINK,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		pleng = get32bit(&rptr);
		if (i!=4+pleng || pleng==0 || rptr[pleng-1]!=0) {
			fs_disconnect();
			ret = MFS_ERROR_IO;
		} else {
			*path = rptr;
			//*path = malloc(pleng);
			//memcpy(*path,ptr,pleng);
			ret = MFS_STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_symlink(uint32_t parent,uint8_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	pleng = strlen((const char *)path)+1;
	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SYMLINK,pleng+nleng+17);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SYMLINK,pleng+nleng+17+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,pleng);
	memcpy(wptr,path,pleng);
	wptr+=pleng;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SYMLINK,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=(uint32_t)(4+asize)) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	}
	return ret;
}

static inline uint8_t fsnodes_type_back_convert(uint8_t type) {
	switch (type) {
		case TYPE_FILE:
			return DISP_TYPE_FILE;
		case TYPE_DIRECTORY:
			return DISP_TYPE_DIRECTORY;
		case TYPE_SYMLINK:
			return DISP_TYPE_SYMLINK;
		case TYPE_FIFO:
			return DISP_TYPE_FIFO;
		case TYPE_BLOCKDEV:
			return DISP_TYPE_BLOCKDEV;
		case TYPE_CHARDEV:
			return DISP_TYPE_CHARDEV;
		case TYPE_SOCKET:
			return DISP_TYPE_SOCKET;
		case TYPE_TRASH:
			return DISP_TYPE_TRASH;
		case TYPE_SUSTAINED:
			return DISP_TYPE_SUSTAINED;
	}
	return type;
}

uint8_t fs_mknod(uint32_t parent,uint8_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t rdev,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(2,0,0)) {
		mode &= ~cumask;
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKNOD,20+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKNOD,22+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	if (master_version()<VERSION2INT(1,7,32)) {
		type = fsnodes_type_back_convert(type);
	}
	put8bit(&wptr,type);
	put16bit(&wptr,mode);
	if (packetver>=1) {
		put16bit(&wptr,cumask);
	}
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	put32bit(&wptr,rdev);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_MKNOD,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=(uint32_t)(4+asize)) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	}
	return ret;
}

uint8_t fs_mkdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t copysgid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(1,6,25)) {
		mode &= ~cumask;
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKDIR,15+nleng);
		packetver = 0;
	} else if (master_version()<VERSION2INT(2,0,0)) {
		mode &= ~cumask;
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKDIR,16+nleng);
		packetver = 1;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKDIR,18+nleng+4*gids);
		packetver = 2;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put16bit(&wptr,mode);
	if (packetver>=2) {
		put16bit(&wptr,cumask);
	}
	put32bit(&wptr,uid);
	if (packetver<2) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	if (packetver>=1) {
		put8bit(&wptr,copysgid);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_MKDIR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=(uint32_t)(4+asize)) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	}
	return ret;
}

uint8_t fs_unlink(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_UNLINK,13+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_UNLINK,13+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_UNLINK,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
		*inode = 0;
	} else if (i==4) {
		ret = MFS_STATUS_OK;
		*inode = get32bit(&rptr);
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_rmdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_RMDIR,13+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_RMDIR,13+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_RMDIR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
		*inode = 0;
	} else if (i==4) {
		ret = MFS_STATUS_OK;
		*inode = get32bit(&rptr);
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_rename(uint32_t parent_src,uint8_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t mfsflags,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_RENAME,18+nleng_src+nleng_dst);
		packetver = 0;
	} else if (master_version()<VERSION2INT(4,18,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_RENAME,18+nleng_src+nleng_dst+4*gids);
		packetver = 1;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_RENAME,19+nleng_src+nleng_dst+4*gids);
		packetver = 2;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,parent_src);
	put8bit(&wptr,nleng_src);
	memcpy(wptr,name_src,nleng_src);
	wptr+=nleng_src;
	put32bit(&wptr,parent_dst);
	put8bit(&wptr,nleng_dst);
	memcpy(wptr,name_dst,nleng_dst);
	wptr+=nleng_dst;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
		if (packetver>1) {
			put8bit(&wptr,mfsflags);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_RENAME,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
		*inode = 0;
		memset(attr,0,ATTR_RECORD_SIZE);
	} else if (i!=(uint32_t)(4+asize)) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	}
	return ret;
}

uint8_t fs_link(uint32_t inode_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_LINK,17+nleng_dst);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_LINK,17+nleng_dst+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode_src);
	put32bit(&wptr,parent_dst);
	put8bit(&wptr,nleng_dst);
	memcpy(wptr,name_dst,nleng_dst);
	wptr+=nleng_dst;
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_LINK,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=(uint32_t)(4+asize)) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		*inode = get32bit(&rptr);
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	}
	return ret;
}
/*
uint8_t fs_getdir(uint32_t inode,uint32_t uid,uint32_t gid,const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_READDIR,12);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_READDIR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = MFS_STATUS_OK;
	}
	return ret;
}
*/

uint8_t fs_readdir(uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint64_t *edgeid,uint8_t wantattr,uint8_t addtocache,const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	uint8_t flags;
	threc *rec = fs_get_my_threc();
	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_READDIR,13);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_READDIR,25+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	flags = 0;
	if (wantattr) {
		flags |= GETDIR_FLAG_WITHATTR;
	}
	if (addtocache) {
		flags |= GETDIR_FLAG_ADDTOCACHE;
	}
	put8bit(&wptr,flags);
	if (packetver>=1) {
		put32bit(&wptr,0xFFFFFFFFU);
		if (edgeid!=NULL) {
			put64bit(&wptr,*edgeid);
		} else {
			put64bit(&wptr,0);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_READDIR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		if (packetver>=1) {
			if (edgeid!=NULL) {
				*edgeid = get64bit(&rptr);
			} else {
				rptr+=8;
			}
			i-=8;
		}
		*dbuff = rptr;
		*dbuffsize = i;
		ret = MFS_STATUS_OK;
	}
	return ret;
}

uint8_t fs_create(uint32_t parent,uint8_t nleng,const uint8_t *name,uint16_t mode,uint16_t cumask,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t *inode,uint8_t attr[ATTR_RECORD_SIZE],uint8_t *oflags) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(1,7,25)) {
		return MFS_ERROR_ENOTSUP;
	}
	if (master_version()<VERSION2INT(2,0,0)) {
		mode &= ~cumask;
		wptr = fs_createpacket(rec,CLTOMA_FUSE_CREATE,15+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_CREATE,17+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put16bit(&wptr,mode);
	if (packetver>=1) {
		put16bit(&wptr,cumask);
	}
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	}
	pthread_mutex_lock(&fdlock);
	donotsendsustainedinodes = 1;
	pthread_mutex_unlock(&fdlock);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_CREATE,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i==(uint32_t)(4+asize)) {
		*oflags = 0xFF;
		*inode = get32bit(&rptr);
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	} else if (i==(uint32_t)(5+asize)) {
		*oflags = get8bit(&rptr);
		*inode = get32bit(&rptr);
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	pthread_mutex_lock(&fdlock);
	donotsendsustainedinodes = 0;
	pthread_mutex_unlock(&fdlock);
	return ret;
}

uint8_t fs_opencheck(uint32_t inode,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t flags,uint8_t attr[ATTR_RECORD_SIZE],uint8_t *oflags) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	if (master_version()<VERSION2INT(2,0,0) || gids==0) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_OPEN,13);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_OPEN,13+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	if (packetver==0) {
		if (gids>0) { // should be always true) 
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFF);
		}
	} else {
		put32bit(&wptr,gids);
		for (i=0 ; i<gids ; i++) {
			put32bit(&wptr,gid[i]);
		}
	}
	put8bit(&wptr,flags);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_OPEN,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		if (attr) {
			memset(attr,0,ATTR_RECORD_SIZE);
		}
		if (oflags) {
			*oflags = 0xFF;
		}
		ret = rptr[0];
	} else if (i==asize) {
		if (attr) {
			copy_attr(rptr,attr,asize);
		}
		if (oflags) {
			*oflags = 0xFF;
		}
		ret = MFS_STATUS_OK;
	} else if (i==(uint32_t)(1+asize)) {
		if (oflags) {
			*oflags = rptr[0];
		}
		rptr++;
		if (attr) {
			copy_attr(rptr,attr,asize);
		}
		ret = MFS_STATUS_OK;
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint8_t chunkopflags,uint8_t *csdataver,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();

	*csdata = NULL;
	*csdatasize = 0;

	if (master_version()>=VERSION2INT(3,0,4)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_READ_CHUNK,9);
		packetver = 1;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_READ_CHUNK,8);
		packetver = 0;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,indx);
	if (packetver>=1) {
		put8bit(&wptr,chunkopflags);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_READ_CHUNK,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		if (i&1) {
			*csdataver = get8bit(&rptr);
			if (i<21 || ((*csdataver)==1 && ((i-21)%10)!=0) || ((*csdataver)==2 && ((i-21)%14)!=0) || ((*csdataver)==3 && i!=21+14*8 && i!=21+14*4)) {
				ret = MFS_ERROR_IO;
			} else {
				*csdatasize = i-21;
				ret = MFS_STATUS_OK;
			}
		} else {
			*csdataver = 0;
			if (i<20 || ((i-20)%6)!=0) {
				ret = MFS_ERROR_IO;
			} else {
				*csdatasize = i-20;
				ret = MFS_STATUS_OK;
			}
		}
		if (ret!=MFS_STATUS_OK) {
			fs_disconnect();
		} else {
			*length = get64bit(&rptr);
			*chunkid = get64bit(&rptr);
			*version = get32bit(&rptr);
			*csdata = rptr;
		}
	}
	return ret;
}

uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint8_t chunkopflags,uint8_t *csdataver,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();

	*csdata = NULL;
	*csdatasize = 0;

	if (master_version()>=VERSION2INT(3,0,4)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_WRITE_CHUNK,9);
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_WRITE_CHUNK,8);
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,indx);
	if (master_version()>=VERSION2INT(3,0,4)) {
		put8bit(&wptr,chunkopflags);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_WRITE_CHUNK,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		if (i&1) {
			*csdataver = get8bit(&rptr);
			if (i<21 || ((*csdataver)==1 && ((i-21)%10)!=0) || ((*csdataver)==2 && ((i-21)%14)!=0)) {
				ret = MFS_ERROR_IO;
			} else {
				*csdatasize = i-21;
				ret = MFS_STATUS_OK;
			}
		} else {
			*csdataver = 0;
			if (i<20 || ((i-20)%6)!=0) {
				ret = MFS_ERROR_IO;
			} else {
				*csdatasize = i-20;
				ret = MFS_STATUS_OK;
			}
		}
		if (ret!=MFS_STATUS_OK) {
			fs_disconnect();
		} else {
			*length = get64bit(&rptr);
			*chunkid = get64bit(&rptr);
			*version = get32bit(&rptr);
			*csdata = rptr;
		}
	}
	return ret;
}

uint8_t fs_writeend(uint64_t chunkid,uint32_t inode,uint32_t indx,uint64_t length,uint8_t chunkopflags,uint32_t offset,uint32_t size) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (master_version()>=VERSION2INT(4,40,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_WRITE_CHUNK_END,33);
	} else if (master_version()>=VERSION2INT(3,0,74)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_WRITE_CHUNK_END,25);
	} else if (master_version()>=VERSION2INT(3,0,4)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_WRITE_CHUNK_END,21);
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_WRITE_CHUNK_END,20);
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put64bit(&wptr,chunkid);
	put32bit(&wptr,inode);
	if (master_version()>=VERSION2INT(3,0,74)) {
		put32bit(&wptr,indx);
	}
	put64bit(&wptr,length);
	if (master_version()>=VERSION2INT(3,0,4)) {
		put8bit(&wptr,chunkopflags);
	}
	if (master_version()>=VERSION2INT(4,40,0)) {
		put32bit(&wptr,offset);
		put32bit(&wptr,size);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_WRITE_CHUNK_END,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}
/*
uint8_t fs_fsync_send(uint32_t inode) {
	uint8_t *wptr;
	threc *rec;
	if (master_version()>=VERSION2INT(3,0,74)) {
		rec = fs_get_my_threc();
		wptr = fs_createpacket(rec,CLTOMA_FUSE_FSYNC,4);
		if (wptr==NULL) {
			return MFS_ERROR_IO;
		}
		put32bit(&wptr,inode);
		if (fs_send_only(rec)) {
			return MFS_STATUS_OK;
		} else {
			return MFS_ERROR_IO;
		}
	} else {
		return MFS_STATUS_OK;
	}
}

uint8_t fs_fsync_wait(void) {
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec;
	if (master_version()>=VERSION2INT(3,0,74)) {
		rec = fs_get_my_threc();
		rptr = fs_receive_only(rec,MATOCL_FUSE_FSYNC,&i);
		if (rptr==NULL) {
			ret = MFS_ERROR_IO;
		} else if (i==1) {
			ret = rptr[0];
		} else {
			fs_disconnect();
			ret = MFS_ERROR_IO;
		}
	} else {
		ret = MFS_STATUS_OK;
	}
	return ret;
}
*/
uint8_t fs_flock(uint32_t inode,uint32_t reqid,uint64_t owner,uint8_t cmd) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_FLOCK,17);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,reqid);
	put64bit(&wptr,owner);
	put8bit(&wptr,cmd);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_FLOCK,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_posixlock(uint32_t inode,uint32_t reqid,uint64_t owner,uint8_t cmd,uint8_t type,uint64_t start,uint64_t end,uint32_t pid,uint8_t *rtype,uint64_t *rstart,uint64_t *rend,uint32_t *rpid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_POSIX_LOCK,38);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,reqid);
	put64bit(&wptr,owner);
	put32bit(&wptr,pid);
	put8bit(&wptr,cmd);
	put8bit(&wptr,type);
	put64bit(&wptr,start);
	put64bit(&wptr,end);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_POSIX_LOCK,&i);
	if (rtype!=NULL) {
		*rtype = POSIX_LOCK_UNLCK;
	}
	if (rstart!=NULL) {
		*rstart = 0;
	}
	if (rend!=NULL) {
		*rend = 0;
	}
	if (rpid!=NULL) {
		*rpid = 0;
	}
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i==21) {
		if (rpid!=NULL) {
			*rpid = get32bit(&rptr);
		} else {
			rptr += 4;
		}
		if (rtype!=NULL) {
			*rtype = get8bit(&rptr);
		} else {
			rptr++;
		}
		if (rstart!=NULL) {
			*rstart = get64bit(&rptr);
		} else {
			rptr += 8;
		}
		if (rend!=NULL) {
			*rend = get64bit(&rptr);
		} else {
			rptr += 8;
		}
		ret = MFS_STATUS_OK;
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

// FUSE - META


uint8_t fs_getsustained(const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETSUSTAINED,0);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETSUSTAINED,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = MFS_STATUS_OK;
	}
	return ret;
}

uint8_t fs_gettrash(uint32_t tid,const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (master_version()>=VERSION2INT(3,0,64)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETTRASH,4);
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETTRASH,0);
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	if (master_version()>=VERSION2INT(3,0,64)) {
		put32bit(&wptr,tid);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETTRASH,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = MFS_STATUS_OK;
	}
	return ret;
}

uint8_t fs_getdetachedattr(uint32_t inode,uint8_t attr[ATTR_RECORD_SIZE]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	uint8_t asize = master_attrsize();

	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETDETACHEDATTR,4);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETDETACHEDATTR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=asize) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		copy_attr(rptr,attr,asize);
		ret = MFS_STATUS_OK;
	}
	return ret;
}

uint8_t fs_gettrashpath(uint32_t inode,const uint8_t **path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETTRASHPATH,4);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETTRASHPATH,&i);
	*path = NULL;
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		pleng = get32bit(&rptr);
		if (i!=4+pleng || pleng==0 || rptr[pleng-1]!=0) {
			fs_disconnect();
			ret = MFS_ERROR_IO;
		} else {
			*path = rptr;
			ret = MFS_STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_settrashpath(uint32_t inode,const uint8_t *path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	pleng = strlen((const char *)path)+1;
	wptr = fs_createpacket(rec,CLTOMA_FUSE_SETTRASHPATH,pleng+8);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,pleng);
	memcpy(wptr,path,pleng);
//	ptr+=pleng;
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETTRASHPATH,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_undel(uint32_t inode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_UNDEL,4);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_UNDEL,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_purge(uint32_t inode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_PURGE,4);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_PURGE,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_getfacl(uint32_t inode,/*uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,*/uint8_t acltype,uint16_t *userperm,uint16_t *groupperm,uint16_t *otherperm,uint16_t *maskperm,uint16_t *namedusers,uint16_t *namedgroups,const uint8_t **namedacls,uint32_t *namedaclssize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	*namedacls = NULL;
	*namedaclssize = 0;
	if (master_version()<VERSION2INT(2,0,0)) {
		return MFS_ERROR_ENOTSUP;
	}
	if (master_version()<VERSION2INT(3,0,91)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETFACL,14);
		if (wptr==NULL) {
			return MFS_ERROR_IO;
		}
		put32bit(&wptr,inode);
		put8bit(&wptr,acltype);
		put8bit(&wptr,1);
		put32bit(&wptr,0);
		put32bit(&wptr,0);
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETFACL,5);
		if (wptr==NULL) {
			return MFS_ERROR_IO;
		}
		put32bit(&wptr,inode);
		put8bit(&wptr,acltype);
	}
//	put8bit(&wptr,opened);
//	put32bit(&wptr,uid);
//	if (gids>0) {
//		put32bit(&wptr,gids);
//		for (i=0 ; i<gids ; i++) {
//			put32bit(&wptr,gid[i]);
//		}
//	} else {
//		put32bit(&wptr,0xFFFFFFFFU);
//	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETFACL,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<12) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		*userperm = get16bit(&rptr);
		*groupperm = get16bit(&rptr);
		*otherperm = get16bit(&rptr);
		*maskperm = get16bit(&rptr);
		*namedusers = get16bit(&rptr);
		*namedgroups = get16bit(&rptr);
		*namedacls = rptr;
		*namedaclssize = i-12;
		ret = MFS_STATUS_OK;
	}
	return ret;
}

uint8_t fs_setfacl(uint32_t inode,uint32_t uid,uint8_t acltype,uint16_t userperm,uint16_t groupperm,uint16_t otherperm,uint16_t maskperm,uint16_t namedusers,uint16_t namedgroups,uint8_t *namedacls,uint32_t namedaclssize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (master_version()<VERSION2INT(2,0,0)) {
		return MFS_ERROR_ENOTSUP;
	}
	wptr = fs_createpacket(rec,CLTOMA_FUSE_SETFACL,21+namedaclssize);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put8bit(&wptr,acltype);
	put16bit(&wptr,userperm);
	put16bit(&wptr,groupperm);
	put16bit(&wptr,otherperm);
	put16bit(&wptr,maskperm);
	put16bit(&wptr,namedusers);
	put16bit(&wptr,namedgroups);
	memcpy(wptr,namedacls,namedaclssize);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETFACL,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_getxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t *name,uint8_t mode,const uint8_t **vbuff,uint32_t *vleng) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	*vbuff = NULL;
	*vleng = 0;
	if (master_version()<VERSION2INT(1,7,0)) {
		return MFS_ERROR_ENOTSUP;
	}
	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETXATTR,15+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETXATTR,15+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver==0) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put8bit(&wptr,mode);
	if (packetver>=1) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETXATTR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		*vleng = get32bit(&rptr);
		*vbuff = (mode==MFS_XATTR_GETA_DATA)?rptr:NULL;
		if ((mode==MFS_XATTR_GETA_DATA && i!=(*vleng)+4) || (mode==MFS_XATTR_LENGTH_ONLY && i!=4)) {
			fs_disconnect();
			ret = MFS_ERROR_IO;
		} else {
			ret = MFS_STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_listxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t mode,const uint8_t **dbuff,uint32_t *dleng) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (master_version()<VERSION2INT(1,7,0)) {
		return MFS_ERROR_ENOTSUP;
	}
	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETXATTR,15);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_GETXATTR,15+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver==0) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	put8bit(&wptr,0);
	put8bit(&wptr,mode);
	if (packetver>=1) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETXATTR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	} else {
		*dleng = get32bit(&rptr);
		*dbuff = (mode==MFS_XATTR_GETA_DATA)?rptr:NULL;
		if ((mode==MFS_XATTR_GETA_DATA && i!=(*dleng)+4) || (mode==MFS_XATTR_LENGTH_ONLY && i!=4)) {
			fs_disconnect();
			ret = MFS_ERROR_IO;
		} else {
			ret = MFS_STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_setxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t *name,uint32_t vleng,const uint8_t *value,uint8_t mode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (master_version()<VERSION2INT(1,7,0)) {
		return MFS_ERROR_ENOTSUP;
	}
	if (mode>=MFS_XATTR_REMOVE) {
		return MFS_ERROR_EINVAL;
	}
	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETXATTR,19+nleng+vleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETXATTR,19+nleng+vleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver==0) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,vleng);
	memcpy(wptr,value,vleng);
	wptr+=vleng;
	put8bit(&wptr,mode);
	if (packetver>=1) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETXATTR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_removexattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gids,uint32_t *gid,uint8_t nleng,const uint8_t *name) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t packetver;
	threc *rec = fs_get_my_threc();
	if (master_version()<VERSION2INT(1,7,0)) {
		return MFS_ERROR_ENOTSUP;
	}
	if (master_version()<VERSION2INT(2,0,0)) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETXATTR,19+nleng);
		packetver = 0;
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETXATTR,19+nleng+4*gids);
		packetver = 1;
	}
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	if (packetver==0) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gid[0]);
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,0);
	put8bit(&wptr,MFS_XATTR_REMOVE);
	if (packetver>=1) {
		put8bit(&wptr,opened);
		put32bit(&wptr,uid);
		if (gids>0) {
			put32bit(&wptr,gids);
			for (i=0 ; i<gids ; i++) {
				put32bit(&wptr,gid[i]);
			}
		} else {
			put32bit(&wptr,0xFFFFFFFFU);
		}
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETXATTR,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}

uint8_t fs_custom(uint32_t qcmd,const uint8_t *query,uint32_t queryleng,uint32_t *acmd,const uint8_t **answer,uint32_t *answerleng) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,qcmd,queryleng);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	memcpy(wptr,query,queryleng);
	rptr = fs_sendandreceive_any(rec,acmd,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else {
		*answerleng = i;
		*answer = rptr;
		ret = MFS_STATUS_OK;
	}
	return ret;
}

/*
uint8_t fs_append(uint32_t inode,uint32_t ainode,uint32_t uid,uint32_t gid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_APPEND,16);
	if (wptr==NULL) {
		return MFS_ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,ainode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_APPEND,&i);
	if (rptr==NULL) {
		ret = MFS_ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		fs_disconnect();
		ret = MFS_ERROR_IO;
	}
	return ret;
}
*/
