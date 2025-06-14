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

#include <sys/syslog.h>
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stddef.h>
#include <time.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <sys/resource.h>
#ifdef HAVE_WRITEV
#include <sys/uio.h>
#endif

#include "MFSCommunication.h"

#include "datapack.h"
#include "matoclserv.h"
#include "matocsserv.h"
#include "matomlserv.h"
#include "sessions.h"
#include "csdb.h"
#include "chunks.h"
#include "filesystem.h"
#include "openfiles.h"
#include "flocklocks.h"
#include "posixlocks.h"
#include "metadata.h"
#include "random.h"
#include "exports.h"
#include "datacachemgr.h"
#include "charts.h"
#include "chartsdata.h"
#include "storageclass.h"
#include "patterns.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "mfslog.h"
#include "massert.h"
#include "clocks.h"
#include "missinglog.h"
#include "mfsstrerr.h"
#include "iptosesid.h"
#include "mfsalloc.h"
#include "multilan.h"
#include "hamaster.h"
#include "haconn.h"
#include "shardmgr.h"
#include "raftconsensus.h"

// HA states matching mfscli.py definitions
#define STATE_DUMMY    0
#define STATE_LEADER   1
#define STATE_ELECT    2
#define STATE_FOLLOWER 3

#define MaxPacketSize CLTOMA_MAXPACKETSIZE

// matoclserventry.mode
enum {KILL,DATA,FINISH};
// chunklis.type
enum {FUSE_WRITE,FUSE_READ,FUSE_TRUNCATE,FUSE_CREATE};
enum {NOTREGISTERED,REGISTERED};

struct matoclserventry;

typedef struct out_packetstruct {
	struct out_packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t data[1];
} out_packetstruct;

typedef struct in_packetstruct {
	struct in_packetstruct *next;
	uint32_t type,leng;
	uint8_t data[1];
} in_packetstruct;

typedef struct matoclserventry {
	uint8_t registered;
	uint8_t mode;				//0 - not active, 1 - read header, 2 - read packet
	int sock;				//socket number
	int32_t pdescpos;
	double lastread,lastwrite;		//time of last activity
	uint8_t input_hdr[8];
	uint8_t *input_startptr;
	uint32_t input_bytesleft;
	uint8_t input_end;
	uint8_t asize;
	in_packetstruct *input_packet;
	in_packetstruct *inputhead,**inputtail;
	out_packetstruct *outputhead,**outputtail;
	uint32_t version;
	uint32_t peerip;
	char *strip;
	uint16_t timeout;

	uint8_t working_flags;

	uint8_t passwordrnd[32];

	// extra data used for change session parameters after "reload"
	uint8_t *path;
	uint8_t *info;
	uint32_t ileng;
	uint8_t usepassword;
	uint8_t passwordmd5[16];

	void *sesdata;

	struct matoclserventry *next;
} matoclserventry;

//static session *sessionshead=NULL;
static matoclserventry *matoclservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;

static uint64_t master_processid;

#define CHUNKHASHSIZE 256
#define CHUNKHASH(chunkid) ((chunkid)&0xFF)

#define CHUNK_WAIT_TIMEOUT 30.0

// status waiting chunks
typedef struct _swchunks {
	uint64_t chunkid;
	uint64_t prevchunkid;
	matoclserventry *eptr;
	uint64_t fleng;
	uint32_t msgid;
	uint32_t inode;
	uint32_t indx;
	uint32_t uid;
	uint32_t gid;
	uint32_t auid;
	uint32_t agid;
	uint8_t flags;
	uint8_t type;
	struct _swchunks *next;
} swchunks;

// lock/busy waiting chunks
typedef struct _lwchunks {
	uint64_t chunkid;
	uint64_t fleng;	// TRUNCATE
	matoclserventry *eptr;
	double time;
	uint32_t msgid;
	uint32_t inode;
	uint32_t indx; // WRITE,READ
	uint32_t uid; // TRUNCATE
	uint32_t gids; // TRUNCATE
	uint32_t *gid; // TRUNCATE
	uint32_t auid; // TRUNCATE
	uint32_t agid; // TRUNCATE
	uint8_t chunkopflags; // WRITE,READ
	uint8_t flags; // TRUNCATE
	uint8_t type;
	uint8_t status;
	struct _lwchunks *next;
} lwchunks;

static lwchunks* lwchunkshashhead[CHUNKHASHSIZE];
static lwchunks** lwchunkshashtail[CHUNKHASHSIZE];
static swchunks* swchunkshash[CHUNKHASHSIZE];


// from config
static char *ListenHost;
static char *ListenPort;
static uint32_t listenip;
static uint16_t listenport;
static uint8_t CreateFirstChunk;
static uint32_t DefaultTimeout;
static uint32_t ForceTimeout;
static uint8_t RestrictIncompatibleClientVersions;
static char *InstanceName;
static uint32_t InstanceNameLeng;

static uint32_t stats_prcvd = 0;
static uint32_t stats_psent = 0;
static uint64_t stats_brcvd = 0;
static uint64_t stats_bsent = 0;
static uint64_t stats_mounts_bread = 0;
static uint64_t stats_mounts_bwrite = 0;
static uint32_t stats_mounts_rcnt = 0;
static uint32_t stats_mounts_wcnt = 0;
static uint32_t stats_mounts_fcnt = 0;
static uint64_t stats_mounts_brcvd = 0;
static uint64_t stats_mounts_bsent = 0;
static uint32_t stats_lcnt = 0;

void matoclserv_stats(uint64_t stats[12]) {
	stats[0] = stats_prcvd;
	stats[1] = stats_psent;
	stats[2] = stats_brcvd;
	stats[3] = stats_bsent;
	stats[4] = stats_mounts_bread;
	stats[5] = stats_mounts_bwrite;
	stats[6] = stats_mounts_rcnt;
	stats[7] = stats_mounts_wcnt;
	stats[8] = stats_mounts_fcnt;
	stats[9] = stats_mounts_brcvd;
	stats[10] = stats_mounts_bsent;
	stats[11] = stats_lcnt;
	stats_prcvd = 0;
	stats_psent = 0;
	stats_brcvd = 0;
	stats_bsent = 0;
	stats_mounts_bread = 0;
	stats_mounts_bwrite = 0;
	stats_mounts_rcnt = 0;
	stats_mounts_wcnt = 0;
	stats_mounts_fcnt = 0;
	stats_mounts_brcvd = 0;
	stats_mounts_bsent = 0;
	stats_lcnt = 0;
}

uint8_t* matoclserv_create_packet(matoclserventry *eptr,uint32_t type,uint32_t size) {
	out_packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	psize = size+8;
	outpacket = malloc(offsetof(out_packetstruct,data)+psize);
#ifndef __clang_analyzer__
	passert(outpacket);
	// clang analyzer has problem with testing for (void*)(-1) which is needed for memory allocated by mmap
#endif
	outpacket->bytesleft = psize;
	ptr = outpacket->data;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = outpacket->data;
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

void matoclserv_fuse_chunk_has_changed(matoclserventry *eptr,uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version,uint64_t fleng,uint8_t truncateflag,uint32_t offset,uint32_t size);

void matoclserv_fuse_fleng_has_changed(matoclserventry *eptr,uint32_t inode,uint64_t fleng);

static inline int matoclserv_fuse_write_chunk_common(matoclserventry *eptr,uint32_t msgid,uint32_t inode,uint32_t indx,uint8_t chunkopflags) {
	uint8_t *ptr;
	uint8_t status;
	uint64_t fleng;
	uint64_t prevchunkid;
	uint64_t chunkid;
	uint8_t opflag;
	swchunks *swc;
	lwchunks *lwc;
	uint32_t i;
	uint32_t version;
	uint8_t split;
	uint8_t count;
	uint8_t cs_data[100*14];

	if (sessions_get_disables(eptr->sesdata)&DISABLE_WRITE) {
		status = MFS_ERROR_EPERM;
	} else if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_READONLY) {
		if (eptr->version>=VERSION2INT(3,0,101)) {
			status = MFS_ERROR_EROFS;
		} else {
			status = MFS_ERROR_IO;
		}
	} else {
		status = fs_writechunk(inode,indx,chunkopflags,&prevchunkid,&chunkid,&fleng,&opflag,eptr->peerip);
	}
	if (status!=MFS_STATUS_OK) {
		if (status==MFS_ERROR_LOCKED || status==MFS_ERROR_CHUNKBUSY) {
			i = CHUNKHASH(prevchunkid);
			lwc = malloc(sizeof(lwchunks));
			passert(lwc);
			lwc->chunkid = prevchunkid;
			lwc->eptr = eptr;
			lwc->time = monotonic_seconds();
			lwc->msgid = msgid;
			lwc->inode = inode;
			lwc->indx = indx;
			lwc->chunkopflags = chunkopflags;
			lwc->type = FUSE_WRITE;
			lwc->status = status;
			lwc->next = NULL;
			*(lwchunkshashtail[i]) = lwc;
			lwchunkshashtail[i] = &(lwc->next);
			return 1;
		}
		if (status==MFS_ERROR_EAGAIN && eptr->version<VERSION2INT(3,0,8)) {
			status = MFS_ERROR_LOCKED;
		}
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_WRITE_CHUNK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
		return 0;
	} else {
		sessions_inc_stats(eptr->sesdata,SES_OP_WRITECHUNK);
	}
	if (opflag) {	// wait for operation end
		i = CHUNKHASH(chunkid);
		swc = malloc(sizeof(swchunks));
		passert(swc);
		swc->eptr = eptr;
		swc->inode = inode;
		swc->indx = indx;
		swc->prevchunkid = prevchunkid;
		swc->chunkid = chunkid;
		swc->msgid = msgid;
		swc->fleng = fleng;
		swc->type = FUSE_WRITE;
		swc->next = swchunkshash[i];
		swchunkshash[i] = swc;
	} else {	// return status immediately
		dcm_modify(inode,sessions_get_id(eptr->sesdata));
		if (eptr->version>=VERSION2INT(3,0,10)) {
			status = chunk_get_version_and_csdata(2,chunkid,eptr->peerip,&version,&count,cs_data,&split);
		} else if (eptr->version>=VERSION2INT(1,7,32)) {
			status = chunk_get_version_and_csdata(1,chunkid,eptr->peerip,&version,&count,cs_data,&split);
		} else {
			status = chunk_get_version_and_csdata(0,chunkid,eptr->peerip,&version,&count,cs_data,&split);
		}
		if (status==MFS_STATUS_OK && split) {
			status=MFS_ERROR_EAGAIN;
		}
		if (status!=MFS_STATUS_OK) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_WRITE_CHUNK,5);
			put32bit(&ptr,msgid);
			put8bit(&ptr,status);
			fs_writeend(0,0,chunkid,0,NULL);	// ignore status - just do it.
			return 0;
		}
		if (eptr->version>=VERSION2INT(3,0,10)) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_WRITE_CHUNK,25+count*14);
		} else if (eptr->version>=VERSION2INT(1,7,32)) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_WRITE_CHUNK,25+count*10);
		} else {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_WRITE_CHUNK,24+count*6);
		}
		put32bit(&ptr,msgid);
		if (eptr->version>=VERSION2INT(3,0,10)) {
			put8bit(&ptr,2);
		} else if (eptr->version>=VERSION2INT(1,7,32)) {
			put8bit(&ptr,1);
		}
		put64bit(&ptr,fleng);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,version);
		if (count>0) {
			if (eptr->version>=VERSION2INT(3,0,10)) {
				memcpy(ptr,cs_data,count*14);
			} else if (eptr->version>=VERSION2INT(1,7,32)) {
				memcpy(ptr,cs_data,count*10);
			} else {
				memcpy(ptr,cs_data,count*6);
			}
		}
		matoclserv_fuse_chunk_has_changed(eptr,inode,indx,chunkid,version,fleng,0,0,0);
	}
	return 0;
}

static inline int matoclserv_fuse_read_chunk_common(matoclserventry *eptr,uint32_t msgid,uint32_t inode,uint32_t indx,uint8_t chunkopflags) {
	uint8_t *ptr;
	uint8_t status;
	uint64_t chunkid;
	uint64_t fleng;
	uint32_t version;
	lwchunks *lwc;
	uint32_t i;
	uint8_t split;
	uint8_t count;
	uint8_t cs_data[100*14];

	if (sessions_get_disables(eptr->sesdata)&DISABLE_READ) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_readchunk(inode,sessions_get_sesflags(eptr->sesdata),indx,chunkopflags,1,&chunkid,&fleng);
	}
	if (status!=MFS_STATUS_OK) {
		if (status==MFS_ERROR_LOCKED || status==MFS_ERROR_CHUNKBUSY) {
			i = CHUNKHASH(chunkid);
			lwc = malloc(sizeof(lwchunks));
			passert(lwc);
			lwc->chunkid = chunkid;
			lwc->eptr = eptr;
			lwc->time = monotonic_seconds();
			lwc->msgid = msgid;
			lwc->inode = inode;
			lwc->indx = indx;
			lwc->chunkopflags = chunkopflags;
			lwc->type = FUSE_READ;
			lwc->status = status;
			lwc->next = NULL;
			*(lwchunkshashtail[i]) = lwc;
			lwchunkshashtail[i] = &(lwc->next);
			return 1;
		}
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_READ_CHUNK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
		return 0;
	} else {
		sessions_inc_stats(eptr->sesdata,SES_OP_READCHUNK);
		split = 0;
		if (chunkid>0) {
			if (eptr->version>=VERSION2INT(3,0,10)) {
				status = chunk_get_version_and_csdata(2,chunkid,eptr->peerip,&version,&count,cs_data,&split);
			} else if (eptr->version>=VERSION2INT(1,7,32)) {
				status = chunk_get_version_and_csdata(1,chunkid,eptr->peerip,&version,&count,cs_data,&split);
			} else {
				status = chunk_get_version_and_csdata(0,chunkid,eptr->peerip,&version,&count,cs_data,&split);
			}
			if (status==MFS_STATUS_OK && count==0 && split==0 && version==0) { // special case - allow reading zeros from missing chunk
				chunkid = 0;
			}
		} else {
			version = 0;
			count = 0;
		}
	}
	if ((eptr->version<VERSION2INT(4,0,0) && split==8) || ((eptr->version<VERSION2INT(4,26,0) && split==4))) {
		status = MFS_ERROR_IO;
	}
	if (status!=MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_READ_CHUNK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
		return 0;
	}
	dcm_access(inode,sessions_get_id(eptr->sesdata));
	if (eptr->version>=VERSION2INT(3,0,10)) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_READ_CHUNK,25+count*14);
	} else if (eptr->version>=VERSION2INT(1,7,32)) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_READ_CHUNK,25+count*10);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_READ_CHUNK,24+count*6);
	}
	put32bit(&ptr,msgid);
	if (eptr->version>=VERSION2INT(3,0,10)) {
		if (split) {
			put8bit(&ptr,3);
		} else {
			put8bit(&ptr,2);
		}
	} else if (eptr->version>=VERSION2INT(1,7,32)) {
		put8bit(&ptr,1);
	}
	put64bit(&ptr,fleng);
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	if (count>0) {
		if (eptr->version>=VERSION2INT(3,0,10)) {
			memcpy(ptr,cs_data,count*14);
		} else if (eptr->version>=VERSION2INT(1,7,32)) {
			memcpy(ptr,cs_data,count*10);
		} else {
			memcpy(ptr,cs_data,count*6);
		}
	}
	return 0;
}

static inline int matoclserv_fuse_truncate_common(matoclserventry *eptr,uint32_t msgid,uint32_t inode,uint8_t flags,uint32_t uid,uint32_t gids,uint32_t *gid,uint32_t auid,uint32_t agid,uint64_t fleng) {
	uint32_t indx;
	uint32_t i;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t *ptr;
	uint8_t status;
	uint64_t prevlength;
	uint64_t prevchunkid;
	uint32_t disables;
	swchunks *swc;
	lwchunks *lwc;
	uint64_t chunkid;
//	uint8_t locked;

	chunkid = 0;

	status = MFS_STATUS_OK;
	disables = sessions_get_disables(eptr->sesdata);
	if (flags & (TRUNCATE_FLAG_RESERVE|TRUNCATE_FLAG_UPDATE)) { // part of write - not actual truncate
		if (disables&DISABLE_WRITE) {
			status = MFS_ERROR_EPERM;
		}
	} else {
		if ((disables&(DISABLE_TRUNCATE|DISABLE_SETLENGTH))==(DISABLE_TRUNCATE|DISABLE_SETLENGTH)) {
			status = MFS_ERROR_EPERM;
		}
	}
	if (status==MFS_STATUS_OK) {
		status = fs_try_setlength(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,flags,uid,gids,gid,((disables&DISABLE_TRUNCATE)?1:0)|((disables&DISABLE_SETLENGTH)?2:0),fleng,&indx,&prevchunkid,&chunkid);
	}
	if (status==MFS_ERROR_DELAYED) {
		i = CHUNKHASH(chunkid);
		swc = malloc(sizeof(swchunks));
		passert(swc);
		swc->chunkid = chunkid;
		swc->prevchunkid = prevchunkid;
		swc->eptr = eptr;
		swc->msgid = msgid;
		swc->inode = inode;
		swc->indx = indx;
		swc->uid = uid;
		swc->gid = gid[0];
		swc->auid = auid;
		swc->agid = agid;
		swc->fleng = fleng;
		swc->flags = flags;
		swc->type = FUSE_TRUNCATE;
		swc->next = swchunkshash[i];
		swchunkshash[i] = swc;
		sessions_inc_stats(eptr->sesdata,SES_OP_TRUNCATE);
		return 0;
	}
	if (status==MFS_ERROR_LOCKED || status==MFS_ERROR_CHUNKBUSY) {
//		locked = 1;
		i = CHUNKHASH(prevchunkid);
		lwc = malloc(sizeof(lwchunks)+sizeof(uint32_t)*gids);
		passert(lwc);
		lwc->chunkid = prevchunkid;
		lwc->fleng = fleng;
		lwc->eptr = eptr;
		lwc->status = status;
		lwc->time = monotonic_seconds();
		lwc->msgid = msgid;
		lwc->inode = inode;
		lwc->indx = indx;
		lwc->uid = uid;
		lwc->gids = gids;
		lwc->gid = (uint32_t*)(((uint8_t*)lwc)+sizeof(lwchunks));
		memcpy(lwc->gid,gid,sizeof(uint32_t)*gids);
		lwc->auid = auid;
		lwc->agid = agid;
		lwc->flags = flags;
		lwc->type = FUSE_TRUNCATE;
		lwc->status = status;
		lwc->next = NULL;
		*(lwchunkshashtail[i]) = lwc;
		lwchunkshashtail[i] = &(lwc->next);
		return 1;
//	} else {
//		locked = 0;
	}
	if (status==MFS_STATUS_OK) {
		status = fs_do_setlength(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,flags,uid,gid[0],auid,agid,fleng,attr,&prevlength);
	}
	if (status==MFS_STATUS_OK && (flags & TRUNCATE_FLAG_UPDATE)==0) {
		dcm_modify(inode,sessions_get_id(eptr->sesdata));
	}
	if (status!=MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_TRUNCATE,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	} else {
		if ((eptr->version>=VERSION2INT(3,0,113) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,22,0)) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_TRUNCATE,(eptr->asize+12));
		} else {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_TRUNCATE,(eptr->asize+4));
		}
		put32bit(&ptr,msgid);
		if ((eptr->version>=VERSION2INT(3,0,113) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,22,0)) {
			put64bit(&ptr,prevlength);
		}
		memcpy(ptr,attr,eptr->asize);
		if ((flags & TRUNCATE_FLAG_RESERVE)==0) {
			matoclserv_fuse_fleng_has_changed(eptr,inode,fleng);
		}
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_TRUNCATE);
	return 0;
}

void matoclserv_chunk_unlocked(uint64_t chunkid,void *cptr) {
	lwchunks *lwc,**plwc;
	uint8_t locked;

	plwc = lwchunkshashhead + CHUNKHASH(chunkid);
	while ((lwc = *plwc)) {
		if (lwc->chunkid == chunkid && lwc->eptr->mode==DATA) {
			if (lwc->type == FUSE_TRUNCATE) {
				locked = matoclserv_fuse_truncate_common(lwc->eptr,lwc->msgid,lwc->inode,lwc->flags,lwc->uid,lwc->gids,lwc->gid,lwc->auid,lwc->agid,lwc->fleng);
			} else if (lwc->type == FUSE_WRITE) {
				locked = matoclserv_fuse_write_chunk_common(lwc->eptr,lwc->msgid,lwc->inode,lwc->indx,lwc->chunkopflags);
			} else if (lwc->type == FUSE_READ) {
				locked = matoclserv_fuse_read_chunk_common(lwc->eptr,lwc->msgid,lwc->inode,lwc->indx,lwc->chunkopflags);
			} else {
				locked = 0;
			}
			*plwc = lwc->next;
			free(lwc);
			if (locked || chunk_locked_or_busy(cptr)) {
				break;
			}
		} else {
			plwc = &(lwc->next);
		}
	}
	if (*plwc==NULL) {
		lwchunkshashtail[CHUNKHASH(chunkid)] = plwc;
	}
}

void matoclserv_timeout_waiting_ops(void) {
	lwchunks *lwc,**plwc;
	uint8_t *ptr;
	uint32_t i;
	double curtime;

	curtime = monotonic_seconds();
	for (i=0 ; i<CHUNKHASHSIZE ; i++) {
		plwc = lwchunkshashhead + i;
		while ((lwc = *plwc)) {
			if (lwc->time + CHUNK_WAIT_TIMEOUT < curtime) {
				if (lwc->type == FUSE_TRUNCATE) {
					ptr = matoclserv_create_packet(lwc->eptr,MATOCL_FUSE_TRUNCATE,5);
					put32bit(&ptr,lwc->msgid);
					put8bit(&ptr,lwc->status);
				} else if (lwc->type == FUSE_WRITE) {
					ptr = matoclserv_create_packet(lwc->eptr,MATOCL_FUSE_WRITE_CHUNK,5);
					put32bit(&ptr,lwc->msgid);
					put8bit(&ptr,lwc->status);
				} else if (lwc->type == FUSE_READ) {
					ptr = matoclserv_create_packet(lwc->eptr,MATOCL_FUSE_READ_CHUNK,5);
					put32bit(&ptr,lwc->msgid);
					put8bit(&ptr,lwc->status);
				}
				*plwc = lwc->next;
				free(lwc);
			} else {
				plwc = &(lwc->next);
			}
		}
		lwchunkshashtail[i] = plwc;
	}
}

void matoclserv_chunk_status(uint64_t chunkid,uint8_t status) {
	uint32_t msgid,inode,indx,uid,gid,auid,agid;
	uint64_t fleng,prevchunkid,prevlength;
	uint64_t offset;
	uint8_t flags,type,attr[ATTR_RECORD_SIZE];
	uint32_t version;
	uint8_t *ptr;
	uint8_t split;
	uint8_t count;
	uint8_t cs_data[100*14];
	matoclserventry *eptr;
	swchunks *swc,**pswc;

	eptr = NULL;
	prevchunkid = 0;
	msgid = 0;
	fleng = 0;
	type = 0;
	inode = 0;
	indx = 0;
	uid = 0;
	gid = 0;
	auid = 0;
	agid = 0;
	flags = 0;
	pswc = swchunkshash + CHUNKHASH(chunkid);
	while ((swc = *pswc)) {
		if (swc->chunkid == chunkid) {
			eptr = swc->eptr;
			prevchunkid = swc->prevchunkid;
			msgid = swc->msgid;
			fleng = swc->fleng;
			type = swc->type;
			flags = swc->flags;
			inode = swc->inode;
			indx = swc->indx;
			uid = swc->uid;
			gid = swc->gid;
			auid = swc->auid;
			agid = swc->agid;
			*pswc = swc->next;
			free(swc);
			break;
		} else {
			pswc = &(swc->next);
		}
	}

	if (!eptr) {
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"got chunk status, but don't want it");
		return;
	}
	if (eptr->mode!=DATA) {
		return;
	}
	if (status==MFS_STATUS_OK && type!=FUSE_CREATE) {
		dcm_modify(inode,sessions_get_id(eptr->sesdata));
	}
	switch (type) {
	case FUSE_CREATE:
		if (status==MFS_STATUS_OK) { // just unlock this chunk
			fs_writeend(inode,0,chunkid,0,NULL);
		}
		return;
	case FUSE_WRITE:
		if (status==MFS_STATUS_OK) {
			if (eptr->version>=VERSION2INT(3,0,10)) {
				status = chunk_get_version_and_csdata(2,chunkid,eptr->peerip,&version,&count,cs_data,&split);
			} else if (eptr->version>=VERSION2INT(1,7,32)) {
				status = chunk_get_version_and_csdata(1,chunkid,eptr->peerip,&version,&count,cs_data,&split);
			} else {
				status = chunk_get_version_and_csdata(0,chunkid,eptr->peerip,&version,&count,cs_data,&split);
			}
			//mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"get version for chunk %"PRIu64" -> %"PRIu32,chunkid,version);
		}
		if (status==MFS_STATUS_OK && split) {
			status=MFS_ERROR_EAGAIN;
		}
		if (status!=MFS_STATUS_OK) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_WRITE_CHUNK,5);
			put32bit(&ptr,msgid);
			put8bit(&ptr,status);
			fs_rollback(inode,indx,prevchunkid,chunkid);	// ignore status - it's error anyway
			return;
		}
		if (eptr->version>=VERSION2INT(3,0,10)) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_WRITE_CHUNK,25+count*14);
		} else if (eptr->version>=VERSION2INT(1,7,32)) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_WRITE_CHUNK,25+count*10);
		} else {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_WRITE_CHUNK,24+count*6);
		}
		put32bit(&ptr,msgid);
		if (eptr->version>=VERSION2INT(3,0,10)) {
			put8bit(&ptr,2);
		} else if (eptr->version>=VERSION2INT(1,7,32)) {
			put8bit(&ptr,1);
		}
		put64bit(&ptr,fleng);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,version);
		if (count>0) {
			if (eptr->version>=VERSION2INT(3,0,10)) {
				memcpy(ptr,cs_data,count*14);
			} else if (eptr->version>=VERSION2INT(1,7,32)) {
				memcpy(ptr,cs_data,count*10);
			} else {
				memcpy(ptr,cs_data,count*6);
			}
		}
		matoclserv_fuse_chunk_has_changed(eptr,inode,indx,chunkid,version,fleng,0,0,0);
//		for (i=0 ; i<count ; i++) {
//			if (matocsserv_getlocation(sptr[i],&ip,&port)<0) {
//				put32bit(&ptr,0);
//				put16bit(&ptr,0);
//			} else {
//				put32bit(&ptr,ip);
//				put16bit(&ptr,port);
//			}
//		}
		return;
	case FUSE_TRUNCATE:
		if (status!=MFS_STATUS_OK) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_TRUNCATE,5);
			put32bit(&ptr,msgid);
			put8bit(&ptr,status);
			fs_rollback(inode,indx,prevchunkid,chunkid);	// ignore status - it's error anyway
			return;
		}
		fs_end_setlength(chunkid); // chunk unlock
		status = fs_do_setlength(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,flags,uid,gid,auid,agid,fleng,attr,&prevlength);
		if (status!=MFS_STATUS_OK) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_TRUNCATE,5);
			put32bit(&ptr,msgid);
			put8bit(&ptr,status);
			return;
		}
		if ((eptr->version>=VERSION2INT(3,0,113) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,22,0)) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_TRUNCATE,eptr->asize+12);
		} else {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_TRUNCATE,eptr->asize+4);
		}
		put32bit(&ptr,msgid);
		if ((eptr->version>=VERSION2INT(3,0,113) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,22,0)) {
			put64bit(&ptr,prevlength);
		}
		memcpy(ptr,attr,eptr->asize);
		chunk_get_version(chunkid,&version);
		if (flags & TRUNCATE_FLAG_RESERVE) { // practically not needed - pro forma only - in "reserve" mode chunks are not touched
			fleng += prevlength;
		}
		offset = indx * MFSCHUNKSIZE;
		if (fleng < prevlength) { // calc prevlength = min(prevlength,fleng)
			prevlength = fleng;
		}
		if (prevlength <= offset) {
			offset = 0;
		} else {
			offset = prevlength - offset;
		}
		if (offset >= MFSCHUNKSIZE) {
			offset = MFSCHUNKSIZE;
		}
		matoclserv_fuse_chunk_has_changed(eptr,inode,indx,chunkid,version,fleng,1,offset,0);
		return;
	default:
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"got chunk status, but operation type is unknown");
	}
}

void matoclserv_cserv_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t mode;
	if (length!=0 && length!=1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_CSERV_LIST - wrong size (%"PRIu32"/0|1)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==1) {
		mode = get8bit(&data);
	} else {
		mode = 0;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_CSERV_LIST,csdb_servlist_data(mode,NULL,0));
	csdb_servlist_data(mode,ptr,eptr->peerip);
}

void matoclserv_cserv_command(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t ip;
	uint16_t port;
	uint8_t cmd,status;
	uint8_t *ptr;
	if (length!=6 && length!=7) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_CSSERV_COMMAND - wrong size (%"PRIu32"/6|7)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==7) {
		cmd = get8bit(&data);
	} else {
		cmd = MFS_CSSERV_COMMAND_REMOVE;
	}
	ip = get32bit(&data);
	port = get16bit(&data);
	status = MFS_ERROR_EINVAL;
	if (cmd==MFS_CSSERV_COMMAND_REMOVE) {
		status = csdb_remove_server(ip,port);
	} else if (cmd==MFS_CSSERV_COMMAND_BACKTOWORK) {
		status = csdb_back_to_work(ip,port);
	} else if (cmd==MFS_CSSERV_COMMAND_MAINTENANCEON) {
		status = csdb_maintenance(ip,port,1);
	} else if (cmd==MFS_CSSERV_COMMAND_MAINTENANCEOFF) {
		status = csdb_maintenance(ip,port,0);
	}
	if (length==6) {
		matoclserv_create_packet(eptr,MATOCL_CSSERV_COMMAND,0);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_CSSERV_COMMAND,1);
		put8bit(&ptr,status);
	}
}

void matoclserv_session_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t size;
	uint8_t vmode;
	uint32_t msgid;

	(void)data;
	if (length!=0 && length!=1 && length!=5) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SESSION_LIST - wrong size (%"PRIu32"/0|1|5)",length);
		eptr->mode = KILL;
		return;
	}
	vmode = 0;
	msgid = 0;
	if (length>=1) {
		if (length>=5) {
			msgid = get32bit(&data);
		}
		vmode = get8bit(&data);
	}
	size = sessions_datasize(vmode);
	ptr = matoclserv_create_packet(eptr,MATOCL_SESSION_LIST,size+((length>=5)?4:0));
	if (length>=5) {
		put32bit(&ptr,msgid);
	}
	sessions_datafill(ptr,vmode);
}

void matoclserv_session_command(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t sessionid;
	uint8_t cmd,status;
	uint8_t *ptr;
	if (length!=5) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SESSION_COMMAND - wrong size (%"PRIu32"/5)",length);
		eptr->mode = KILL;
		return;
	}
	cmd = get8bit(&data);
	sessionid = get32bit(&data);
	if (cmd==MFS_SESSION_COMMAND_REMOVE) {
		status = sessions_force_remove(sessionid);
	} else {
		status = MFS_ERROR_EINVAL;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_SESSION_COMMAND,1);
	put8bit(&ptr,status);
}

void matoclserv_chart(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint8_t *ptr;
	uint32_t l;
	uint16_t w,h;

	if (length!=4 && length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOAN_CHART - wrong size (%"PRIu32"/4|8)",length);
		eptr->mode = KILL;
		return;
	}
	chartid = get32bit(&data);
	if (length==8) {
		w = get16bit(&data);
		h = get16bit(&data);
	} else {
		w = 0;
		h = 0;
	}
	l = charts_make_png(chartid,w,h);
	ptr = matoclserv_create_packet(eptr,ANTOCL_CHART,l);
	if (l>0) {
		charts_get_png(ptr);
	}
}

void matoclserv_chart_data(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t chartid;
	uint32_t maxentries;
	uint8_t multimode;
	uint8_t *ptr;
	uint32_t l;

	if (length!=4 && length!=8 && length!=9) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOAN_CHART_DATA - wrong size (%"PRIu32"/4|8|9)",length);
		eptr->mode = KILL;
		return;
	}
	maxentries = UINT32_C(0xFFFFFFFF);
	multimode = 0;
	chartid = get32bit(&data);
	if (length>=8) {
		maxentries = get32bit(&data);
	}
	if (length>=9) {
		multimode = get8bit(&data);
	}
	l = charts_makedata(NULL,chartid,maxentries,multimode);
	ptr = matoclserv_create_packet(eptr,ANTOCL_CHART_DATA,l);
	if (l>0) {
		charts_makedata(ptr,chartid,maxentries,multimode);
	}
}

void matoclserv_monotonic_data(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t l;

	(void)data;
	if (length!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOAN_MONOTONIC_DATA - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	l = charts_monotonic_data(NULL);
	ptr = matoclserv_create_packet(eptr,ANTOCL_MONOTONIC_DATA,l);
	if (l>0) {
		charts_monotonic_data(ptr);
	}
}

void matoclserv_get_version(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid = 0;
	uint8_t *ptr;
	static const char vstring[] = VERSSTR;
	if (length!=0 && length!=4) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOAN_GET_VERSION - wrong size (%"PRIu32"/4|0)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==4) {
		msgid = get32bit(&data);
		ptr = matoclserv_create_packet(eptr,ANTOAN_VERSION,4+4+strlen(vstring));
		put32bit(&ptr,msgid);
	} else {
		ptr = matoclserv_create_packet(eptr,ANTOAN_VERSION,4+strlen(vstring));
	}
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	memcpy(ptr,vstring,strlen(vstring));
}

void matoclserv_get_config(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	char name[256];
	uint8_t nleng;
	uint32_t vleng;
	char *val;
	uint8_t *ptr;

	if (length<5) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOAN_GET_CONFIG - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=5U+(uint32_t)nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOAN_GET_CONFIG - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	memcpy(name,data,nleng);
	name[nleng] = 0;
	val = cfg_getdefaultstr(name);
	if (val!=NULL) {
		vleng = strlen(val);
		if (vleng>255) {
			vleng=255;
		}
	} else {
		vleng = 0;
	}
	if (msgid==0) {
		ptr = matoclserv_create_packet(eptr,ANTOAN_CONFIG_VALUE,6+nleng+vleng);
		put32bit(&ptr,0);
		put8bit(&ptr,nleng);
		if (nleng>0) {
			memcpy(ptr,name,nleng);
			ptr+=nleng;
		}
	} else {
		ptr = matoclserv_create_packet(eptr,ANTOAN_CONFIG_VALUE,5+vleng);
		put32bit(&ptr,msgid);
	}
	put8bit(&ptr,vleng);
	if (vleng>0 && val!=NULL) {
		memcpy(ptr,val,vleng);
	}
	if (val!=NULL) {
		free(val);
	}
}

void matoclserv_get_config_file(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	char name[256];
	uint8_t nleng;
	cfg_buff *fdata;
	uint8_t *ptr;

	if (length<5) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOAN_GET_CONFIG_FILE - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=5U+(uint32_t)nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOAN_GET_CONFIG_FILE - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	memcpy(name,data,nleng);
	name[nleng] = 0;
	fdata = cfg_getdefaultfile(name,65535);
	if (fdata==NULL) {
		ptr = matoclserv_create_packet(eptr,ANTOAN_CONFIG_FILE_CONTENT,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,MFS_ERROR_ENOENT);
	} else {
		ptr = matoclserv_create_packet(eptr,ANTOAN_CONFIG_FILE_CONTENT,6+fdata->leng);
		put32bit(&ptr,msgid);
		put16bit(&ptr,fdata->leng);
		memcpy(ptr,fdata->data,fdata->leng);
		free(fdata);
	}
}

void matoclserv_syslog(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t priority;
	uint32_t timestamp;
	uint16_t msgsize;
	if (length<3) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOMA_SYSLOG - wrong size (%"PRIu32"/>=7)",length);
		eptr->mode = KILL;
		return;
	}
	priority = get8bit(&data);
	timestamp = get32bit(&data);
	msgsize = get16bit(&data);
	if (length!=3U+msgsize) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"ANTOMA_SYSLOG - wrong size (%"PRIu32"/7+msgsize(%"PRIu16"))",length,msgsize);
		eptr->mode = KILL;
		return;
	}
	(void)priority;
	(void)timestamp;
	// lc_log_new_pstr(eptr->modulelogname,priority,timestamp,msgsize,data);
}

void matoclserv_module_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid = 0;
	uint8_t *ptr;

	if (length!=0 && length!=4) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOAN_MODULE_INFO - wrong size (%"PRIu32"/4|0)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==4) {
		msgid = get32bit(&data);
		ptr = matoclserv_create_packet(eptr,ANTOCL_MODULE_INFO,25);
		put32bit(&ptr,msgid);
	} else {
		ptr = matoclserv_create_packet(eptr,ANTOCL_MODULE_INFO,21);
	}
	put8bit(&ptr,MODULE_TYPE_MASTER);
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	put16bit(&ptr,0);
	put64bit(&ptr,meta_get_id());
	put32bit(&ptr,0);
	put16bit(&ptr,0);
}

void matoclserv_list_open_files(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid = 0;
	uint32_t sessionid;
	uint32_t size;
	uint8_t *ptr;

	if (length!=4 && length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_LIST_OPEN_FILES - wrong size (%"PRIu32"/4|8)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==8) {
		msgid = get32bit(&data);
	}
	sessionid = get32bit(&data);
	size = of_lsof(sessionid,NULL);
	if (length==8) {
		ptr = matoclserv_create_packet(eptr,MATOCL_LIST_OPEN_FILES,4+size);
		put32bit(&ptr,msgid);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_LIST_OPEN_FILES,size);
	}
	of_lsof(sessionid,ptr);
}

void matoclserv_list_acquired_locks(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid = 0;
	uint32_t inode;
	uint32_t psize,fsize;
	uint8_t *ptr;

	if (length!=4 && length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_LIST_ACQUIRED_LOCKS - wrong size (%"PRIu32"/4|8)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==8) {
		msgid = get32bit(&data);
	}
	inode = get32bit(&data);
	psize = posix_lock_list(inode,NULL);
	fsize = flock_list(inode,NULL);
	if (length==8) {
		ptr = matoclserv_create_packet(eptr,MATOCL_LIST_ACQUIRED_LOCKS,4+psize+fsize);
		put32bit(&ptr,msgid);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_LIST_ACQUIRED_LOCKS,psize+fsize);
	}
	posix_lock_list(inode,ptr);
	flock_list(inode,ptr+psize);
}

void matoclserv_mass_resolve_paths(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	static uint32_t *inodetab = NULL;
	static uint32_t *psizetab = NULL;
	static uint32_t tabsleng = 0;
	uint32_t i,j;
	uint32_t totalsize;
	uint32_t psize;
	uint8_t *ptr;

	if ((length%4)!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_MASS_RESOLVE_PATHS - wrong size (%"PRIu32"/N*4)",length);
		eptr->mode = KILL;
		return;
	}
	length>>=2;
	if (length>tabsleng) {
		if (inodetab) {
			free(inodetab);
		}
		if (psizetab) {
			free(psizetab);
		}
		tabsleng = ((length+0xFF)&0xFFFFFF00);
		inodetab = malloc(sizeof(uint32_t)*tabsleng);
		passert(inodetab);
		psizetab = malloc(sizeof(uint32_t)*tabsleng);
		passert(psizetab);
	}
	j = 0;
	totalsize = 0;
	while (length>0) {
		i = get32bit(&data);
		if (i>0) {
			fs_get_paths_size(MFS_ROOT_ID,i,&psize);
			inodetab[j] = i;
			psizetab[j] = psize;
			j++;
			totalsize += 8 + psize;
		}
		length--;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_MASS_RESOLVE_PATHS,totalsize);
	for (i=0 ; i<j ; i++) {
		put32bit(&ptr,inodetab[i]);
		put32bit(&ptr,psizetab[i]);
		fs_get_paths_data(MFS_ROOT_ID,inodetab[i],ptr);
		ptr+=psizetab[i];
	}
}

void matoclserv_sclass_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t fver;
	if (length!=0 && length!=1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_INFO - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==1) {
		fver = get8bit(&data);
	} else {
		fver = 0;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_SCLASS_INFO,sclass_info(NULL,fver));
	sclass_info(ptr,fver);
}

void matoclserv_missing_chunks(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t mode;
	if (length!=0 && length!=1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_MISSING_CHUNKS - wrong size (%"PRIu32"/0|1)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==1) {
		mode = get8bit(&data);
	} else {
		mode = 0;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_MISSING_CHUNKS,missing_log_getdata(NULL,mode));
	missing_log_getdata(ptr,mode);
}

void matoclserv_node_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t inode;
	uint32_t maxentries;
	uint64_t continueid;
	uint32_t msgid;
	uint8_t eights_mode;

	if (length!=16 && length!=20) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_NODE_INFO - wrong size (%"PRIu32"/16|20)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==20) {
		msgid = get32bit(&data);
	} else {
		msgid = 0;
	}
	inode = get32bit(&data);
	maxentries = get32bit(&data);
	continueid = get64bit(&data);
	eights_mode = (eptr->version>=VERSION2INT(4,0,0))?1:0;

	ptr = matoclserv_create_packet(eptr,MATOCL_NODE_INFO,fs_node_info(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),eights_mode,inode,maxentries,continueid,NULL)+((length==20)?4:0));
	if (length==20) {
		put32bit(&ptr,msgid);
	}
	fs_node_info(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),eights_mode,inode,maxentries,continueid,ptr);
}

void matoclserv_full_directory_data(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t *ptr,*neptr;
	uint8_t status;
	uint32_t dleng;
	uint32_t maxentries;
	uint64_t nedgeid;
	uint8_t flags;
	void *userptr;

	if (eptr->asize!=ATTR_RECORD_SIZE) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FULL_DIRECTORY_DATA - requested attr size not implemented");
		eptr->mode = KILL;
		return;
	}
	if (length!=20 && length!=21) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FULL_DIRECTORY_DATA - wrong size (%"PRIu32"/20|21)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	maxentries = get32bit(&data);
	nedgeid = get64bit(&data);
	if (length>=21) {
		flags = get8bit(&data);
	} else {
		flags = FULL_DIRECTORY_ADD_CHUNKID|FULL_DIRECTORY_ADD_SYMLINK;
	}
	if (sessions_get_disables(eptr->sesdata)&DISABLE_READDIR) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_readdirfull(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,flags,maxentries,&nedgeid,&userptr,NULL,&dleng);
	}
	if (status!=MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FULL_DIRECTORY_DATA,5);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FULL_DIRECTORY_DATA,12+dleng);
	}
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		neptr = ptr;
		put64bit(&ptr,0);
		fs_readdirfull(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,flags,maxentries,&nedgeid,&userptr,ptr,&dleng);
		put64bit(&neptr,nedgeid);
	}
//	sessions_inc_stats(eptr->sesdata,SES_OP_READDIR);
}

void matoclserv_set_all_node_attributes(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint8_t flags;
	uint32_t disables;
	uint32_t uid;

	if (length<13) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SET_ALL_NODE_ATTRIBUTES - wrong size (%"PRIu32"/<13)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	flags = get8bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	disables = sessions_get_disables(eptr->sesdata);
	if ((flags & SET_ALL_EATTR) && (disables & DISABLE_SETEATTR)) {
		status = MFS_ERROR_EPERM;
	} else if ((flags & SET_ALL_XATTR) && (disables & DISABLE_SETXATTR)) {
		status = MFS_ERROR_EPERM;
	} else if ((flags & SET_ALL_FACL) && (disables & DISABLE_SETFACL)) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_set_additional_attributes(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,flags,uid,data,length-13);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_SET_ALL_NODE_ATTRIBUTES,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t totalspace,availspace,freespace,trspace,respace;
	uint64_t memusage,syscpu,usercpu;
	uint32_t trnodes,renodes,inodes,dnodes,fnodes;
	uint32_t chunks,copychunks,ec8chunks,ec4chunks;
	uint64_t chunkcopies,chunkec8parts,chunkec4parts,chunkhypotheticalcopies;
	uint64_t lsmetaversion;
	uint32_t lsstore,lstime,lsmetachecksum;
	uint8_t lsstat;
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_INFO - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	meta_info(&lsstore,&lstime,&lsstat,&lsmetaversion,&lsmetachecksum);
	fs_info(&totalspace,&availspace,&freespace,&trspace,&trnodes,&respace,&renodes,&inodes,&dnodes,&fnodes);
	chunk_info(&chunks,&copychunks,&ec8chunks,&ec4chunks,&chunkcopies,&chunkec8parts,&chunkec4parts,&chunkhypotheticalcopies);
	chartsdata_resusage(&memusage,&syscpu,&usercpu);
	ptr = matoclserv_create_packet(eptr,MATOCL_INFO,205);
	/* put32bit(&buff,VERSION): */
	put16bit(&ptr,VERSMAJ);
	put8bit(&ptr,VERSMID);
	put8bit(&ptr,VERSMIN);
	put64bit(&ptr,memusage);
	put64bit(&ptr,syscpu);
	put64bit(&ptr,usercpu);
	put64bit(&ptr,totalspace);
	put64bit(&ptr,availspace);
	put64bit(&ptr,freespace);
	put64bit(&ptr,trspace);
	put32bit(&ptr,trnodes);
	put64bit(&ptr,respace);
	put32bit(&ptr,renodes);
	put32bit(&ptr,inodes);
	put32bit(&ptr,dnodes);
	put32bit(&ptr,fnodes);
	put32bit(&ptr,chunks);
	put32bit(&ptr,copychunks);
	put32bit(&ptr,ec8chunks);
	put32bit(&ptr,ec4chunks);
	put32bit(&ptr,lsstore);
	put32bit(&ptr,lstime);
	put8bit(&ptr,lsstat);
	if (ha_mode_enabled()) {
		uint8_t ha_state = STATE_DUMMY;
		if (raft_is_leader()) {
			ha_state = STATE_LEADER;
		} else if (raft_is_candidate()) {
			ha_state = STATE_ELECT;
		} else if (raft_is_follower()) {
			ha_state = STATE_FOLLOWER;
		}
		put8bit(&ptr,ha_state);    // workingstate
		put8bit(&ptr,ha_state);    // nextstate
		put8bit(&ptr,ha_state);    // stablestate
		put8bit(&ptr,1);           // sync (1 = synced, 0 = desynced)
		put32bit(&ptr,raft_get_leader_ip());  // leaderip
		put32bit(&ptr,0);          // changetime
	} else {
		put8bit(&ptr,0xFF);
		put8bit(&ptr,0xFF);
		put8bit(&ptr,0xFF);
		put8bit(&ptr,0xFF);
		put32bit(&ptr,0);
		put32bit(&ptr,0);
	}
	put64bit(&ptr,meta_version());
	put64bit(&ptr,exports_checksum());
	put64bit(&ptr,meta_get_id());
	put64bit(&ptr,lsmetaversion);
	put32bit(&ptr,lsmetachecksum);
	put64bit(&ptr,chunkcopies);
	put64bit(&ptr,chunkec8parts);
	put64bit(&ptr,chunkec4parts);
	put64bit(&ptr,chunkhypotheticalcopies);
	put64bit(&ptr,main_utime());
	put32bit(&ptr,0);
}

void matoclserv_memory_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint64_t allocated[8];
	uint64_t used[8];
	(void)data;
	if (length!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_MEMORY_INFO - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_MEMORY_INFO,176);
	chunk_get_memusage(allocated,used);
	put64bit(&ptr,allocated[0]);
	put64bit(&ptr,used[0]);
	put64bit(&ptr,allocated[1]);
	put64bit(&ptr,used[1]);
	put64bit(&ptr,allocated[2]);
	put64bit(&ptr,used[2]);
	// ignore 4th,5th and 6th values returned by 'chunk_get_memusage' - memory used by those structures are irrelevant
	fs_get_memusage(allocated,used);
	put64bit(&ptr,allocated[0]);
	put64bit(&ptr,used[0]);
	put64bit(&ptr,allocated[1]);
	put64bit(&ptr,used[1]);
	put64bit(&ptr,allocated[2]);
	put64bit(&ptr,used[2]);
	put64bit(&ptr,allocated[3]);
	put64bit(&ptr,used[3]);
	put64bit(&ptr,allocated[4]);
	put64bit(&ptr,used[4]);
	put64bit(&ptr,allocated[5]);
	put64bit(&ptr,used[5]);
	put64bit(&ptr,allocated[6]);
	put64bit(&ptr,used[6]);
	put64bit(&ptr,allocated[7]);
	put64bit(&ptr,used[7]);
}

void matoclserv_fstest_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t loopstart,loopend,files,ugfiles,mfiles,mtfiles,msfiles,chunks,ugchunks,mchunks,msgbuffleng;
	char *msgbuff;
	uint8_t *ptr;
	(void)data;
	if (length!=0 && length!=1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FSTEST_INFO - wrong size (%"PRIu32"/0|1)",length);
		eptr->mode = KILL;
		return;
	}
	fs_test_getdata(&loopstart,&loopend,&files,&ugfiles,&mfiles,&mtfiles,&msfiles,&chunks,&ugchunks,&mchunks,&msgbuff,&msgbuffleng);
	ptr = matoclserv_create_packet(eptr,MATOCL_FSTEST_INFO,msgbuffleng+((length==1)?44:36));
	put32bit(&ptr,loopstart);
	put32bit(&ptr,loopend);
	put32bit(&ptr,files);
	put32bit(&ptr,ugfiles);
	put32bit(&ptr,mfiles);
	if (length==1) {
		put32bit(&ptr,mtfiles);
		put32bit(&ptr,msfiles);
	}
	put32bit(&ptr,chunks);
	put32bit(&ptr,ugchunks);
	put32bit(&ptr,mchunks);
	put32bit(&ptr,msgbuffleng);
	if (msgbuffleng>0) {
		memcpy(ptr,msgbuff,msgbuffleng);
	}
}

void matoclserv_chunkstest_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_CHUNKSTEST_INFO - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_CHUNKSTEST_INFO,chunk_store_info(NULL));
	chunk_store_info(ptr);
}

void matoclserv_chunks_matrix(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t mcnt,i;
	(void)data;
	if (length>2) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_CHUNKS_MATRIX - wrong size (%"PRIu32"/0|1|2)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==1) { // this is old version of the packet !!!
		uint8_t matrixid;
		matrixid = get8bit(&data);
		ptr = matoclserv_create_packet(eptr,MATOCL_CHUNKS_MATRIX,484);
		chunk_store_chunkcounters(ptr,matrixid,0);
	} else {
		uint8_t progressstatus;
		int16_t classid; // <0 means all classes
		mcnt = 8;
		ptr = matoclserv_create_packet(eptr,MATOCL_CHUNKS_MATRIX,1+mcnt*484);
		progressstatus = chunk_counters_in_progress();
		classid = -1;
		if (length==2) {
			data+=1; // skip null_byte - in the future it may be a packet version
			classid = get8bit(&data);
		}
//		mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"progressstatus: %u",progressstatus);
		put8bit(&ptr,progressstatus);
		for (i=0 ; i<mcnt ; i++) {
			chunk_store_chunkcounters(ptr+i*484,i,classid);
		}
	}
}

void matoclserv_quota_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t ver;
	if (length!=0 && length!=1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_QUOTA_INFO - wrong size (%"PRIu32"/(0|1))",length);
		eptr->mode = KILL;
		return;
	}
	if (length==0) {
		ver = 0;
	} else {
		ver = get8bit(&data);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_QUOTA_INFO,fs_getquotainfo(NULL,ver));
	fs_getquotainfo(ptr,ver);
}

void matoclserv_exports_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint8_t vmode;
	if (length!=0 && length!=1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_EXPORTS_INFO - wrong size (%"PRIu32"/0|1)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==0) {
		vmode = 0;
	} else {
		vmode = get8bit(&data);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_EXPORTS_INFO,exports_info_size(vmode));
	exports_info_data(vmode,ptr);
}

void matoclserv_mlog_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	(void)data;
	if (length!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_MLOG_LIST - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_MLOG_LIST,matomlserv_mloglist_size());
	matomlserv_mloglist_data(ptr);
}


void matoclserv_instance_name(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid = 0;
	uint8_t *ptr;

	if (length!=0 && length!=4) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_INSTANCE_NAME - wrong size (%"PRIu32"/4|0)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==4) {
		msgid = get32bit(&data);
		ptr = matoclserv_create_packet(eptr,MATOCL_INSTANCE_NAME,4+1+InstanceNameLeng);
		put32bit(&ptr,msgid);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_INSTANCE_NAME,1+InstanceNameLeng);
	}
	put8bit(&ptr,InstanceNameLeng);
	if (InstanceNameLeng>0) {
		memcpy(ptr,InstanceName,InstanceNameLeng);
	}
}


void matoclserv_fuse_register(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	const uint8_t *rptr;
	uint8_t *wptr;
	uint32_t asize;
	uint32_t sessionid;
	uint8_t status;

	/* In HA mode, only the leader should accept client registrations */
	if (ha_mode_enabled() && !raft_is_leader()) {
		uint32_t leader_id = raft_get_leader();
		uint32_t my_node_id = ha_get_node_id();
		uint32_t leader_ip = 0;
		uint16_t leader_port = 0;
		
		mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"Client registration rejected - not leader (my_node=%u, leader=%u)", my_node_id, leader_id);
		
		/* Sanity check - if raft_get_leader returns our node ID, we're actually the leader */
		if (leader_id == my_node_id) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"Client registration: raft_is_leader/raft_get_leader inconsistency - we are actually the leader, accepting registration");
			/* Continue with normal registration processing */
		} else {
			/* Try to get leader connection information for redirection */
			if (leader_id != 0 && haconn_get_leader_info(leader_id, &leader_ip, &leader_port) == 0) {
				/* Send custom redirection response with leader info */
				uint8_t *ptr = matoclserv_create_packet(eptr, MATOCL_HA_LEADER_REDIRECT, 6);
				put32bit(&ptr, leader_ip);      /* Leader IP address */
				put16bit(&ptr, leader_port);    /* Leader port */
				mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"Client redirection: leader is at %u.%u.%u.%u:%u - closing connection", 
				        (leader_ip >> 24) & 0xFF, (leader_ip >> 16) & 0xFF, (leader_ip >> 8) & 0xFF, leader_ip & 0xFF, leader_port);
				/* Close connection after redirect to force client to reconnect to leader */
				eptr->mode = FINISH;
			} else {
				/* Send standard rejection response */
				uint8_t *ptr = matoclserv_create_packet(eptr, MATOCL_FUSE_REGISTER, 1);
				put8bit(&ptr, MFS_ERROR_EPERM); /* Operation not permitted - no leader info */
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"Client redirection: leader info not available (leader_id=%u)", leader_id);
				/* Close connection since we can't provide service */
				eptr->mode = KILL;
			}
			return;
		}
	}

	if (length<64) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER - wrong size (%"PRIu32"/<64)",length);
		eptr->mode = KILL;
		return;
	}
	if (memcmp(data,FUSE_REGISTER_BLOB_ACL,64)==0) {
		uint64_t expected_metaid;
		uint32_t rootinode;
		uint8_t sesflags;
		uint16_t umaskval;
		uint16_t sclassgroups;
		uint32_t mintrashretention,maxtrashretention;
		uint32_t disables;
		uint32_t rootuid,rootgid;
		uint32_t mapalluid,mapallgid;
		uint32_t ileng,pleng;
		uint8_t i,rcode,created;

		if (length<65) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL - wrong size (%"PRIu32"/<65)",length);
			eptr->mode = KILL;
			return;
		}

		rptr = data+64;
		rcode = get8bit(&rptr);

		if ((eptr->registered==NOTREGISTERED && rcode==REGISTER_CLOSESESSION) || (eptr->registered!=NOTREGISTERED && rcode!=REGISTER_CLOSESESSION)) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL - wrong rcode (%d) for registered status (%d)",rcode,eptr->registered);
			eptr->mode = KILL;
			return;
		}

//		printf("rcode: %d\n",rcode);
//
		switch (rcode) {
		case REGISTER_GETRANDOM:
			if (length!=65) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.1 - wrong size (%"PRIu32"/65)",length);
				eptr->mode = KILL;
				return;
			}
			wptr = matoclserv_create_packet(eptr,MATOCL_FUSE_REGISTER,32);
			for (i=0 ; i<32 ; i++) {
				eptr->passwordrnd[i]=rndu8();
			}
			memcpy(wptr,eptr->passwordrnd,32);
			return;
		case REGISTER_NEWSESSION:
			if (length<77) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%"PRIu32"/>=77)",length);
				eptr->mode = KILL;
				return;
			}
			eptr->version = get32bit(&rptr);
			eptr->asize = (eptr->version>=VERSION2INT(3,0,93)&&eptr->version!=VERSION2INT(4,0,0)&&eptr->version!=VERSION2INT(4,0,1))?ATTR_RECORD_SIZE:35;

			status = MFS_STATUS_OK;
			if (sclass_ec_version()>0 && eptr->version<VERSION2INT(4,0,0)) {
				if (RestrictIncompatibleClientVersions) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.2 - client (ip:%s) is too old - erasure coding needs clients at least 4.x",eptr->strip);
					status = MFS_ERROR_EPERM;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - old client registered - erasure coding needs clients at least 4.x - files in EC format will not be accessible");
				}
			}
			if (sclass_ec_version()>1 && eptr->version<VERSION2INT(4,26,0)) {
				if (RestrictIncompatibleClientVersions) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.2 - client (ip:%s) is too old - erasure coding 4+n needs clients at least 4.26.x",eptr->strip);
					status = MFS_ERROR_EPERM;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.2 - old client registered - erasure coding 4+n needs clients at least 4.26.x - files in EC4 format will not be accessible");
				}
			}

			ileng = get32bit(&rptr);
			if (length<77+ileng) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%"PRIu32"/>=77+ileng(%"PRIu32"))",length,ileng);
				eptr->mode = KILL;
				return;
			}
			if (eptr->info!=NULL) {
				free(eptr->info);
			}
			eptr->ileng = ileng;
			eptr->info = malloc(ileng);
			passert(eptr->info);
			memcpy(eptr->info,rptr,ileng);
			rptr+=ileng;

			pleng = get32bit(&rptr);
			if (length!=77+ileng+pleng && length!=77+16+ileng+pleng && length!=77+4+ileng+pleng && length!=77+4+16+ileng+pleng && length!=77+12+ileng+pleng && length!=77+12+16+ileng+pleng) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.2 - wrong size (%"PRIu32"/77+ileng(%"PRIu32")+pleng(%"PRIu32")+[0|4|12]+[0|16])",length,ileng,pleng);
				eptr->mode = KILL;
				return;
			}
			if (eptr->path!=NULL) {
				free(eptr->path);
			}
			if (pleng>0) {
				eptr->path = malloc(pleng);
				passert(eptr->path);
				memcpy(eptr->path,rptr,pleng);
				rptr+=pleng;
				if (rptr[-1]!=0) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.2 - received path without ending zero");
					eptr->mode = KILL;
					return;
				}
			} else {
				eptr->path = malloc(1);
				passert(eptr->path);
				eptr->path[0] = 0;
			}

			if (length==77+4+ileng+pleng || length==77+4+16+ileng+pleng) {
				sessionid = get32bit(&rptr);
			} else if (length==77+12+ileng+pleng || length==77+12+16+ileng+pleng) {
				sessionid = get32bit(&rptr);
				expected_metaid = get64bit(&rptr);
				if (expected_metaid != meta_get_id()) {
					sessionid = 0;
				}
			} else {
				sessionid = iptosesid_get(eptr->peerip); // patch for clients < 3.0
			}
			if (status==MFS_STATUS_OK) {
				if (length>=77+16+ileng+pleng) {
					eptr->usepassword = 1;
					memcpy(eptr->passwordmd5,rptr,16);
					status = exports_check(eptr->peerip,eptr->version,eptr->path,eptr->passwordrnd,rptr,&sesflags,&umaskval,&rootuid,&rootgid,&mapalluid,&mapallgid,&sclassgroups,&mintrashretention,&maxtrashretention,&disables);
					if (status==MFS_ERROR_BADPASSWORD) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"client from IP:%s attempted a connection with wrong password",eptr->strip);
					}
				} else {
					eptr->usepassword = 0;
					status = exports_check(eptr->peerip,eptr->version,eptr->path,NULL,NULL,&sesflags,&umaskval,&rootuid,&rootgid,&mapalluid,&mapallgid,&sclassgroups,&mintrashretention,&maxtrashretention,&disables);
					if (status==MFS_ERROR_NOPASSWORD) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"client from IP:%s attempted a connection without needed password",eptr->strip);
					}
				}
			}
			if (status==MFS_STATUS_OK) {
				status = fs_getrootinode(&rootinode,eptr->path);
			}
			created = 0;
			if (status==MFS_STATUS_OK) {
				if (sessionid!=0) {
					eptr->sesdata = sessions_find_session(sessionid);
					if (eptr->sesdata==NULL) {
						sessionid = 0;
					} else {
						sessionid = sessions_chg_session(eptr->sesdata,exports_checksum(),rootinode,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,eptr->peerip,eptr->info,eptr->ileng);
					}
				}
				if (sessionid==0) {
					eptr->sesdata = sessions_new_session(exports_checksum(),rootinode,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables,eptr->peerip,eptr->info,eptr->ileng);
					created = 1;
				}
				if (eptr->sesdata==NULL) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't allocate session record");
					eptr->mode = KILL;
					return;
				}
			}
			asize = 1;
			if (status==MFS_STATUS_OK) {
				if (eptr->version>=VERSION2INT(4,40,0)) {
					asize = 57;
				} else if (eptr->version>=VERSION2INT(4,21,0)||(eptr->version>=VERSION2INT(3,0,112)&&eptr->version<VERSION2INT(4,0,0))) {
					asize = 49;
				} else if (eptr->version>=VERSION2INT(3,0,72)) {
					asize = 45;
				} else if (eptr->version>=VERSION2INT(3,0,11)) {
					asize = 43;
				} else if (eptr->version>=VERSION2INT(1,6,26)) {
					asize = 35;
				} else if (eptr->version>=VERSION2INT(1,6,21)) {
					asize = 25;
				} else if (eptr->version>=VERSION2INT(1,6,1)) {
					asize = 21;
				} else {
					asize = 13;
				}
			}
			wptr = matoclserv_create_packet(eptr,MATOCL_FUSE_REGISTER,asize);
			if (status!=MFS_STATUS_OK) {
				put8bit(&wptr,status);
				eptr->sesdata = NULL;
				return;
			}
			sessionid = sessions_get_id(eptr->sesdata);
			if (eptr->version==VERSION2INT(1,6,21)) {
				put32bit(&wptr,0);
			} else if (eptr->version>=VERSION2INT(1,6,22)) {
				put16bit(&wptr,VERSMAJ);
				put8bit(&wptr,VERSMID);
				put8bit(&wptr,VERSMIN);
			}
			put32bit(&wptr,sessionid);
			if (eptr->version>=VERSION2INT(3,0,11)) {
				put64bit(&wptr,meta_get_id());
			}
			put8bit(&wptr,sesflags);
			if (eptr->version>=VERSION2INT(3,0,72)) {
				put16bit(&wptr,umaskval);
			}
			put32bit(&wptr,rootuid);
			put32bit(&wptr,rootgid);
			if (eptr->version>=VERSION2INT(1,6,1)) {
				put32bit(&wptr,mapalluid);
				put32bit(&wptr,mapallgid);
			}
			if (eptr->version>=VERSION2INT(1,6,26)) {
				if (eptr->version>=VERSION2INT(4,57,0)) {
					put16bit(&wptr,sclassgroups);
				} else {
					put8bit(&wptr,1);
					put8bit(&wptr,9);
				}
				put32bit(&wptr,mintrashretention);
				put32bit(&wptr,maxtrashretention);
			}
			if (eptr->version>=VERSION2INT(4,21,0)||(eptr->version>=VERSION2INT(3,0,112)&&eptr->version<VERSION2INT(4,0,0))) {
				put32bit(&wptr,disables);
			}
			if (eptr->version>=VERSION2INT(4,40,0)) {
				put64bit(&wptr,master_processid);
			}
			sessions_attach_session(eptr->sesdata,eptr->peerip,eptr->version);
			eptr->registered = REGISTERED;
			if (created) {
				if (sessionid>0 && sessionid<UINT32_C(0x80000000)) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"created new sessionid:%"PRIu32" (client ip:%s)",sessionid,eptr->strip);
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"created temporary session (id:TMP/%u ; client ip:%s)",sessionid&0x7FFFFFFF,eptr->strip);
				}
			}
			return;
		case REGISTER_NEWMETASESSION:
			if (length<73) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.5 - wrong size (%"PRIu32"/>=73)",length);
				eptr->mode = KILL;
				return;
			}
			eptr->version = get32bit(&rptr);
			eptr->asize = (eptr->version>=VERSION2INT(3,0,93)&&eptr->version!=VERSION2INT(4,0,0)&&eptr->version!=VERSION2INT(4,0,1))?ATTR_RECORD_SIZE:35;

			status = MFS_STATUS_OK;
			if (sclass_ec_version()>0 && eptr->version<VERSION2INT(4,0,0)) {
				if (RestrictIncompatibleClientVersions) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.5 - client (ip:%s) is too old - erasure coding needs clients at least 4.x",eptr->strip);
					status = MFS_ERROR_EPERM;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.5 - old client registered - erasure coding needs clients at least 4.x - files in EC format will not be accessible");
				}
			}
			if (sclass_ec_version()>1 && eptr->version<VERSION2INT(4,26,0)) {
				if (RestrictIncompatibleClientVersions) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.5 - client (ip:%s) is too old - erasure coding 4+n needs clients at least 4.26.x",eptr->strip);
					status = MFS_ERROR_EPERM;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.5 - old client registered - erasure coding 4+n needs clients at least 4.26.x - files in EC4 format will not be accessible");
				}
			}

			ileng = get32bit(&rptr);
			if (length!=73+ileng && length!=73+16+ileng && length!=73+4+ileng && length!=73+4+16+ileng && length!=73+12+ileng && length!=73+12+16+ileng) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.5 - wrong size (%"PRIu32"/73+ileng(%"PRIu32")+[0|4|12]+[0|16])",length,ileng);
				eptr->mode = KILL;
				return;
			}
			if (eptr->info!=NULL) {
				free(eptr->info);
			}
			eptr->ileng = ileng;
			eptr->info = malloc(ileng);
			passert(eptr->info);
			memcpy(eptr->info,rptr,ileng);
			rptr+=ileng;

			if (eptr->path!=NULL) {
				free(eptr->path);
				eptr->path = NULL;
			}

			if (length==73+4+ileng || length==73+4+16+ileng) {
				sessionid = get32bit(&rptr);
			} else if (length==73+12+ileng || length==73+12+16+ileng) {
				sessionid = get32bit(&rptr);
				expected_metaid = get64bit(&rptr);
				if (expected_metaid != meta_get_id()) {
					sessionid = 0;
				}
			} else {
				sessionid = iptosesid_get(eptr->peerip); // patch for clients < 3.0
			}
			if (status==MFS_STATUS_OK) {
				if (length>=73+16+ileng) {
					eptr->usepassword = 1;
					memcpy(eptr->passwordmd5,rptr,16);
					status = exports_check(eptr->peerip,eptr->version,NULL,eptr->passwordrnd,rptr,&sesflags,&umaskval,&rootuid,&rootgid,&mapalluid,&mapallgid,&sclassgroups,&mintrashretention,&maxtrashretention,&disables);
					if (status==MFS_ERROR_BADPASSWORD) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"client from IP:%s attempted a connection with wrong password",eptr->strip);
					}
				} else {
					eptr->usepassword = 0;
					status = exports_check(eptr->peerip,eptr->version,NULL,NULL,NULL,&sesflags,&umaskval,&rootuid,&rootgid,&mapalluid,&mapallgid,&sclassgroups,&mintrashretention,&maxtrashretention,&disables);
					if (status==MFS_ERROR_NOPASSWORD) {
						mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"client from IP:%s attempted a connection without needed password",eptr->strip);
					}
				}
			}
			if (status==MFS_STATUS_OK) {
				if (sessionid!=0) {
					eptr->sesdata = sessions_find_session(sessionid);
					if (eptr->sesdata==NULL) {
						sessionid = 0;
					} else {
						sessionid = sessions_chg_session(eptr->sesdata,exports_checksum(),0,sesflags,umaskval,0,0,0,0,sclassgroups,mintrashretention,maxtrashretention,disables,eptr->peerip,eptr->info,eptr->ileng);
					}
				}
				if (sessionid==0) {
					eptr->sesdata = sessions_new_session(exports_checksum(),0,sesflags,umaskval,0,0,0,0,sclassgroups,mintrashretention,maxtrashretention,disables,eptr->peerip,eptr->info,eptr->ileng);
				}
				if (eptr->sesdata==NULL) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"can't allocate session record");
					eptr->mode = KILL;
					return;
				}
			}
			asize = 1;
			if (status==MFS_STATUS_OK) {
				if (eptr->version>=VERSION2INT(4,40,0)) {
					asize = 35;
				} else if (eptr->version>=VERSION2INT(3,0,11)) {
					asize = 27;
				} else if (eptr->version>=VERSION2INT(1,6,26)) {
					asize = 19;
				} else if (eptr->version>=VERSION2INT(1,6,21)) {
					asize = 9;
				} else {
					asize = 5;
				}
			}
			wptr = matoclserv_create_packet(eptr,MATOCL_FUSE_REGISTER,asize);
			if (status!=MFS_STATUS_OK) {
				put8bit(&wptr,status);
				eptr->sesdata = NULL;
				return;
			}
			sessionid = sessions_get_id(eptr->sesdata);
			if (eptr->version>=VERSION2INT(1,6,21)) {
				put16bit(&wptr,VERSMAJ);
				put8bit(&wptr,VERSMID);
				put8bit(&wptr,VERSMIN);
			}
			put32bit(&wptr,sessionid);
			if (eptr->version>=VERSION2INT(3,0,11)) {
				put64bit(&wptr,meta_get_id());
			}
			put8bit(&wptr,sesflags);
			if (eptr->version>=VERSION2INT(1,6,26)) {
				if (eptr->version>=VERSION2INT(4,57,0)) {
					put16bit(&wptr,sclassgroups);
				} else {
					put8bit(&wptr,1);
					put8bit(&wptr,9);
				}
				put32bit(&wptr,mintrashretention);
				put32bit(&wptr,maxtrashretention);
			}
			if (eptr->version>=VERSION2INT(4,40,0)) {
				put64bit(&wptr,master_processid);
			}
			sessions_attach_session(eptr->sesdata,eptr->peerip,eptr->version);
			eptr->registered = REGISTERED;
			return;
		case REGISTER_RECONNECT:
			if (length<73) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.%"PRIu8" - wrong size (%"PRIu32"/73)",rcode,length);
				eptr->mode = KILL;
				return;
			}

			sessionid = get32bit(&rptr);
			if (iptosesid_check(eptr->peerip)) { // patch for clients < 3.0
				eptr->mode = KILL;
				return;
			}

			eptr->version = get32bit(&rptr);
			eptr->asize = (eptr->version>=VERSION2INT(3,0,93)&&eptr->version!=VERSION2INT(4,0,0)&&eptr->version!=VERSION2INT(4,0,1))?ATTR_RECORD_SIZE:35;

			status = MFS_STATUS_OK;
			if (sclass_ec_version()>0 && eptr->version<VERSION2INT(4,0,0)) {
				if (RestrictIncompatibleClientVersions) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.%"PRIu8" - client (ip:%s) is too old - erasure coding needs clients at least 4.x",rcode,eptr->strip);
					status = MFS_ERROR_EPERM;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.%"PRIu8" - old client registered - erasure coding needs clients at least 4.x - files in EC format will not be accessible",rcode);
				}
			}
			if (sclass_ec_version()>1 && eptr->version<VERSION2INT(4,26,0)) {
				if (RestrictIncompatibleClientVersions) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.%"PRIu8" - client (ip:%s) is too old - erasure coding 4+n needs clients at least 4.26.x",rcode,eptr->strip);
					status = MFS_ERROR_EPERM;
				} else {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"CLTOMA_FUSE_REGISTER/ACL.%"PRIu8" - old client registered - erasure coding 4+n needs clients at least 4.26.x - files in EC4 format will not be accessible",rcode);
				}
			}

			if (status==MFS_STATUS_OK) {
				if (length>=81) {
					expected_metaid = get64bit(&rptr);
					if (expected_metaid!=meta_get_id()) {
						status = MFS_ERROR_BADSESSIONID;
					}
				}
			}
			if (status==MFS_STATUS_OK) {
				eptr->sesdata = sessions_find_session(sessionid);
//				mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"session reconnect: ip:%u.%u.%u.%u ; version:%u.%u.%u ; sessionid: %u (%s)",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF,eptr->version>>16,(eptr->version>>8)&0xFF,(eptr->version>>1)&0x7F,sessionid,(eptr->sesdata)?"found":"not found");
				if (eptr->sesdata==NULL || sessions_get_peerip(eptr->sesdata)==0) { // no such session or session created by entries in metadata
					status = MFS_ERROR_BADSESSIONID;
				} else {
//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"session exports checksum: %016"PRIX64" ; current exports checksum: %016"PRIX64,sessions_get_exportscsum(eptr->sesdata),exports_checksum());
//					mfs_log(MFSLOG_SYSLOG,MFSLOG_DEBUG,"session reconnect: ip:%u.%u.%u.%u (%08X) ; session_peerip(%08X)",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF,eptr->peerip,sessions_get_peerip(eptr->sesdata));
					if (sessions_get_exportscsum(eptr->sesdata)!=exports_checksum() || ((sessions_get_sesflags(eptr->sesdata)&SESFLAG_DYNAMICIP)==0 && eptr->peerip!=sessions_get_peerip(eptr->sesdata))) {
						status = MFS_ERROR_EPERM; // masters < 2.1.0 returned MFS_ERROR_EACCES, so MFS_ERROR_EPERM means that client can use register with sessionid
						iptosesid_add(eptr->peerip,sessionid); // patch for clients < 3.0
					}
				}
			}
			if (rcode==REGISTER_RECONNECT && ((eptr->version>=VERSION2INT(3,0,95)&&eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,0,7))) {
				if (eptr->version>=VERSION2INT(4,40,0)) {
					wptr = matoclserv_create_packet(eptr,MATOCL_FUSE_REGISTER,13);
					put16bit(&wptr,VERSMAJ);
					put8bit(&wptr,VERSMID);
					put8bit(&wptr,VERSMIN);
					put64bit(&wptr,master_processid);
				} else {
					wptr = matoclserv_create_packet(eptr,MATOCL_FUSE_REGISTER,5);
					put16bit(&wptr,VERSMAJ);
					put8bit(&wptr,VERSMID);
					put8bit(&wptr,VERSMIN);
				}
			} else {
				wptr = matoclserv_create_packet(eptr,MATOCL_FUSE_REGISTER,1);
			}
			put8bit(&wptr,status);
			if (status!=MFS_STATUS_OK) {
				eptr->sesdata = NULL;
				return;
			}
			sessions_attach_session(eptr->sesdata,eptr->peerip,eptr->version);
			eptr->registered = REGISTERED;
			return;
		case REGISTER_CLOSESESSION:
			if (length<69) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL.6 - wrong size (%"PRIu32"/69)",length);
				eptr->mode = KILL;
				return;
			}
			sessionid = get32bit(&rptr);
			status = MFS_STATUS_OK;
			if (sessionid>0 && sessionid<UINT32_C(0x80000000)) {
				if (sessions_get_id(eptr->sesdata)!=sessionid) {
					status = MFS_ERROR_BADSESSIONID;
				} else if (length>=77) {
					expected_metaid = get64bit(&rptr);
					if (expected_metaid!=meta_get_id()) {
						status = MFS_ERROR_BADSESSIONID;
					}
				}
				if (status==MFS_STATUS_OK) {
					sessions_close_session(eptr->sesdata);
				}
			}
			if (eptr->version>=VERSION2INT(1,7,29)) {
				wptr = matoclserv_create_packet(eptr,MATOCL_FUSE_REGISTER,1);
				put8bit(&wptr,status);
			}
			eptr->mode = FINISH;
			return;
		}
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER/ACL - wrong rcode (%"PRIu8")",rcode);
		eptr->mode = KILL;
		return;
	} else {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REGISTER - wrong register blob");
		eptr->mode = KILL;
		return;
	}
}

void matoclserv_reload_sessions(void) {
	matoclserventry *eptr;

	exports_reload();
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode==DATA && (eptr->registered!=NOTREGISTERED) && eptr->sesdata!=NULL) {
			if (sessions_get_exportscsum(eptr->sesdata)!=exports_checksum()) {
				eptr->mode = KILL;
			}
		}
	}
}

void matoclserv_broadcast_timeout(void) {
	matoclserventry *eptr;
	uint8_t *data;

	if (ForceTimeout>0) {
		for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
			if (eptr->mode==DATA && (eptr->registered!=NOTREGISTERED) && eptr->version>=VERSION2INT(4,12,0)) {
				eptr->timeout = ForceTimeout;
				data = matoclserv_create_packet(eptr,ANTOAN_FORCE_TIMEOUT,6);
				put32bit(&data,0);
				put16bit(&data,ForceTimeout);
			}
		}
	}
}

void matoclserv_fuse_sustained_inodes(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	const uint8_t *rptr;
	static uint32_t *inodetab = NULL;
	static uint32_t inodetabsize = 0;
	uint32_t i,j;

	if ((length&0x3)!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SUSTAINED_INODES - wrong size (%"PRIu32"/N*4)",length);
		eptr->mode = KILL;
		return;
	}

	if (eptr->sesdata==NULL) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SUSTAINED_INODES - session doesn't exist");
		eptr->mode = KILL;
		return;
	}
	length>>=2;
	if (length>inodetabsize) {
		if (inodetab) {
			free(inodetab);
		}
		inodetabsize = ((length+0xFF)&0xFFFFFF00);
		inodetab = malloc(sizeof(uint32_t)*inodetabsize);
		passert(inodetab);
	}
	rptr = data;
	j = 0;
	while (length>0) {
		i = get32bit(&rptr);
		if (i>0) {
			inodetab[j] = i;
			j++;
		}
		length--;
	}
	of_sync(sessions_get_id(eptr->sesdata),inodetab,j);
//	sessions_sync_open_files(eptr->sesdata,data,length>>2);
}

void matoclserv_fuse_amtime_inodes(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	const uint8_t *rptr;
	static uint32_t *inodetab=NULL,*atimetab=NULL,*mtimetab=NULL;
	static uint32_t tabsizes = 0;
	uint32_t i,a,m,j;

	if ((length%12)!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_AMTIME_INODES - wrong size (%"PRIu32"/N*12)",length);
		eptr->mode = KILL;
		return;
	}

	if (eptr->sesdata==NULL) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_AMTIME_INODES - session doesn't exist");
		eptr->mode = KILL;
		return;
	}
	length/=12;
	if (length>tabsizes) {
		if (inodetab) {
			free(inodetab);
		}
		if (atimetab) {
			free(atimetab);
		}
		if (mtimetab) {
			free(mtimetab);
		}
		tabsizes = ((length+0xFF)&0xFFFFFF00);
		inodetab = malloc(sizeof(uint32_t)*tabsizes);
		passert(inodetab);
		atimetab = malloc(sizeof(uint32_t)*tabsizes);
		passert(atimetab);
		mtimetab = malloc(sizeof(uint32_t)*tabsizes);
		passert(mtimetab);
	}
	rptr = data;
	j = 0;
	while (length>0) {
		i = get32bit(&rptr);
		a = get32bit(&rptr);
		m = get32bit(&rptr);
		if (i>0 && (a>0 || m>0)) {
			inodetab[j] = i;
			atimetab[j] = a;
			mtimetab[j] = m;
			j++;
		}
		length--;
	}
	fs_amtime_update(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inodetab,atimetab,mtimetab,j);
}

void matoclserv_fuse_opdata(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	const uint8_t *rptr;
	uint32_t rcnt,wcnt,fcnt;
	uint64_t rbyt,wbyt;
	uint64_t rcvdbyt,sentbyt;

	if (length!=28 && length!=44) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_OPDATA - wrong size (%"PRIu32"/28|44)",length);
		eptr->mode = KILL;
		return;
	}

	if (eptr->sesdata==NULL) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_OPDATA - session doesn't exist");
		eptr->mode = KILL;
		return;
	}

	rptr = data;
	rbyt = get64bit(&rptr);
	wbyt = get64bit(&rptr);
	rcnt = get32bit(&rptr);
	wcnt = get32bit(&rptr);
	fcnt = get32bit(&rptr);
	if (length==44) {
		rcvdbyt = get64bit(&rptr);
		sentbyt = get64bit(&rptr);
	} else {
		rcvdbyt = 0;
		sentbyt = 0;
	}
	stats_mounts_bread += rbyt;
	stats_mounts_bwrite += wbyt;
	stats_mounts_rcnt += rcnt;
	stats_mounts_wcnt += wcnt;
	stats_mounts_fcnt += fcnt;
	stats_mounts_brcvd += rcvdbyt;
	stats_mounts_bsent += sentbyt;
	sessions_add_stats(eptr->sesdata,SES_OP_READ,rcnt);
	sessions_add_stats(eptr->sesdata,SES_OP_WRITE,wcnt);
	sessions_add_stats(eptr->sesdata,SES_OP_FSYNC,fcnt);
}

void matoclserv_fuse_wflags(matoclserventry *eptr,const uint8_t *data,uint32_t length) {

	if (length!=1) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_FLAGS - wrong size (%"PRIu32"/1)",length);
		eptr->mode = KILL;
		return;
	}

	eptr->working_flags = data[0];
}

void matoclserv_fuse_time_sync(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t msgid;

	if (length!=0 && length!=4) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_TIME_SYNC - wrong size (%"PRIu32"/0)",length);
		eptr->mode = KILL;
		return;
	}

	if (length==4) {
		msgid = get32bit(&data);
	} else {
		msgid = 0;
	}

	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_TIME_SYNC,8+length);
	if (length==4) {
		put32bit(&ptr,msgid);
	}
	put64bit(&ptr,main_utime());
}

uint32_t* matoclserv_gid_storage(uint32_t gids) {
	static uint32_t *gid=NULL;
	static uint32_t gidleng=0;
	if (gids==0) {
		if (gid!=NULL) {
			free(gid);
		}
		gidleng=0;
		return NULL;
	} else {
		if (gidleng<gids) {
			gidleng = (gids+255)&UINT32_C(0xFFFFFF00);
			if (gid!=NULL) {
				free(gid);
			}
			gid = malloc(sizeof(uint32_t)*gidleng);
			passert(gid);
		}
		return gid;
	}
}

void matoclserv_path_lookup(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t uid,gids,auid,agid;
	uint32_t *gid;
	const uint8_t *path;
	uint32_t pleng;
	uint32_t base_inode;
	uint32_t i;
	uint32_t parent_inode,last_inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint8_t name[MFS_NAME_MAX];
	uint8_t nleng;
//	uint16_t lflags;
//	uint8_t accmode;
//	uint8_t filenode;
//	uint8_t validchunk;
//	uint64_t chunkid;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<20U) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATH_LOOKUP - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	base_inode = get32bit(&data);
	pleng = get32bit(&data);
	if (length<20U+pleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATH_LOOKUP - wrong size (%"PRIu32":pleng=%"PRIu32")",length,pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	auid = uid = get32bit(&data);
	gids = get32bit(&data);
	if (gids==0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATH_LOOKUP - group ids missing");
		eptr->mode = KILL;
		return;
	}
	if (length!=20U+pleng+4*gids) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATH_LOOKUP - wrong size (%"PRIu32":pleng=%"PRIu32":gids=%"PRIu32")",length,pleng,gids);
		eptr->mode = KILL;
		return;
	}
	gid = matoclserv_gid_storage(gids);
	for (i=0 ; i<gids ; i++) {
		gid[i] = get32bit(&data);
	}
	agid = gid[0];
	sessions_ugid_remap(eptr->sesdata,&uid,gid);

	status = fs_path_lookup(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),base_inode,pleng,path,uid,gids,gid,auid,agid,&parent_inode,&last_inode,&nleng,name,attr);
	if (status==MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_PATH_LOOKUP,eptr->asize+12+nleng+1);
		put32bit(&ptr,msgid);
		put32bit(&ptr,parent_inode);
		put8bit(&ptr,nleng);
		if (nleng>0){
			memcpy(ptr,name,nleng);
			ptr += nleng;
		}
		put32bit(&ptr,last_inode);
		memcpy(ptr,attr,eptr->asize);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_PATH_LOOKUP,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	}
//	sessions_inc_stats(eptr->sesdata,SES_OP_LOOKUP);
}

void matoclserv_fuse_statfs(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t totalspace,availspace,freespace,trashspace,sustainedspace;
	uint32_t msgid,inodes;
	uint8_t addfreespace;
	uint8_t *ptr;
	if (length!=4) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_STATFS - wrong size (%"PRIu32"/4)",length);
		eptr->mode = KILL;
		return;
	}
	addfreespace = ((eptr->version>=VERSION2INT(3,0,102) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,9,0))?1:0;
	msgid = get32bit(&data);
	fs_statfs(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),&totalspace,&availspace,&freespace,&trashspace,&sustainedspace,&inodes);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_STATFS,addfreespace?48:40);
	put32bit(&ptr,msgid);
	put64bit(&ptr,totalspace);
	put64bit(&ptr,availspace);
	if (addfreespace) {
		put64bit(&ptr,freespace);
	}
	put64bit(&ptr,trashspace);
	put64bit(&ptr,sustainedspace);
	put32bit(&ptr,inodes);
	sessions_inc_stats(eptr->sesdata,SES_OP_STATFS);
}

void matoclserv_fuse_access(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t *gid;
	uint32_t i;
	uint32_t inode,uid,gids;
	uint16_t modemask;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if ((length&1)==1) {
		if (length!=17) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_ACCESS - wrong size (%"PRIu32"/17)",length);
			eptr->mode = KILL;
			return;
		}
		msgid = get32bit(&data);
		inode = get32bit(&data);
		uid = get32bit(&data);
		gid = matoclserv_gid_storage(1);
		gid[0] = get32bit(&data);
		gids = 1;
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
		modemask = get8bit(&data);
	} else {
		if (length<18) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_ACCESS - wrong size (%"PRIu32"/18+4*N)",length);
			eptr->mode = KILL;
			return;
		}
		msgid = get32bit(&data);
		inode = get32bit(&data);
		uid = get32bit(&data);
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_ACCESS - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=18+gids*4) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_ACCESS - wrong size (%"PRIu32"/18+4*N)",length);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
		modemask = get16bit(&data);
	}
	status = fs_access(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,gids,gid,modemask);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_ACCESS,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_lookup(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t newinode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint16_t lflags;
	uint16_t accmode;
	uint8_t filenode;
	uint8_t validchunk;
	uint64_t chunkid;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<17) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_LOOKUP - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length<17U+nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_LOOKUP - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	auid = uid = get32bit(&data);
	if (length==17U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_LOOKUP - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=17U+nleng+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_LOOKUP - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	}
	if (eptr->version>=VERSION2INT(3,0,40)) {
		uint8_t sesflags = sessions_get_sesflags(eptr->sesdata);
		status = fs_lookup(sessions_get_rootinode(eptr->sesdata),sesflags,inode,nleng,name,uid,gids,gid,auid,agid,&newinode,attr,1,&accmode,&filenode,&validchunk,&chunkid);
		if (status==MFS_STATUS_OK) {
			uint32_t version;
			uint8_t split;
			uint8_t count;
			uint8_t cs_data[100*14];
			uint8_t knowflags = ((eptr->version>=VERSION2INT(3,0,113) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,22,0)) ? 1 : 0;
			lflags = (accmode & LOOKUP_ACCESS_BITS);
			split = 0;
			count = 0;
			version = 0;
			if (knowflags==0 && lflags&LOOKUP_APPENDONLY) { // this mount doesn't support append only, so remove 'W' access
				lflags &= LOOKUP_ACCESS_MODES_RO;
			}
			if (filenode && (lflags&LOOKUP_ACCESS_MODES_IO)!=0) { // can be read and/or written
				if (knowflags) {
					if ((lflags&LOOKUP_DIRECTMODE)==0) {
						if (dcm_open(newinode,sessions_get_id(eptr->sesdata))) {
							lflags |= LOOKUP_KEEPCACHE;
						} else { // just fix for old clients
							if (sesflags&SESFLAG_ATTRBIT) {
								attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
							} else {
								attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
							}
						}
					}
				} else {
					if ((sesflags&SESFLAG_ATTRBIT)==0 || (attr[0]&MATTR_DIRECTMODE)==0) {
						if (dcm_open(newinode,sessions_get_id(eptr->sesdata))==0) {
							if (sesflags&SESFLAG_ATTRBIT) {
								attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
							} else {
								attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
							}
						}
					}
				}
				if (validchunk && (sessions_get_disables(eptr->sesdata)&DISABLE_READ)==0) {
					if (chunkid>0) {
						if (chunk_get_version_and_csdata(2,chunkid,eptr->peerip,&version,&count,cs_data,&split)==MFS_STATUS_OK) {
							if (split==0 || (split==8 && eptr->version>=VERSION2INT(4,0,0)) || (split==4 && eptr->version>=VERSION2INT(4,26,0))) {
								lflags |= LOOKUP_CHUNK_ZERO_DATA;
							}
							if (split==0 && count==0 && version==0) {
								chunkid = 0;
							}
						}
					} else {
						version = 0;
						count = 0;
						lflags |= LOOKUP_CHUNK_ZERO_DATA;
					}
				}
			}
			if (sesflags&SESFLAG_READONLY) {
				lflags |= LOOKUP_RO_FILESYSTEM;
			}
			if (lflags & LOOKUP_CHUNK_ZERO_DATA) {
				ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_LOOKUP,eptr->asize+23+count*14);
				put32bit(&ptr,msgid);
				put32bit(&ptr,newinode);
				memcpy(ptr,attr,eptr->asize);
				ptr+=eptr->asize;
				put16bit(&ptr,lflags);
				if (split) {
					put8bit(&ptr,3);
				} else {
					put8bit(&ptr,2);
				}
				put64bit(&ptr,chunkid);
				put32bit(&ptr,version);
				if (count>0) {
					memcpy(ptr,cs_data,count*14);
				}
			} else {
				ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_LOOKUP,eptr->asize+10);
				put32bit(&ptr,msgid);
				put32bit(&ptr,newinode);
				memcpy(ptr,attr,eptr->asize);
				ptr+=eptr->asize;
				put16bit(&ptr,lflags);
			}
		}
	} else {
		status = fs_lookup(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,uid,gids,gid,auid,agid,&newinode,attr,0,NULL,NULL,NULL,NULL);
		if (status==MFS_ERROR_ENOENT_NOCACHE && eptr->version<VERSION2INT(3,0,25)) {
			status = MFS_ERROR_ENOENT;
		}
		if (status==MFS_STATUS_OK) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_LOOKUP,eptr->asize+8);
			put32bit(&ptr,msgid);
			put32bit(&ptr,newinode);
			memcpy(ptr,attr,eptr->asize);
		}
	}
	if (status!=MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_LOOKUP,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_LOOKUP);
}

void matoclserv_fuse_getattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gid,auid,agid;
	uint8_t opened;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8 && length!=16 && length!=17) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETATTR - wrong size (%"PRIu32"/8|16|17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length==17) {
		opened = get8bit(&data);
	} else {
		opened = 0;
	}
	if (length>=16) {
		auid = uid = get32bit(&data);
		agid = gid = get32bit(&data);
		sessions_ugid_remap(eptr->sesdata,&uid,&gid);
	} else {
		auid = uid = 12345;
		agid = gid = 12345;
	}
	status = fs_getattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gid,auid,agid,attr);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETATTR,(status!=MFS_STATUS_OK)?5:(eptr->asize+4));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,eptr->asize);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_GETATTR);
}

void matoclserv_fuse_setattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t opened;
	uint16_t setmask;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint8_t sugidclearmode;
	uint16_t attrmode;
	uint32_t attruid,attrgid,attratime,attrmtime;
	uint32_t disables;
	uint8_t winattr,basesize;

	basesize = (eptr->version>=VERSION2INT(3,0,93)&&eptr->version!=VERSION2INT(4,0,0)&&eptr->version!=VERSION2INT(4,0,1))?38:37;
	if (length!=35 && length!=36 && length<basesize) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETATTR - wrong size (%"PRIu32"/35|36|37|37+N*4|38+N*4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length>=37) {
		opened = get8bit(&data);
	} else {
		opened = 0;
	}
	auid = uid = get32bit(&data);
	if (length<=37) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETATTR - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=basesize+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETATTR - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	setmask = get8bit(&data);
	attrmode = get16bit(&data);
	attruid = get32bit(&data);
	attrgid = get32bit(&data);
	attratime = get32bit(&data);
	attrmtime = get32bit(&data);
	if (basesize==38) {
		winattr = get8bit(&data);
	} else {
		winattr = 0;
	}
	if (length>=36) {
		sugidclearmode = get8bit(&data);
	} else {
		sugidclearmode = SUGID_CLEAR_MODE_ALWAYS; // this is safest option
	}
	disables = sessions_get_disables(eptr->sesdata);
	if (((disables&DISABLE_CHOWN) && (setmask&(SET_UID_FLAG|SET_GID_FLAG))) || ((disables&DISABLE_CHMOD) && (setmask&SET_MODE_FLAG))) {
		status = MFS_ERROR_EPERM;
	} else if (setmask&SET_WINATTR_FLAG && basesize==37) {
		status = MFS_ERROR_EINVAL;
	} else {
		status = fs_setattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gids,gid,auid,agid,setmask,attrmode,attruid,attrgid,attratime,attrmtime,winattr,sugidclearmode,attr);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_SETATTR,(status!=MFS_STATUS_OK)?5:(eptr->asize+4));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,eptr->asize);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_SETATTR);
}

void matoclserv_fuse_truncate(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint32_t msgid;
	uint8_t flags;
	uint64_t fleng;
	if (length!=24 && length<25) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_TRUNCATE - wrong size (%"PRIu32"/24|25+N*4)",length);
		eptr->mode = KILL;
		return;
	}
	flags = 0;
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length>=25) {
		flags = get8bit(&data);
	}
	auid = uid = get32bit(&data);
	if (length<=25) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
		if (length==24) {
			if (uid==0 && gid[0]!=0) {	// stupid "flags" patch for old clients
				flags = TRUNCATE_FLAG_OPENED;
			}
		}
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_TRUNCATE - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=25+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_TRUNCATE - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	}
	fleng = get64bit(&data);
	matoclserv_fuse_truncate_common(eptr,msgid,inode,flags,uid,gids,gid,auid,agid,fleng);
}

void matoclserv_fuse_readlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t pleng;
	uint8_t *path;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_READLINK - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_readlink(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,&pleng,&path);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_READLINK,(status!=MFS_STATUS_OK)?5:8+pleng+1);
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,pleng+1);
		if (pleng>0) {
			memcpy(ptr,path,pleng);
		}
		ptr[pleng]=0;
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_READLINK);
}

void matoclserv_fuse_symlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t nleng;
	const uint8_t *name,*path;
	uint32_t uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint32_t pleng;
	uint32_t newinode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length<21) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SYMLINK - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length<21U+nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SYMLINK - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	pleng = get32bit(&data);
	if (length<21U+nleng+pleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SYMLINK - wrong size (%"PRIu32":nleng=%"PRIu8":pleng=%"PRIu32")",length,nleng,pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	auid = uid = get32bit(&data);
	if (length==21U+nleng+pleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SYMLINK - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=21U+nleng+pleng+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SYMLINK - wrong size (%"PRIu32":nleng=%"PRIu8":pleng=%"PRIu32":gids=%"PRIu32")",length,nleng,pleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	while (pleng>0 && path[pleng-1]==0) {
		pleng--;
	}
	if (sessions_get_disables(eptr->sesdata)&DISABLE_SYMLINK) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_symlink(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,pleng,path,uid,gids,gid,auid,agid,&newinode,attr);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_SYMLINK,(status!=MFS_STATUS_OK)?5:(eptr->asize+8));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,eptr->asize);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_SYMLINK);
}

void matoclserv_fuse_mknod(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid,rdev;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint8_t type;
	uint16_t mode,cumask;
	uint32_t disables;
	uint32_t newinode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<24) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_MKNOD - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=24U+nleng && length<26U+nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_MKNOD - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	type = get8bit(&data);
	mode = get16bit(&data);
	if (length>=26U+nleng) {
		cumask = get16bit(&data);
	} else {
		cumask = 0;
	}
	cumask |= sessions_get_umask(eptr->sesdata);
	auid = uid = get32bit(&data);
	if (length<=26U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_MKNOD - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=26U+nleng+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_MKNOD - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	rdev = get32bit(&data);
	disables = sessions_get_disables(eptr->sesdata);
	if (((disables&DISABLE_MKFIFO) && (type==TYPE_FIFO)) || ((disables&DISABLE_MKDEV) && (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV)) || ((disables&DISABLE_MKSOCK) && (type==TYPE_SOCKET)) || ((disables&DISABLE_CREATE) && (type==TYPE_FILE))) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_mknod(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,type,mode,cumask,uid,gids,gid,auid,agid,rdev,&newinode,attr,NULL);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_MKNOD,(status!=MFS_STATUS_OK)?5:(eptr->asize+8));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,eptr->asize);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_MKNOD);
}

void matoclserv_fuse_mkdir(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint16_t mode,cumask;
	uint32_t newinode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint8_t copysgid;
	if (length<19) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_MKDIR - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=19U+nleng && length!=20U+nleng && length<22U+nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_MKDIR - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	mode = get16bit(&data);
	if (length>=22U+nleng) {
		cumask = get16bit(&data);
	} else {
		cumask = 0;
	}
	cumask |= sessions_get_umask(eptr->sesdata);
	auid = uid = get32bit(&data);
	if (length<=22U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_MKDIR - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=22U+nleng+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_MKDIR - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (length>20U+nleng) {
		copysgid = get8bit(&data);
	} else {
		copysgid = 0; // by default do not copy sgid bit
	}
	if (sessions_get_disables(eptr->sesdata)&DISABLE_MKDIR) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_mkdir(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,mode,cumask,uid,gids,gid,auid,agid,copysgid,&newinode,attr);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_MKDIR,(status!=MFS_STATUS_OK)?5:(eptr->asize+8));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,eptr->asize);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_MKDIR);
}

void matoclserv_fuse_unlink(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,uinode;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<17) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_UNLINK - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length<17U+nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_UNLINK - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	uid = get32bit(&data);
	if (length==17U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_UNLINK - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=17U+nleng+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_UNLINK - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (sessions_get_disables(eptr->sesdata)&DISABLE_UNLINK) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_unlink(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,uid,gids,gid,&uinode);
	}
	if (((eptr->version>=VERSION2INT(3,0,107) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,18,0)) && status==MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_UNLINK,8);
		put32bit(&ptr,msgid);
		put32bit(&ptr,uinode);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_UNLINK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_UNLINK);
}

void matoclserv_fuse_rmdir(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,uinode;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<17) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_RMDIR - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length<17U+nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_RMDIR - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	uid = get32bit(&data);
	if (length==17U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_RMDIR - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=17U+nleng+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_RMDIR - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (sessions_get_disables(eptr->sesdata)&DISABLE_RMDIR) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_rmdir(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,nleng,name,uid,gids,gid,&uinode);
	}
	if (((eptr->version>=VERSION2INT(3,0,107) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,18,0)) && status==MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_RMDIR,8);
		put32bit(&ptr,msgid);
		put32bit(&ptr,uinode);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_RMDIR,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_RMDIR);
}

void matoclserv_fuse_rename(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_src,inode_dst;
	uint8_t nleng_src,nleng_dst;
	const uint8_t *name_src,*name_dst;
	uint32_t uid,gids,auid,agid;
	uint32_t *gid;
	uint8_t rmode;
	uint32_t i;
	uint32_t disables;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length<22) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_RENAME - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode_src = get32bit(&data);
	nleng_src = get8bit(&data);
	if (length<22U+nleng_src) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_RENAME - wrong size (%"PRIu32":nleng_src=%"PRIu8")",length,nleng_src);
		eptr->mode = KILL;
		return;
	}
	name_src = data;
	data += nleng_src;
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length<22U+nleng_src+nleng_dst) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_RENAME - wrong size (%"PRIu32":nleng_src=%"PRIu8":nleng_dst=%"PRIu8")",length,nleng_src,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	auid = uid = get32bit(&data);
	rmode = MFS_RENAME_STD;
	if (length==22U+nleng_src+nleng_dst) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_RENAME - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=22U+nleng_src+nleng_dst+4*gids && length!=23U+nleng_src+nleng_dst+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_RENAME - wrong size (%"PRIu32":nleng_src=%"PRIu8":nleng_dst=%"PRIu8":gids=%"PRIu32")",length,nleng_src,nleng_dst,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
		if (length==23U+nleng_src+nleng_dst+4*gids) {
			rmode = get8bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	disables = sessions_get_disables(eptr->sesdata);
	if ((disables&(DISABLE_RENAME|DISABLE_MOVE))==(DISABLE_RENAME|DISABLE_MOVE) || ((disables&DISABLE_RENAME) && (nleng_src!=nleng_dst || memcmp(name_src,name_dst,nleng_src)!=0)) || ((disables&DISABLE_MOVE) && inode_src!=inode_dst)) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_rename(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode_src,nleng_src,name_src,inode_dst,nleng_dst,name_dst,uid,gids,gid,auid,agid,rmode,((disables&DISABLE_UNLINK)?1:0)|((disables&DISABLE_RMDIR)?2:0),&inode,attr);
	}
	if (eptr->version>=VERSION2INT(1,6,21) && status==MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_RENAME,eptr->asize+8);
		put32bit(&ptr,msgid);
		put32bit(&ptr,inode);
		memcpy(ptr,attr,eptr->asize);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_RENAME,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_RENAME);
}

void matoclserv_fuse_link(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_dst;
	uint8_t nleng_dst;
	const uint8_t *name_dst;
	uint32_t uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint32_t newinode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<21) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_LINK - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length<21U+nleng_dst) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_LINK - wrong size (%"PRIu32":nleng_dst=%"PRIu8")",length,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	auid = uid = get32bit(&data);
	if (length==21U+nleng_dst) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_LINK - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=21U+nleng_dst+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_LINK - wrong size (%"PRIu32":nleng_dst=%"PRIu8":gids=%"PRIu32")",length,nleng_dst,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (sessions_get_disables(eptr->sesdata)&DISABLE_LINK) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_link(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,inode_dst,nleng_dst,name_dst,uid,gids,gid,auid,agid,&newinode,attr);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_LINK,(status!=MFS_STATUS_OK)?5:(eptr->asize+8));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,newinode);
		memcpy(ptr,attr,eptr->asize);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_LINK);
}

void matoclserv_fuse_readdir(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t flags;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	uint32_t maxentries;
	uint64_t nedgeid;
	uint8_t attrmode;
	void *c1,*c2;

	if (eptr->asize==35) {
		attrmode = 1;
	} else if (eptr->asize==ATTR_RECORD_SIZE) {
		attrmode = 2;
	} else {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_READDIR - requested attr size not implemented");
		eptr->mode = KILL;
		return;
	}
	if (length!=16 && length!=17 && length<29) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_READDIR - wrong size (%"PRIu32"/16|17|29+N*4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	auid = uid = get32bit(&data);
	if (length<=29) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_READDIR - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=29+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_READDIR - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (length>=17) {
		flags = get8bit(&data);
	} else {
		flags = 0;
	}
	if (length>=29) {
		maxentries = get32bit(&data);
		nedgeid = get64bit(&data);
	} else {
		maxentries = 0xFFFFFFFF;
		nedgeid = 0;
	}
	if (sessions_get_disables(eptr->sesdata)&DISABLE_READDIR) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_readdir_size(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,gids,gid,flags,maxentries,nedgeid,&c1,&c2,&dleng,attrmode);
	}
	if (status!=MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_READDIR,5);
	} else if (length>=29) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_READDIR,12+dleng);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_READDIR,4+dleng);
	}
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		if (length>=29) {
			put64bit(&ptr,nedgeid);
		}
		fs_readdir_data(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),uid,gid[0],auid,agid,flags,maxentries,&nedgeid,c1,c2,ptr,attrmode);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_READDIR);
}

void matoclserv_fuse_open(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t flags;
	uint8_t oflags;
	uint8_t sesflags;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint8_t knowflags = ((eptr->version>=VERSION2INT(3,0,113) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,22,0)) ? 1 : 0;
	if (length<17) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_OPEN - wrong size (%"PRIu32"/17+N*4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length==17) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		auid = uid = get32bit(&data);
		agid = gid[0] = get32bit(&data);
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	} else {
		auid = uid = get32bit(&data);
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_OPEN - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=17+gids*4) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_OPEN - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
		sessions_ugid_remap(eptr->sesdata,&uid,gid);
	}
	flags = get8bit(&data);
	oflags = 0;
	sesflags = sessions_get_sesflags(eptr->sesdata);
	if ((flags&OPEN_TRUNCATE) && sessions_get_disables(eptr->sesdata)&DISABLE_TRUNCATE) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_opencheck(sessions_get_rootinode(eptr->sesdata),sesflags,inode,uid,gids,gid,auid,agid,flags,attr,&oflags);
	}
	if (status==MFS_STATUS_OK && knowflags==0 && (oflags&OPEN_APPENDONLY) && (flags&OPEN_WRITE)) {
		// this mount doesn't support append only, so we should deny access to any write
		status=MFS_ERROR_EACCES;
	}
	if (status==MFS_STATUS_OK) {
		of_openfile(sessions_get_id(eptr->sesdata),inode);
		if (flags&OPEN_CACHE_CLEARED) {
			dcm_access(inode,sessions_get_id(eptr->sesdata));
		}
	}
	if (eptr->version>=VERSION2INT(1,6,9) && status==MFS_STATUS_OK) {
		if (knowflags) {
			if ((oflags&OPEN_DIRECTMODE)==0) {
				if (dcm_open(inode,sessions_get_id(eptr->sesdata))) {
					oflags |= OPEN_KEEPCACHE;
				} else { // just fix for old clients
					if (sesflags&SESFLAG_ATTRBIT) {
						attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
					} else {
						attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
					}
				}
			}
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_OPEN,(eptr->asize+5));
			put32bit(&ptr,msgid);
			put8bit(&ptr,oflags);
			memcpy(ptr,attr,eptr->asize);
		} else {
			if ((sesflags&SESFLAG_ATTRBIT)==0 || (attr[0]&MATTR_DIRECTMODE)==0) {
				if (dcm_open(inode,sessions_get_id(eptr->sesdata))==0) {
					if (sesflags&SESFLAG_ATTRBIT) {
						attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
					} else {
						attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
					}
				}
			}
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_OPEN,(eptr->asize+4));
			put32bit(&ptr,msgid);
			memcpy(ptr,attr,eptr->asize);
		}
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_OPEN,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_OPEN);
}

void matoclserv_fuse_create(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids,auid,agid;
	uint32_t *gid;
	uint32_t i;
	uint8_t nleng;
	const uint8_t *name;
	uint16_t mode,cumask;
	uint32_t newinode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint8_t oflags;
	uint8_t sesflags;
	uint8_t knowflags = ((eptr->version>=VERSION2INT(3,0,113) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,22,0)) ? 1 : 0;
	if (length<19) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_CREATE - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	nleng = get8bit(&data);
	if (length!=19U+nleng && length<21U+nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_CREATE - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	name = data;
	data += nleng;
	mode = get16bit(&data);
	if (length>=21U+nleng) {
		cumask = get16bit(&data);
	} else {
		cumask = 0;
	}
	cumask |= sessions_get_umask(eptr->sesdata);
	auid = uid = get32bit(&data);
	if (length<=21U+nleng) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		agid = gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_CREATE - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=21U+nleng+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_CREATE - wrong size (%"PRIu32":nleng=%"PRIu8":gids=%"PRIu32")",length,nleng,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		agid = gid[0];
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	sesflags = sessions_get_sesflags(eptr->sesdata);
	if (sessions_get_disables(eptr->sesdata)&DISABLE_CREATE) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_mknod(sessions_get_rootinode(eptr->sesdata),sesflags,inode,nleng,name,TYPE_FILE,mode,cumask,uid,gids,gid,auid,agid,0,&newinode,attr,&oflags);
	}
	if (status==MFS_STATUS_OK) {
		if (CreateFirstChunk) {
			uint64_t prevchunkid,chunkid,fleng;
			uint8_t opflag;
			swchunks *swc;
			/* create first chunk */
			if (fs_writechunk(newinode,0,0,&prevchunkid,&chunkid,&fleng,&opflag,eptr->peerip)==MFS_STATUS_OK) {
				massert(prevchunkid==0,"chunk created after mknod - prevchunkid should be always zero");
				if (opflag) {
					i = CHUNKHASH(chunkid);
					swc = malloc(sizeof(swchunks));
					passert(swc);
					swc->eptr = eptr;
					swc->inode = newinode;
					swc->indx = 0;
					swc->prevchunkid = prevchunkid;
					swc->chunkid = chunkid;
					swc->msgid = 0;
					swc->fleng = fleng;
					swc->type = FUSE_CREATE;
					swc->next = swchunkshash[i];
					swchunkshash[i] = swc;
				} else {
					fs_writeend(newinode,0,chunkid,0,NULL); // no operation? - just unlock this chunk
				}
			}
		}
		/* open file */
		of_openfile(sessions_get_id(eptr->sesdata),newinode);
		if (knowflags) {
			if ((oflags&OPEN_DIRECTMODE)==0) {
// testing dcm_open doesn't make sense here - this is new inode
//				if (dcm_open(newinode,sessions_get_id(eptr->sesdata))) {
//					oflags |= OPEN_KEEPCACHE;
//				} else { // just fix for old clients
					if (sesflags&SESFLAG_ATTRBIT) {
						attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
					} else {
						attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
					}
//				}
			}
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CREATE,eptr->asize+9);
			put32bit(&ptr,msgid);
			put8bit(&ptr,oflags);
			put32bit(&ptr,newinode);
			memcpy(ptr,attr,eptr->asize);
		} else {
			if ((sesflags&SESFLAG_ATTRBIT)==0 || (attr[0]&MATTR_DIRECTMODE)==0) {
//				if (dcm_open(newinode,sessions_get_id(eptr->sesdata))==0) {
					if (sesflags&SESFLAG_ATTRBIT) {
						attr[0]&=(0xFF^MATTR_ALLOWDATACACHE);
					} else {
						attr[1]&=(0xFF^(MATTR_ALLOWDATACACHE<<4));
					}
//				}
			}
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CREATE,eptr->asize+8);
			put32bit(&ptr,msgid);
			put32bit(&ptr,newinode);
			memcpy(ptr,attr,eptr->asize);
		}
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CREATE,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_CREATE);
}

void matoclserv_fuse_read_chunk(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t indx;
	uint8_t chunkopflags;
	uint32_t msgid;

	if (length!=12 && length!=13) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_READ_CHUNK - wrong size (%"PRIu32"/12|13)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	indx = get32bit(&data);
	if (length==13) {
		chunkopflags = get8bit(&data);
	} else {
		chunkopflags = CHUNKOPFLAG_CANMODTIME;
	}
	if (eptr->version>=VERSION2INT(3,0,74)) {
		chunkopflags &= ~CHUNKOPFLAG_CANMODTIME;
	}
	matoclserv_fuse_read_chunk_common(eptr,msgid,inode,indx,chunkopflags);
}

void matoclserv_fuse_write_chunk(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t indx;
	uint32_t msgid;
	uint8_t chunkopflags;

	if (length!=12 && length!=13) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_WRITE_CHUNK - wrong size (%"PRIu32"/12|13)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	indx = get32bit(&data);
	if (length>=13) {
		chunkopflags = get8bit(&data);
	} else {
		chunkopflags = CHUNKOPFLAG_CANMODTIME;
	}
	if (eptr->version>=VERSION2INT(3,0,74)) {
		chunkopflags &= ~CHUNKOPFLAG_CANMODTIME;
	}
	matoclserv_fuse_write_chunk_common(eptr,msgid,inode,indx,chunkopflags);
}

void matoclserv_fuse_write_chunk_end(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t msgid;
	uint32_t inode;
	uint64_t fleng;
	uint32_t indx;
	uint32_t version;
	uint64_t chunkid;
	uint32_t offset;
	uint32_t size;
	uint8_t status;
	uint8_t chunkopflags;
	uint8_t flenghaschanged;
//	chunklist *cl,**acl;
	if (length!=24 && length!=25 && length!=29 && length!=37) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_WRITE_CHUNK_END - wrong size (%"PRIu32"/24|25|29|37)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	chunkid = get64bit(&data);
	inode = get32bit(&data);
	if (length>=29) {
		indx = get32bit(&data);
	} else {
		indx = 0;
	}
	fleng = get64bit(&data);
	if (length>=25) {
		chunkopflags = get8bit(&data);
	} else {
		chunkopflags = CHUNKOPFLAG_CANMODTIME;
	}
	if (length>=37) {
		if (eptr->version<VERSION2INT(4,48,0)) { // due to bug in versions prior to 4.48.0 invalidated region has to be ignored
			offset = 0;
			size = MFSCHUNKSIZE;
			data += 8;
		} else {
			offset = get32bit(&data);
			size = get32bit(&data);
		}
	} else {
		offset = 0;
		size = MFSCHUNKSIZE;
	}
	if (eptr->version>=VERSION2INT(3,0,74)) {
		chunkopflags &= ~CHUNKOPFLAG_CANMODTIME;
	}
	flenghaschanged = 0;
	if (sessions_get_disables(eptr->sesdata)&DISABLE_WRITE) {
		status = MFS_ERROR_EPERM;
	} else if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_READONLY) {
		if (eptr->version>=VERSION2INT(3,0,101)) {
			status = MFS_ERROR_EROFS;
		} else {
			status = MFS_ERROR_IO;
		}
	} else {
		status = fs_writeend(inode,fleng,chunkid,chunkopflags,&flenghaschanged);
	}
	dcm_modify(inode,sessions_get_id(eptr->sesdata));
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_WRITE_CHUNK_END,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	if (length>=29) {
		chunk_get_version(chunkid,&version);
		matoclserv_fuse_chunk_has_changed(eptr,inode,indx,chunkid,version,fleng,0,offset,size);
	} else if (flenghaschanged) {
		matoclserv_fuse_fleng_has_changed(eptr,inode,fleng);
	}
}

void matoclserv_fuse_chunk_has_changed(matoclserventry *eptr,uint32_t inode,uint32_t chindx,uint64_t chunkid,uint32_t version,uint64_t fleng,uint8_t truncateflag,uint32_t offset,uint32_t size) {
	matoclserventry *xeptr;
	uint8_t *ptr;
	uint8_t pver;
	for (xeptr=matoclservhead ; xeptr ; xeptr=xeptr->next) {
		if (xeptr!=eptr && xeptr->mode==DATA && xeptr->registered==REGISTERED && xeptr->sesdata!=NULL && xeptr->version>=VERSION2INT(3,0,74)) {
			if (of_isfileopened_by_session(inode,sessions_get_id(xeptr->sesdata))) {
				pver = (xeptr->version>=VERSION2INT(4,40,0))?1:0;
				ptr = matoclserv_create_packet(xeptr,MATOCL_FUSE_CHUNK_HAS_CHANGED,pver?41:33);
				put32bit(&ptr,0);
				put32bit(&ptr,inode);
				put32bit(&ptr,chindx);
				put64bit(&ptr,chunkid);
				put32bit(&ptr,version);
				put64bit(&ptr,fleng);
				put8bit(&ptr,truncateflag);
				if (pver) {
					put32bit(&ptr,offset);
					put32bit(&ptr,size);
				}
				if (xeptr->working_flags & WFLAG_INVALIDATE_CACHE) {
					dcm_access(inode,sessions_get_id(xeptr->sesdata)); // do not clear cache on next open, because we have invalidated modified chunk
				}
			}
		}
	}
}

void matoclserv_fuse_fleng_has_changed(matoclserventry *eptr,uint32_t inode,uint64_t fleng) {
	matoclserventry *xeptr;
	uint8_t *ptr;
	for (xeptr=matoclservhead ; xeptr ; xeptr=xeptr->next) {
		if (xeptr!=eptr && xeptr->mode==DATA && xeptr->registered==REGISTERED && xeptr->sesdata!=NULL && xeptr->version>=VERSION2INT(3,0,74)) {
			if (of_isfileopened_by_session(inode,sessions_get_id(xeptr->sesdata))) {
				ptr = matoclserv_create_packet(xeptr,MATOCL_FUSE_FLENG_HAS_CHANGED,16);
				put32bit(&ptr,0);
				put32bit(&ptr,inode);
				put64bit(&ptr,fleng);
			}
		}
	}
}

void matoclserv_fuse_invalidate_chunk_cache(void) {
	matoclserventry *xeptr;
	uint8_t *ptr;
	for (xeptr=matoclservhead ; xeptr ; xeptr=xeptr->next) {
		if (xeptr->mode==DATA && xeptr->registered==REGISTERED && xeptr->sesdata!=NULL && (xeptr->version>=VERSION2INT(4,3,0) || (xeptr->version>=VERSION2INT(3,0,100) && xeptr->version<VERSION2INT(4,0,0)))) {
			ptr = matoclserv_create_packet(xeptr,MATOCL_FUSE_INVALIDATE_CHUNK_CACHE,4);
			put32bit(&ptr,0);
		}
	}
}

void matoclserv_fuse_flock_wake_up(void *veptr,uint32_t msgid,uint8_t status) {
	matoclserventry *eptr;
	uint8_t *ptr;
	eptr = (matoclserventry*)veptr;
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_FLOCK,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_flock(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint32_t inode;
	uint32_t reqid;
	uint64_t owner;
	uint8_t cmd;
	uint8_t *ptr;
	uint8_t status;
	if (length!=21) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_FLOCK - wrong size (%"PRIu32"/21)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	reqid = get32bit(&data);
	owner = get64bit(&data);
	cmd = get8bit(&data);
	status = flock_locks_cmd(eptr,sessions_get_id(eptr->sesdata),msgid,reqid,inode,owner,cmd);
	sessions_inc_stats(eptr->sesdata,SES_OP_LOCK);
	stats_lcnt++;
	if (status==MFS_ERROR_WAITING) {
		return;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_FLOCK,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_posix_lock_wake_up(void *veptr,uint32_t msgid,uint8_t status) {
	matoclserventry *eptr;
	uint8_t *ptr;
	eptr = (matoclserventry*)veptr;
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_POSIX_LOCK,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_posix_lock(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint32_t inode;
	uint32_t reqid;
	uint32_t pid;
	uint64_t owner;
	uint64_t start,end;
	uint8_t cmd,type;
	uint8_t *ptr;
	uint8_t status;
	if (length!=42) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_POSIX_LOCK - wrong size (%"PRIu32"/42)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	reqid = get32bit(&data);
	owner = get64bit(&data);
	pid = get32bit(&data);
	cmd = get8bit(&data);
	type = get8bit(&data);
	start = get64bit(&data);
	end = get64bit(&data);
	status = posix_lock_cmd(eptr,sessions_get_id(eptr->sesdata),msgid,reqid,inode,owner,cmd,&type,&start,&end,&pid);
	sessions_inc_stats(eptr->sesdata,SES_OP_LOCK);
	stats_lcnt++;
	if (status==MFS_ERROR_WAITING) {
		return;
	}
	if (cmd==POSIX_LOCK_CMD_GET && status==MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_POSIX_LOCK,25);
		put32bit(&ptr,msgid);
		put32bit(&ptr,pid);
		put8bit(&ptr,type);
		put64bit(&ptr,start);
		put64bit(&ptr,end);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_POSIX_LOCK,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	}
}

void matoclserv_fuse_repair(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint32_t msgid;
	uint32_t chunksnotchanged,chunkserased,chunksrepaired;
	uint8_t *ptr;
	uint8_t status;
	uint8_t flags;
	if (length<16) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REPAIR - wrong size (%"PRIu32"/(16|17)+N*4)",length);
		eptr->mode = KILL;
		return;
	}
	flags = 0;
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	if (length==16) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REPAIR - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=16+4*gids && length!=17+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_REPAIR - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
		if (length==17+4*gids) {
			flags = get8bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	status = fs_repair(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,gids,gid,flags,&chunksnotchanged,&chunkserased,&chunksrepaired);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_REPAIR,(status!=MFS_STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=0) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,chunksnotchanged);
		put32bit(&ptr,chunkserased);
		put32bit(&ptr,chunksrepaired);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_META);
}

void matoclserv_fuse_check(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t indx;
	uint64_t chunkid;
	uint32_t version;
	uint32_t chunkmtime;
	uint32_t basesize;
	uint8_t cs_data[100*8];
	uint8_t count;
	uint8_t cdescsize;
	uint32_t i,chunkcount[2774];
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint8_t mode;
	if (length!=8 && length!=9 && length!=12 && length!=13) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_CHECK - wrong size (%"PRIu32"/8|9|12|13)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	if (length==13 || length==9) {
		mode = get8bit(&data);
	} else {
		mode = 0;
	}
	inode = get32bit(&data);
	if (length==12 || length==13) {
		if (mode!=0 && mode!=1 && mode!=4) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,5);
			put32bit(&ptr,msgid);
			put8bit(&ptr,MFS_ERROR_EINVAL);
			return;
		}
		indx = get32bit(&data);
		status = fs_filechunk(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,indx,&chunkid);
		if (status!=MFS_STATUS_OK) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,5);
			put32bit(&ptr,msgid);
			put8bit(&ptr,status);
			return;
		}
		if (chunkid>0) {
			status = chunk_get_version_and_copies(mode,chunkid,eptr->peerip,&version,&chunkmtime,&count,cs_data);
			if (status!=MFS_STATUS_OK) {
				ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,5);
				put32bit(&ptr,msgid);
				put8bit(&ptr,status);
				return;
			}
		} else {
			version = 0;
			chunkmtime = 0;
			count = 0;
		}
		cdescsize = (mode>=1)?8:7;
		basesize = (mode>=4)?20:16;
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,basesize+count*cdescsize);
		put32bit(&ptr,msgid);
		put64bit(&ptr,chunkid);
		put32bit(&ptr,version);
		if (mode>=4) {
			put32bit(&ptr,chunkmtime);
		}
		if (count>0) {
			memcpy(ptr,cs_data,count*cdescsize);
		}
	} else {
		if (mode!=0 && mode!=2 && mode!=3) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,5);
			put32bit(&ptr,msgid);
			put8bit(&ptr,MFS_ERROR_EINVAL);
			return;
		}
		status = fs_checkfile(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,mode,chunkcount);
		if (status!=MFS_STATUS_OK) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,5);
			put32bit(&ptr,msgid);
			put8bit(&ptr,status);
			return;
		}
		if (mode!=0) {
			if (mode==2) {
				uint8_t j;
				j=0;
				for (i=0 ; i<200 ; i++) {
					if (chunkcount[i]>0) {
						j++;
					}
				}
				ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,4+5*j);
				put32bit(&ptr,msgid);
				for (i=0 ; i<200 ; i++) {
					if (chunkcount[i]>0) {
						put8bit(&ptr,i);
						put32bit(&ptr,chunkcount[i]);
					}
				}
			} else { // mode==3
				uint16_t j;
				j=0;
				for (i=0 ; i<2774 ; i++) {
					if (chunkcount[i]>0) {
						j++;
					}
				}
				ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,4+6*j);
				put32bit(&ptr,msgid);
				for (i=0 ; i<2774 ; i++) {
					if (chunkcount[i]>0) {
						put16bit(&ptr,i);
						put32bit(&ptr,chunkcount[i]);
					}
				}
			}
		} else if (eptr->version>=VERSION2INT(3,0,30)) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,52);
			put32bit(&ptr,msgid);
			for (i=0 ; i<12 ; i++) {
				put32bit(&ptr,chunkcount[i]);
			}
		} else if (eptr->version>=VERSION2INT(1,6,23)) {
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,48);
			put32bit(&ptr,msgid);
			for (i=0 ; i<11 ; i++) {
				put32bit(&ptr,chunkcount[i]);
			}
		} else {
			uint8_t j;
			j=0;
			for (i=0 ; i<11 ; i++) {
				if (chunkcount[i]>0) {
					j++;
				}
			}
			ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_CHECK,4+3*j);
			put32bit(&ptr,msgid);
			for (i=0 ; i<11 ; i++) {
				if (chunkcount[i]>0) {
					put8bit(&ptr,i);
					if (chunkcount[i]<=65535) {
						put16bit(&ptr,chunkcount[i]);
					} else {
						put16bit(&ptr,65535);
					}
				}
			}
		}
	}
}


void matoclserv_fuse_gettrashretention(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t gmode;
	void *fptr,*dptr;
	uint32_t fnodes,dnodes;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETTRASHRETENTION - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	gmode = get8bit(&data);
	status = fs_gettrashretention_prepare(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,gmode,&fptr,&dptr,&fnodes,&dnodes);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETTRASHRETENTION,(status!=MFS_STATUS_OK)?5:12+8*(fnodes+dnodes));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,fnodes);
		put32bit(&ptr,dnodes);
		fs_gettrashretention_store(fptr,dptr,ptr);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_META);
}

void matoclserv_fuse_settrashretention(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,trashretention;
	uint32_t msgid;
	uint8_t smode;
	uint32_t changed,notchanged,notpermitted;
	uint8_t *ptr;
	uint8_t status;
	if (length!=17) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETTRASHRETENTION - wrong size (%"PRIu32"/17)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	trashretention = get32bit(&data);
	smode = get8bit(&data);
	if (sessions_get_disables(eptr->sesdata)&DISABLE_SETTRASH) {
		status = MFS_ERROR_EPERM;
	} else {
		status = sessions_check_trashretention(eptr->sesdata,smode&SMODE_TMASK,trashretention);
	}
	if (status==MFS_STATUS_OK) {
		status = fs_settrashretention(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,trashretention,smode,&changed,&notchanged,&notpermitted);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_SETTRASHRETENTION,(status!=MFS_STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,changed);
		put32bit(&ptr,notchanged);
		put32bit(&ptr,notpermitted);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_META);
}

void matoclserv_fuse_getsclass(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t fgtab[MAXSCLASS],dgtab[MAXSCLASS];
	uint8_t nleng;
	uint16_t i;
	uint8_t fn,dn,gmode;
	uint32_t psize;
	uint8_t *ptr;
	uint8_t status;
	if (length!=9) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETSCLASS - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	gmode = get8bit(&data);
	status = fs_getsclass(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,gmode,fgtab,dgtab);
	fn = 0;
	dn = 0;
	psize = 6;
	if (status==MFS_STATUS_OK) {
		for (i=1 ; i<MAXSCLASS ; i++) {
			if (i<10 && eptr->version<VERSION2INT(3,0,75)) {
				if (fgtab[i]) {
					fn++;
					psize += 5;
				}
				if (dgtab[i]) {
					dn++;
					psize += 5;
				}
			} else if (eptr->version>=VERSION2INT(3,0,75)) {
				if (fgtab[i]) {
					fn++;
					psize += 6 + sclass_get_nleng(i);
				}
				if (dgtab[i]) {
					dn++;
					psize += 6 + sclass_get_nleng(i);
				}
			}
		}
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETSCLASS,(status!=MFS_STATUS_OK)?5:psize);
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put8bit(&ptr,fn);
		put8bit(&ptr,dn);
		for (i=1 ; i<MAXSCLASS ; i++) {
			if (fgtab[i]) {
				if (i<10 && eptr->version<VERSION2INT(3,0,75)) {
					put8bit(&ptr,i);
					put32bit(&ptr,fgtab[i]);
				} else if (eptr->version>=VERSION2INT(3,0,75)) {
					put8bit(&ptr,0xFF);
					nleng = sclass_get_nleng(i);
					put8bit(&ptr,nleng);
					memcpy(ptr,sclass_get_name(i),nleng);
					ptr+=nleng;
					put32bit(&ptr,fgtab[i]);
				}
			}
		}
		for (i=1 ; i<MAXSCLASS ; i++) {
			if (dgtab[i]) {
				if (i<10 && eptr->version<VERSION2INT(3,0,75)) {
					put8bit(&ptr,i);
					put32bit(&ptr,dgtab[i]);
				} else if (eptr->version>=VERSION2INT(3,0,9)) {
					put8bit(&ptr,0xFF);
					nleng = sclass_get_nleng(i);
					put8bit(&ptr,nleng);
					memcpy(ptr,sclass_get_name(i),nleng);
					ptr+=nleng;
					put32bit(&ptr,dgtab[i]);
				}
			}
		}
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_META);
}

void matoclserv_fuse_setsclass(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid;
	uint32_t msgid;
	uint8_t setid,smode;
	uint32_t changed,notchanged,notpermitted;
	uint8_t scnleng;
	uint8_t src_sclassid,dst_sclassid;
	uint32_t pskip;
	uint8_t *ptr;
	uint8_t status;
	if (length<14) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETSCLASS - wrong size (%"PRIu32"/<14)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	setid = get8bit(&data);
	smode = get8bit(&data);
	if (setid==0xFF) {
		if ((smode&SMODE_TMASK)==SMODE_EXCHANGE) {
			if (length<15) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETSCLASS - wrong size (%"PRIu32"/<15)",length);
				eptr->mode = KILL;
				return;
			}
			scnleng = get8bit(&data);
			src_sclassid = sclass_find_by_name(scnleng,data);
			data += scnleng;
			pskip = 15+scnleng;
		} else {
			pskip = 14;
			src_sclassid = 0x1; // any non 0 value
		}
		if (length<pskip+1) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETSCLASS - wrong size (%"PRIu32")",length);
			eptr->mode = KILL;
			return;
		}
		scnleng = get8bit(&data);
		dst_sclassid = sclass_find_by_name(scnleng,data);
		data += scnleng;
		if (length<pskip+1+scnleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETSCLASS - wrong size (%"PRIu32")",length);
			eptr->mode = KILL;
			return;
		}
		if (sessions_get_disables(eptr->sesdata)&DISABLE_SETSCLASS) {
			status = MFS_ERROR_EPERM;
		} else if (src_sclassid==0 || dst_sclassid==0) {
			status = MFS_ERROR_NOSUCHCLASS;
		} else {
			status = sessions_check_sclass(eptr->sesdata,smode&SMODE_TMASK,dst_sclassid);
		}
	} else { // setid == goal
		if (sessions_get_disables(eptr->sesdata)&DISABLE_SETSCLASS) {
			status = MFS_ERROR_EPERM;
		} else if (setid<1 || setid>9) {
			status = MFS_ERROR_EINVAL;
		} else {
			src_sclassid = setid;
			dst_sclassid = setid;
			if (length!=14) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETSCLASS (classic version) - wrong size (%"PRIu32"/14)",length);
				eptr->mode = KILL;
				return;
			}
			status = sessions_check_sclass(eptr->sesdata,smode&SMODE_TMASK,setid);
		}
	}
	if (status==MFS_STATUS_OK) {
		status = fs_setsclass(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,src_sclassid,dst_sclassid,smode,&changed,&notchanged,&notpermitted);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_SETSCLASS,(status!=MFS_STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,changed);
		put32bit(&ptr,notchanged);
		put32bit(&ptr,notpermitted);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_META);
}

void matoclserv_fuse_geteattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t feattrtab[1<<EATTR_BITS],deattrtab[1<<EATTR_BITS];
	uint8_t gmode;
	uint16_t i,fn,dn;
	uint8_t *ptr;
	uint8_t status;
	uint8_t knowflags = ((eptr->version>=VERSION2INT(3,0,113) && eptr->version<VERSION2INT(4,0,0)) || eptr->version>=VERSION2INT(4,22,0)) ? 1 : 0;
	if (length!=9) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETEATTR - wrong size (%"PRIu32"/9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	gmode = get8bit(&data);
	status = fs_geteattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,gmode,feattrtab,deattrtab);
	if (eptr->version < VERSION2INT(3,0,30)) {
		for (i=16 ; i<(1<<EATTR_BITS) ; i++) {
			feattrtab[i&0xF] += feattrtab[i];
			feattrtab[i] = 0;
			deattrtab[i&0xF] += deattrtab[i];
			deattrtab[i] = 0;
		}
	} else if (knowflags==0) {
		for (i=32 ; i<(1<<EATTR_BITS) ; i++) {
			feattrtab[i&0x1F] += feattrtab[i];
			feattrtab[i] = 0;
			deattrtab[i&0x1F] += deattrtab[i];
			deattrtab[i] = 0;
		}
	}
	fn=0;
	dn=0;
	if (status==MFS_STATUS_OK) {
		for (i=0 ; i<(1<<EATTR_BITS) ; i++) {
			if (feattrtab[i] && fn<255) {
				fn++;
			} else {
				feattrtab[i]=0; // TODO increase fn and dn in packets
			}
			if (deattrtab[i] && dn<255) {
				dn++;
			} else {
				deattrtab[i]=0;
			}
		}
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETEATTR,(status!=MFS_STATUS_OK)?5:6+5*(fn+dn));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put8bit(&ptr,fn);
		put8bit(&ptr,dn);
		for (i=0 ; i<(1<<EATTR_BITS) ; i++) {
			if (feattrtab[i]) {
				put8bit(&ptr,i);
				put32bit(&ptr,feattrtab[i]);
			}
		}
		for (i=0 ; i<(1<<EATTR_BITS) ; i++) {
			if (deattrtab[i]) {
				put8bit(&ptr,i);
				put32bit(&ptr,deattrtab[i]);
			}
		}
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_META);
}

void matoclserv_fuse_seteattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid;
	uint32_t msgid;
	uint8_t eattr,smode;
	uint32_t changed,notchanged,notpermitted;
	uint8_t *ptr;
	uint8_t status;
	if (length!=14) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETEATTR - wrong size (%"PRIu32"/14)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	eattr = get8bit(&data);
	smode = get8bit(&data);
	if (sessions_get_disables(eptr->sesdata)&DISABLE_SETEATTR) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_seteattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,eattr,smode,&changed,&notchanged,&notpermitted);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_SETEATTR,(status!=MFS_STATUS_OK)?5:16);
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,changed);
		put32bit(&ptr,notchanged);
		put32bit(&ptr,notpermitted);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_META);
}

void matoclserv_fuse_parents(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t pcount;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_PARENTS - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_parents_count(sessions_get_rootinode(eptr->sesdata),inode,&pcount);
	if (status!=MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_PARENTS,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_PARENTS,4+4*pcount);
		put32bit(&ptr,msgid);
		fs_get_parents_data(sessions_get_rootinode(eptr->sesdata),inode,ptr);
	}
}

void matoclserv_fuse_paths(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint32_t psize;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_PATHS - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_paths_size(sessions_get_rootinode(eptr->sesdata),inode,&psize);
	if (status!=MFS_STATUS_OK) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_PATHS,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_PATHS,4+psize);
		put32bit(&ptr,msgid);
		fs_get_paths_data(sessions_get_rootinode(eptr->sesdata),inode,ptr);
	}
}

void matoclserv_fuse_getxattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint32_t msgid;
	uint8_t opened;
	uint8_t mode;
	uint8_t *ptr;
	uint8_t status;
	uint8_t anleng;
	const uint8_t *attrname;
	if (length<19) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETXATTR - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	opened = 0; // makes gcc happy
	gid = NULL; // makes gcc happy
	uid = 0;
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (eptr->version<VERSION2INT(2,0,0)) {
		opened = get8bit(&data);
		uid = get32bit(&data);
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	}
	anleng = get8bit(&data);
	attrname = data;
	data+=anleng;
	if (length<19U+anleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETXATTR - wrong size (%"PRIu32":anleng=%"PRIu8")",length,anleng);
		eptr->mode = KILL;
		return;
	}
	mode = get8bit(&data);
	if (eptr->version>=VERSION2INT(2,0,0)) {
		opened = get8bit(&data);
		uid = get32bit(&data);
		if (length==19U+anleng) {
			gids = 1;
			gid = matoclserv_gid_storage(gids);
			gid[0] = get32bit(&data);
		} else {
			gids = get32bit(&data);
			if (gids==0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETXATTR - group ids missing");
				eptr->mode = KILL;
				return;
			}
			if (length!=19U+anleng+4*gids) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETXATTR - wrong size (%"PRIu32":anleng=%"PRIu8":gids=%"PRIu32")",length,anleng,gids);
				eptr->mode = KILL;
				return;
			}
			gid = matoclserv_gid_storage(gids);
			for (i=0 ; i<gids ; i++) {
				gid[i] = get32bit(&data);
			}
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (mode!=MFS_XATTR_GETA_DATA && mode!=MFS_XATTR_LENGTH_ONLY) {
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETXATTR,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,MFS_ERROR_EINVAL);
	} else if (anleng==0) {
		void *xanode;
		uint32_t xasize;
		status = fs_listxattr_leng(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gids,gid,&xanode,&xasize);
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETXATTR,(status!=MFS_STATUS_OK)?5:8+((mode==MFS_XATTR_GETA_DATA)?xasize:0));
		put32bit(&ptr,msgid);
		if (status!=MFS_STATUS_OK) {
			put8bit(&ptr,status);
		} else {
			put32bit(&ptr,xasize);
			if (mode==MFS_XATTR_GETA_DATA && xasize>0) {
				fs_listxattr_data(xanode,ptr);
			}
		}
	} else {
		const uint8_t *attrvalue;
		uint32_t avleng;
		status = fs_getxattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gids,gid,anleng,attrname,&avleng,&attrvalue);
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETXATTR,(status!=MFS_STATUS_OK)?5:8+((mode==MFS_XATTR_GETA_DATA)?avleng:0));
		put32bit(&ptr,msgid);
		if (status!=MFS_STATUS_OK) {
			put8bit(&ptr,status);
		} else {
			put32bit(&ptr,avleng);
			if (mode==MFS_XATTR_GETA_DATA && avleng>0) {
				memcpy(ptr,attrvalue,avleng);
			}
		}
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_GETXATTR);
}

void matoclserv_fuse_setxattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint32_t msgid;
	const uint8_t *attrname,*attrvalue;
	uint8_t opened;
	uint8_t anleng;
	uint32_t avleng;
	uint8_t mode;
	uint8_t *ptr;
	uint8_t status;
	if (length<23) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETXATTR - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	opened = 0; // makes gcc happy
	uid = 0; // makes gcc happy
	gid = NULL; // makes gcc happy
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (eptr->version<VERSION2INT(2,0,0)) {
		opened = get8bit(&data);
		uid = get32bit(&data);
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	}
	anleng = get8bit(&data);
	if (length<23U+anleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETXATTR - wrong size (%"PRIu32":anleng=%"PRIu8")",length,anleng);
		eptr->mode = KILL;
		return;
	}
	attrname = data;
	data += anleng;
	avleng = get32bit(&data);
	if (length<23U+anleng+avleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETXATTR - wrong size (%"PRIu32":anleng=%"PRIu8":avleng=%"PRIu32")",length,anleng,avleng);
		eptr->mode = KILL;
		return;
	}
	attrvalue = data;
	data += avleng;
	mode = get8bit(&data);
	if (eptr->version>=VERSION2INT(2,0,0)) {
		opened = get8bit(&data);
		uid = get32bit(&data);
		if (length==23U+anleng+avleng) {
			gids = 1;
			gid = matoclserv_gid_storage(gids);
			gid[0] = get32bit(&data);
		} else {
			gids = get32bit(&data);
			if (gids==0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETXATTR - group ids missing");
				eptr->mode = KILL;
				return;
			}
			if (length!=23U+anleng+avleng+4*gids) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETXATTR - wrong size (%"PRIu32":anleng=%"PRIu8":avleng=%"PRIu32":gids=%"PRIu32")",length,anleng,avleng,gids);
				eptr->mode = KILL;
				return;
			}
			gid = matoclserv_gid_storage(gids);
			for (i=0 ; i<gids ; i++) {
				gid[i] = get32bit(&data);
			}
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (sessions_get_disables(eptr->sesdata)&DISABLE_SETXATTR) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_setxattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,opened,uid,gids,gid,anleng,attrname,avleng,attrvalue,mode);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_SETXATTR,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	sessions_inc_stats(eptr->sesdata,SES_OP_SETXATTR);
}

void matoclserv_fuse_getfacl(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
//	uint32_t uid,gids;
//	uint32_t *gid;
//	uint32_t i;
//	uint8_t opened;
	uint32_t msgid;
	uint8_t acltype;
	uint8_t *ptr;
	uint8_t status;
	void *c;
	uint16_t userperm;
	uint16_t groupperm;
	uint16_t otherperm;
	uint16_t mask;
	uint16_t namedusers;
	uint16_t namedgroups;
	uint32_t aclleng;
	if (length<9) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETFACL - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	acltype = get8bit(&data);
	if (length>9) { // just sanity length check
		if (length<18) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETFACL - wrong size (%"PRIu32")",length);
			eptr->mode = KILL;
			return;
		} else if (length>18) {
			uint32_t gids;
			data+=5;
			gids = get32bit(&data);
			if (gids==0) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETFACL - group ids missing");
				eptr->mode = KILL;
				return;
			}
			if (length!=18+4*gids) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETFACL - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
				eptr->mode = KILL;
				return;
			}
		}
	}
#if 0
	// for future use (rich acl)
	opened = get8bit(&data);
	uid = get32bit(&data);
	if (length==18) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (length!=18+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETFACL - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
#endif
	status = fs_getfacl_size(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,/*opened,uid,gids,gid,*/acltype,&c,&aclleng);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETFACL,(status!=MFS_STATUS_OK)?5:16+aclleng);
	if (status!=MFS_STATUS_OK) {
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	} else {
		fs_getfacl_data(c,&userperm,&groupperm,&otherperm,&mask,&namedusers,&namedgroups,ptr+16);
		put32bit(&ptr,msgid);
		put16bit(&ptr,userperm);
		put16bit(&ptr,groupperm);
		put16bit(&ptr,otherperm);
		put16bit(&ptr,mask);
		put16bit(&ptr,namedusers);
		put16bit(&ptr,namedgroups);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_GETFACL);
}

void matoclserv_fuse_setfacl(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t uid;
	uint32_t msgid;
	uint8_t acltype;
	uint16_t userperm;
	uint16_t groupperm;
	uint16_t otherperm;
	uint16_t mask;
	uint16_t namedusers;
	uint16_t namedgroups;
	uint8_t *ptr;
	uint8_t status;
	if (length<25) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETFACL - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	acltype = get8bit(&data);
	userperm = get16bit(&data);
	groupperm = get16bit(&data);
	otherperm = get16bit(&data);
	mask = get16bit(&data);
	namedusers = get16bit(&data);
	namedgroups = get16bit(&data);
	if (length!=(namedusers+namedgroups)*6U+25U) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETFACL - wrong size (%"PRIu32":namedusers=%"PRIu16":namedgroups=%"PRIu16")",length,namedusers,namedgroups);
		eptr->mode = KILL;
		return;
	}
//	uid = get32bit(&data);
//	gid = get32bit(&data);
//	sessions_ugid_remap(eptr->sesdata,&uid,&gid);
	if (sessions_get_disables(eptr->sesdata)&DISABLE_SETFACL) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_setfacl(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,acltype,userperm,groupperm,otherperm,mask,namedusers,namedgroups,data);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_SETFACL,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	sessions_inc_stats(eptr->sesdata,SES_OP_SETFACL);
}

void matoclserv_fuse_append_slice(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_src,uid,gids;
	uint32_t slice_from,slice_to;
	uint32_t *gid;
	uint32_t i;
	uint32_t msgid;
	uint64_t fleng;
	uint8_t *ptr;
	uint8_t status;
	uint8_t flags;
	if (length<20 || ((length&1)==1 && length<29)) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_APPEND_SLICE - wrong size (%"PRIu32"/20+|29+)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	if (length&1) {
		flags = get8bit(&data);
	} else {
		flags = 0;
	}
	inode = get32bit(&data);
	inode_src = get32bit(&data);
	if (length&1) {
		slice_from = get32bit(&data);
		slice_to = get32bit(&data);
	} else {
		slice_from = 0;
		slice_to = 0;
	}
	uid = get32bit(&data);
	if (length==20) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_APPEND_SLICE - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=20+4*gids && length!=29+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_APPEND_SLICE - wrong size (%"PRIu32":gids=%"PRIu32")",length,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	if (sessions_get_disables(eptr->sesdata)&DISABLE_APPENDCHUNKS) {
		status = MFS_ERROR_EPERM;
	} else {
		status = fs_append_slice(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),flags,inode,inode_src,slice_from,slice_to,uid,gids,gid,&fleng);
	}
	if (status==MFS_STATUS_OK) {
		matoclserv_fuse_fleng_has_changed(NULL,inode,fleng);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_APPEND_SLICE,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	sessions_inc_stats(eptr->sesdata,SES_OP_SNAPSHOT);
}

void matoclserv_fuse_snapshot(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inode_dst;
	uint8_t nleng_dst;
	const uint8_t *name_dst;
	uint32_t uid,gids;
	uint32_t *gid;
	uint32_t i;
	uint8_t smode;
	uint16_t requmask;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length<22) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SNAPSHOT - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	inode_dst = get32bit(&data);
	nleng_dst = get8bit(&data);
	if (length!=22U+nleng_dst && length<24U+nleng_dst) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SNAPSHOT - wrong size (%"PRIu32":nleng_dst=%"PRIu8")",length,nleng_dst);
		eptr->mode = KILL;
		return;
	}
	name_dst = data;
	data += nleng_dst;
	uid = get32bit(&data);
	if (length<=24U+nleng_dst) {
		gids = 1;
		gid = matoclserv_gid_storage(gids);
		gid[0] = get32bit(&data);
	} else {
		gids = get32bit(&data);
		if (gids==0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SNAPSHOT - group ids missing");
			eptr->mode = KILL;
			return;
		}
		if (length!=24U+nleng_dst+4*gids) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SNAPSHOT - wrong size (%"PRIu32":nleng_dst=%"PRIu8":gids=%"PRIu32")",length,nleng_dst,gids);
			eptr->mode = KILL;
			return;
		}
		gid = matoclserv_gid_storage(gids);
		for (i=0 ; i<gids ; i++) {
			gid[i] = get32bit(&data);
		}
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	smode = get8bit(&data);
	if (length>=24U+nleng_dst) {
		requmask = get16bit(&data);
	} else {
		smode &= ~SNAPSHOT_MODE_CPLIKE_ATTR;
		requmask = 0;
	}
	requmask |= sessions_get_umask(eptr->sesdata);
	status = MFS_STATUS_OK;
	if (smode & SNAPSHOT_MODE_DELETE) {
		if (sessions_get_disables(eptr->sesdata)&(DISABLE_UNLINK|DISABLE_RMDIR)) {
			status = MFS_ERROR_EPERM;
		}
	} else {
		if (sessions_get_disables(eptr->sesdata)&DISABLE_SNAPSHOT) {
			status = MFS_ERROR_EPERM;
		}
	}
	if (status==MFS_STATUS_OK) {
		status = fs_snapshot(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,inode_dst,nleng_dst,name_dst,uid,gids,gid,smode,requmask);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_SNAPSHOT,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
	sessions_inc_stats(eptr->sesdata,SES_OP_SNAPSHOT);
}

void matoclserv_fuse_quotacontrol(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t flags,del;
	uint32_t sinodes,hinodes,curinodes;
	uint64_t slength,ssize,srealsize,hlength,hsize,hrealsize,curlength,cursize,currealsize;
	uint32_t msgid,inode,graceperiod;
	uint8_t defaultgp;
	uint8_t *ptr;
	uint8_t status;
	if (length!=65 && length!=69 && length!=9) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_QUOTACONTROL - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	flags = get8bit(&data);
	if (length==65 || length==69) {
		if (length==69) {
			graceperiod = get32bit(&data);
		} else {
			graceperiod = 0;
		}
		sinodes = get32bit(&data);
		slength = get64bit(&data);
		ssize = get64bit(&data);
		srealsize = get64bit(&data);
		hinodes = get32bit(&data);
		hlength = get64bit(&data);
		hsize = get64bit(&data);
		hrealsize = get64bit(&data);
		del=0;
	} else {
		graceperiod = QUOTA_PERIOD_DONT_CHANGE;
		sinodes = 0;
		slength = 0;
		ssize = 0;
		srealsize = 0;
		hinodes = 0;
		hlength = 0;
		hsize = 0;
		hrealsize = 0;
		del = 1;
	}
	if (flags && sessions_is_root_remapped(eptr->sesdata)) {
		status = MFS_ERROR_EACCES;
	} else {
		status = fs_quotacontrol(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,del,&flags,&defaultgp,&graceperiod,&sinodes,&slength,&ssize,&srealsize,&hinodes,&hlength,&hsize,&hrealsize,&curinodes,&curlength,&cursize,&currealsize);
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_QUOTACONTROL,(status!=MFS_STATUS_OK)?5:(eptr->version>=VERSION2INT(4,51,0))?94:(eptr->version>=VERSION2INT(3,0,9))?93:89);
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put8bit(&ptr,flags);
		if (eptr->version>=VERSION2INT(4,51,0)) {
			put8bit(&ptr,defaultgp);
		}
		if (eptr->version>=VERSION2INT(3,0,9)) {
			put32bit(&ptr,graceperiod);
		}
		put32bit(&ptr,sinodes);
		put64bit(&ptr,slength);
		put64bit(&ptr,ssize);
		put64bit(&ptr,srealsize);
		put32bit(&ptr,hinodes);
		put64bit(&ptr,hlength);
		put64bit(&ptr,hsize);
		put64bit(&ptr,hrealsize);
		put32bit(&ptr,curinodes);
		put64bit(&ptr,curlength);
		put64bit(&ptr,cursize);
		put64bit(&ptr,currealsize);
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_META);
}

void matoclserv_fuse_archctl(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t cmd,status;
	uint8_t *ptr;
	if (length!=13 && length!=9) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_ARCHCTL - wrong size (%"PRIu32"/9|13)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	cmd = get8bit(&data);
	if (cmd==ARCHCTL_GET) {
		uint32_t archinodes,partinodes,notarchinodes;
		uint64_t archchunks,notarchchunks;
		if (length!=9) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_ARCHCTL (GET) - wrong size (%"PRIu32"/9)",length);
			eptr->mode = KILL;
			return;
		}
		status = fs_archget(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,&archchunks,&notarchchunks,&archinodes,&partinodes,&notarchinodes);
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_ARCHCTL,(status!=MFS_STATUS_OK)?5:32);
		put32bit(&ptr,msgid);
		if (status!=MFS_STATUS_OK) {
			put8bit(&ptr,status);
		} else {
			put64bit(&ptr,archchunks);
			put64bit(&ptr,notarchchunks);
			put32bit(&ptr,archinodes);
			put32bit(&ptr,partinodes);
			put32bit(&ptr,notarchinodes);
		}
	} else {
		uint32_t uid;
		uint64_t changed,notchanged;
		uint32_t notpermitted;
		if (length!=13) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_ARCHCTL (SET/CLR) - wrong size (%"PRIu32"/13)",length);
			eptr->mode = KILL;
			return;
		}
		uid = get32bit(&data);
		sessions_ugid_remap(eptr->sesdata,&uid,NULL);
		if (sessions_get_disables(eptr->sesdata)&DISABLE_SETEATTR) {
			status = MFS_ERROR_EPERM;
		} else {
			status = fs_archchg(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,cmd,&changed,&notchanged,&notpermitted);
		}
		ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_ARCHCTL,(status!=MFS_STATUS_OK)?5:24);
		put32bit(&ptr,msgid);
		if (status!=MFS_STATUS_OK) {
			put8bit(&ptr,status);
		} else {
			put64bit(&ptr,changed);
			put64bit(&ptr,notchanged);
			put32bit(&ptr,notpermitted);
		}
	}
	sessions_inc_stats(eptr->sesdata,SES_OP_META);
}

void matoclserv_sclass_create(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t nleng;
	uint8_t dleng;
	uint8_t constleng;
	uint8_t fver;
	uint8_t i;
	storagemode create,keep,arch,trash;
	uint32_t old_labelmasks[9*MASKORGROUP];
	uint8_t labels_mode;
	uint8_t export_group;
	uint32_t priority;
	uint8_t arch_mode;
	uint16_t arch_delay;
	uint16_t min_trashretention;
	uint64_t arch_min_size;
	uint8_t admin_only;
	const uint8_t *name;
	const uint8_t *desc;
	uint8_t *ptr;
	uint8_t status;

	if (length<5U) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CREATE - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_ADMIN) {
		nleng = get8bit(&data);
		name = data;
		data += nleng;
		if (length<6U+nleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CREATE - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
			eptr->mode = KILL;
			return;
		}
		fver = get8bit(&data);
		if (fver<=6) {
			// fver==1 and fver==2 are the same
			constleng = (fver>=6)?53:(fver>=5)?47:(fver>=4)?43:(fver>=3)?35:(fver>=1)?34:13;
			if (length<(uint32_t)(constleng+nleng)) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CREATE/%"PRIu8" - wrong size (%"PRIu32":nleng=%"PRIu8")",fver,length,nleng);
				eptr->mode = KILL;
				return;
			}
			memset(&create,0,sizeof(storagemode));
			memset(&keep,0,sizeof(storagemode));
			memset(&arch,0,sizeof(storagemode));
			memset(&trash,0,sizeof(storagemode));
			if (fver>=6) {
				dleng = get8bit(&data);
			} else {
				dleng = 0;
			}
			if (length<(uint32_t)(constleng+nleng+dleng)) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CREATE/%"PRIu8" - wrong size (%"PRIu32":nleng=%"PRIu8":dleng=%"PRIu8")",fver,length,nleng,dleng);
				eptr->mode = KILL;
				return;
			}
			if (fver>=6) {
				desc = data;
				data += dleng;
				priority = get32bit(&data);
				export_group = get8bit(&data);
			} else {
				desc = name;
				priority = 0;
				export_group = 0;
			}
			admin_only = get8bit(&data);
			labels_mode = get8bit(&data);
			if (fver>=3) {
				arch_mode = get8bit(&data);
			} else {
				arch_mode = SCLASS_ARCH_MODE_CTIME;
			}
			arch_delay = get16bit(&data);
			if (fver>=4) {
				arch_min_size = get64bit(&data);
			} else {
				arch_min_size = 0;
			}
			if (fver>=1) {
				min_trashretention = get16bit(&data);
				arch.ec_data_chksum_parts = get8bit(&data);
				trash.ec_data_chksum_parts = get8bit(&data);
				if (fver>=5) {
					create.labels_mode = get8bit(&data);
					keep.labels_mode = get8bit(&data);
					arch.labels_mode = get8bit(&data);
					trash.labels_mode = get8bit(&data);
				} else {
					create.labels_mode = LABELS_MODE_GLOBAL;
					keep.labels_mode = LABELS_MODE_GLOBAL;
					arch.labels_mode = LABELS_MODE_GLOBAL;
					trash.labels_mode = LABELS_MODE_GLOBAL;
				}
				create.uniqmask = get32bit(&data);
				keep.uniqmask = get32bit(&data);
				arch.uniqmask = get32bit(&data);
				trash.uniqmask = get32bit(&data);
			} else {
				min_trashretention = 0;
			}
			create.labelscnt = get8bit(&data);
			keep.labelscnt = get8bit(&data);
			arch.labelscnt = get8bit(&data);
			if (fver>=1) {
				trash.labelscnt = get8bit(&data);
				if (length!=(uint32_t)(constleng+nleng+dleng+(create.labelscnt+keep.labelscnt+arch.labelscnt+trash.labelscnt)*SCLASS_EXPR_MAX_SIZE)) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CREATE/%"PRIu8" - wrong size (%"PRIu32":nleng=%"PRIu8":dleng=%"PRIu8":labels C=%"PRIu8";K=%"PRIu8";A=%"PRIu8";T=%"PRIu8")",fver,length,nleng,dleng,create.labelscnt,keep.labelscnt,arch.labelscnt,trash.labelscnt);
					eptr->mode = KILL;
					return;
				}
				for (i=0 ; i<create.labelscnt ; i++) {
					memcpy(create.labelexpr[i],data,SCLASS_EXPR_MAX_SIZE);
					data+=SCLASS_EXPR_MAX_SIZE;
				}
				for (i=0 ; i<keep.labelscnt ; i++) {
					memcpy(keep.labelexpr[i],data,SCLASS_EXPR_MAX_SIZE);
					data+=SCLASS_EXPR_MAX_SIZE;
				}
				for (i=0 ; i<arch.labelscnt ; i++) {
					memcpy(arch.labelexpr[i],data,SCLASS_EXPR_MAX_SIZE);
					data+=SCLASS_EXPR_MAX_SIZE;
				}
				for (i=0 ; i<trash.labelscnt ; i++) {
					memcpy(trash.labelexpr[i],data,SCLASS_EXPR_MAX_SIZE);
					data+=SCLASS_EXPR_MAX_SIZE;
				}
			} else {
				arch_delay *= 24;
				trash.labelscnt = 0;
				if (length!=constleng+nleng+(create.labelscnt+keep.labelscnt+arch.labelscnt)*4U*MASKORGROUP) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CREATE/%"PRIu8" - wrong size (%"PRIu32":nleng=%"PRIu8":labels C=%"PRIu8";K=%"PRIu8";A=%"PRIu8")",fver,length,nleng,create.labelscnt,keep.labelscnt,arch.labelscnt);
					eptr->mode = KILL;
					return;
				}
				for (i=0 ; i<create.labelscnt*MASKORGROUP ; i++) {
					old_labelmasks[i] = get32bit(&data);
				}
				sclass_maskorgroup_to_labelexpr(create.labelexpr,old_labelmasks,create.labelscnt);
				for (i=0 ; i<keep.labelscnt*MASKORGROUP ; i++) {
					old_labelmasks[i] = get32bit(&data);
				}
				sclass_maskorgroup_to_labelexpr(keep.labelexpr,old_labelmasks,keep.labelscnt);
				for (i=0 ; i<arch.labelscnt*MASKORGROUP ; i++) {
					old_labelmasks[i] = get32bit(&data);
				}
				sclass_maskorgroup_to_labelexpr(arch.labelexpr,old_labelmasks,arch.labelscnt);
			}
			if ((arch.ec_data_chksum_parts&0xF)>1 || (trash.ec_data_chksum_parts&0xF)>1) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"CLTOMA_SCLASS_CREATE - redundancy levels > 1 supported only in pro version");
				status = MFS_ERROR_EINVAL;
			} else {
				status = sclass_create_entry(nleng,name,dleng,desc,priority,export_group,admin_only,labels_mode,arch_mode,arch_delay,arch_min_size,min_trashretention,&create,&keep,&arch,&trash);
			}
		} else {
			status = MFS_ERROR_EINVAL;
		}
	} else {
		status = MFS_ERROR_EPERM_NOTADMIN;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_SCLASS_CREATE,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_sclass_change(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t nleng;
	uint8_t dleng;
	uint8_t constleng;
	uint8_t fver;
	uint8_t i;
	uint16_t chgmask;
	storagemode create,keep,arch,trash;
	uint32_t create_labelmasks[9*MASKORGROUP];
	uint32_t keep_labelmasks[9*MASKORGROUP];
	uint32_t arch_labelmasks[9*MASKORGROUP];
	uint8_t labels_mode;
	uint8_t export_group;
	uint32_t priority;
	uint8_t arch_mode;
	uint16_t arch_delay;
	uint64_t arch_min_size;
	uint16_t min_trashretention;
	uint8_t admin_only;
	const uint8_t *name;
	uint8_t desc[MAXSCLASSDESCLENG];
	uint8_t *ptr;
	uint8_t status;

	labels_mode = 0;
	arch_mode = 0;
	arch_delay = 0;
	arch_min_size = 0;
	min_trashretention = 0;
	admin_only = 0;
	export_group = 0;
	dleng = 0;
	priority = 0;
	if (length<5U) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CHANGE - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	nleng = get8bit(&data);
	name = data;
	data += nleng;
	if (length<6U+nleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CHANGE - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
		eptr->mode = KILL;
		return;
	}
	fver = get8bit(&data);
//	arch_delay = 0; // make clang static analyzer happy
	if (fver<=6) {
		// fver==1 and fver==2 are the same
		constleng = (fver>=6)?55:(fver>=5)?49:(fver>=4)?45:(fver>=3)?37:(fver>=1)?36:15;
		if (length<(uint32_t)(constleng+nleng)) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CHANGE/%"PRIu8" - wrong size (%"PRIu32":nleng=%"PRIu8")",fver,length,nleng);
			eptr->mode = KILL;
			return;
		}
		memset(&create,0,sizeof(storagemode));
		memset(&keep,0,sizeof(storagemode));
		memset(&arch,0,sizeof(storagemode));
		memset(&trash,0,sizeof(storagemode));
		chgmask = get16bit(&data);
		if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_ADMIN || chgmask==0) {
			if (fver>=6) {
				dleng = get8bit(&data);
			} else {
				dleng = 0;
			}
			if (length<(uint32_t)(constleng+nleng+dleng)) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CHANGE/%"PRIu8" - wrong size (%"PRIu32":nleng=%"PRIu8":dleng=%"PRIu8")",fver,length,nleng,dleng);
				eptr->mode = KILL;
				return;
			}
			if (fver>=6) {
				memcpy(desc,data,dleng);
				data += dleng;
				priority = get32bit(&data);
				export_group = get8bit(&data);
			} else {
				dleng = 0;
				priority = 0;
				export_group = 0;
			}
			admin_only = get8bit(&data);
			labels_mode = get8bit(&data);
			if (fver>=3) {
				arch_mode = get8bit(&data);
			} else {
				arch_mode = SCLASS_ARCH_MODE_CTIME;
			}
			arch_delay = get16bit(&data);
			if (fver>=4) {
				arch_min_size = get64bit(&data);
			} else {
				arch_min_size = 0;
			}
			if (fver>=1) {
				min_trashretention = get16bit(&data);
				arch.ec_data_chksum_parts = get8bit(&data);
				trash.ec_data_chksum_parts = get8bit(&data);
				if (fver>=5) {
					create.labels_mode = get8bit(&data);
					keep.labels_mode = get8bit(&data);
					arch.labels_mode = get8bit(&data);
					trash.labels_mode = get8bit(&data);
				} else {
					create.labels_mode = LABELS_MODE_GLOBAL;
					keep.labels_mode = LABELS_MODE_GLOBAL;
					arch.labels_mode = LABELS_MODE_GLOBAL;
					trash.labels_mode = LABELS_MODE_GLOBAL;
				}
				create.uniqmask = get32bit(&data);
				keep.uniqmask = get32bit(&data);
				arch.uniqmask = get32bit(&data);
				trash.uniqmask = get32bit(&data);
			}
			create.labelscnt = get8bit(&data);
			keep.labelscnt = get8bit(&data);
			arch.labelscnt = get8bit(&data);
			if (fver>=1) {
				trash.labelscnt = get8bit(&data);
				if (length!=(uint32_t)(constleng+nleng+dleng+(create.labelscnt+keep.labelscnt+arch.labelscnt+trash.labelscnt)*SCLASS_EXPR_MAX_SIZE)) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CHANGE/%"PRIu8" - wrong size (%"PRIu32":nleng=%"PRIu8":dleng=%"PRIu8":labels C=%"PRIu8";K=%"PRIu8";A=%"PRIu8";T=%"PRIu8")",fver,length,nleng,dleng,create.labelscnt,keep.labelscnt,arch.labelscnt,trash.labelscnt);
					eptr->mode = KILL;
					return;
				}
				for (i=0 ; i<create.labelscnt ; i++) {
					memcpy(create.labelexpr[i],data,SCLASS_EXPR_MAX_SIZE);
					data+=SCLASS_EXPR_MAX_SIZE;
				}
				for (i=0 ; i<keep.labelscnt ; i++) {
					memcpy(keep.labelexpr[i],data,SCLASS_EXPR_MAX_SIZE);
					data+=SCLASS_EXPR_MAX_SIZE;
				}
				for (i=0 ; i<arch.labelscnt ; i++) {
					memcpy(arch.labelexpr[i],data,SCLASS_EXPR_MAX_SIZE);
					data+=SCLASS_EXPR_MAX_SIZE;
				}
				for (i=0 ; i<trash.labelscnt ; i++) {
					memcpy(trash.labelexpr[i],data,SCLASS_EXPR_MAX_SIZE);
					data+=SCLASS_EXPR_MAX_SIZE;
				}
			} else {
				arch_delay *= 24;
				if (length!=constleng+nleng+(create.labelscnt+keep.labelscnt+arch.labelscnt)*4U*MASKORGROUP) {
					mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_CHANGE/%"PRIu8" - wrong size (%"PRIu32":nleng=%"PRIu8":labels C=%"PRIu8";K=%"PRIu8";A=%"PRIu8")",fver,length,nleng,create.labelscnt,keep.labelscnt,arch.labelscnt);
					eptr->mode = KILL;
					return;
				}
				for (i=0 ; i<create.labelscnt*MASKORGROUP ; i++) {
					create_labelmasks[i] = get32bit(&data);
				}
				sclass_maskorgroup_to_labelexpr(create.labelexpr,create_labelmasks,create.labelscnt);
				for (i=0 ; i<keep.labelscnt*MASKORGROUP ; i++) {
					keep_labelmasks[i] = get32bit(&data);
				}
				sclass_maskorgroup_to_labelexpr(keep.labelexpr,keep_labelmasks,keep.labelscnt);
				for (i=0 ; i<arch.labelscnt*MASKORGROUP ; i++) {
					arch_labelmasks[i] = get32bit(&data);
				}
				sclass_maskorgroup_to_labelexpr(arch.labelexpr,arch_labelmasks,arch.labelscnt);
			}
			if ((arch.ec_data_chksum_parts&0xF)>1 || (trash.ec_data_chksum_parts&0xF)>1) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"CLTOMA_SCLASS_CHANGE - redundancy levels > 1 supported only in pro version");
				status = MFS_ERROR_EINVAL;
			} else {
				status = sclass_change_entry(nleng,name,chgmask,&dleng,desc,&priority,&export_group,&admin_only,&labels_mode,&arch_mode,&arch_delay,&arch_min_size,&min_trashretention,&create,&keep,&arch,&trash);
			}
		} else {
			status = MFS_ERROR_EPERM_NOTADMIN;
		}
	} else {
		status = MFS_ERROR_EINVAL;
	}
	if (fver==0) {
		if (status==MFS_STATUS_OK) {
			if (trash.labelscnt>0) {
				status = MFS_ERROR_INCOMPATVERSION;
			}
			if (sclass_labelexpr_to_maskorgroup(create_labelmasks,create.labelexpr,create.labelscnt)==0) {
				status = MFS_ERROR_INCOMPATVERSION;
			}
			if (sclass_labelexpr_to_maskorgroup(keep_labelmasks,keep.labelexpr,keep.labelscnt)==0) {
				status = MFS_ERROR_INCOMPATVERSION;
			}
			if (sclass_labelexpr_to_maskorgroup(arch_labelmasks,arch.labelexpr,arch.labelscnt)==0) {
				status = MFS_ERROR_INCOMPATVERSION;
			}
			if ((arch_delay%24)!=0) {
				status = MFS_ERROR_INCOMPATVERSION;
			}
			if ((arch.ec_data_chksum_parts | trash.ec_data_chksum_parts | create.uniqmask | keep.uniqmask | arch.uniqmask | trash.uniqmask | min_trashretention) > 0) {
				status = MFS_ERROR_INCOMPATVERSION;
			}
		}
		constleng = 12;
		ptr = matoclserv_create_packet(eptr,MATOCL_SCLASS_CHANGE,(status!=MFS_STATUS_OK)?5:(constleng+4U*MASKORGROUP*(create.labelscnt+keep.labelscnt+arch.labelscnt)));
	} else {
		constleng = (fver>=6)?52:(fver>=5)?46:(fver>=4)?42:(fver>=3)?34:33;
		ptr = matoclserv_create_packet(eptr,MATOCL_SCLASS_CHANGE,(status!=MFS_STATUS_OK)?5:(constleng+dleng+SCLASS_EXPR_MAX_SIZE*(create.labelscnt+keep.labelscnt+arch.labelscnt+trash.labelscnt)));
	}
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put8bit(&ptr,fver);
		if (fver>=6) {
			put8bit(&ptr,dleng);
			if (dleng>0) {
				memcpy(ptr,desc,dleng);
				ptr += dleng;
			}
			put32bit(&ptr,priority);
			put8bit(&ptr,export_group);
		}
		put8bit(&ptr,admin_only);
		put8bit(&ptr,labels_mode);
		if (fver>=3) {
			put8bit(&ptr,arch_mode);
		}
		if (fver>=1) {
			put16bit(&ptr,arch_delay);
			if (fver>=4) {
				put64bit(&ptr,arch_min_size);
			}
			put16bit(&ptr,min_trashretention);
			put8bit(&ptr,arch.ec_data_chksum_parts);
			put8bit(&ptr,trash.ec_data_chksum_parts);
			if (fver>=5) {
				put8bit(&ptr,create.labels_mode);
				put8bit(&ptr,keep.labels_mode);
				put8bit(&ptr,arch.labels_mode);
				put8bit(&ptr,trash.labels_mode);
			}
			put32bit(&ptr,create.uniqmask);
			put32bit(&ptr,keep.uniqmask);
			put32bit(&ptr,arch.uniqmask);
			put32bit(&ptr,trash.uniqmask);
		} else {
			put16bit(&ptr,arch_delay/24);
		}
		put8bit(&ptr,create.labelscnt);
		put8bit(&ptr,keep.labelscnt);
		put8bit(&ptr,arch.labelscnt);
		if (fver>=1) {
			put8bit(&ptr,trash.labelscnt);
			for (i=0 ; i<create.labelscnt ; i++) {
				memcpy(ptr,create.labelexpr[i],SCLASS_EXPR_MAX_SIZE);
				ptr+=SCLASS_EXPR_MAX_SIZE;
			}
			for (i=0 ; i<keep.labelscnt ; i++) {
				memcpy(ptr,keep.labelexpr[i],SCLASS_EXPR_MAX_SIZE);
				ptr+=SCLASS_EXPR_MAX_SIZE;
			}
			for (i=0 ; i<arch.labelscnt ; i++) {
				memcpy(ptr,arch.labelexpr[i],SCLASS_EXPR_MAX_SIZE);
				ptr+=SCLASS_EXPR_MAX_SIZE;
			}
			for (i=0 ; i<trash.labelscnt ; i++) {
				memcpy(ptr,trash.labelexpr[i],SCLASS_EXPR_MAX_SIZE);
				ptr+=SCLASS_EXPR_MAX_SIZE;
			}
		} else {
			for (i = 0 ; i < create.labelscnt*MASKORGROUP ; i++) {
				put32bit(&ptr,create_labelmasks[i]);
			}
			for (i = 0 ; i < keep.labelscnt*MASKORGROUP ; i++) {
				put32bit(&ptr,keep_labelmasks[i]);
			}
			for (i = 0 ; i < arch.labelscnt*MASKORGROUP ; i++) {
				put32bit(&ptr,arch_labelmasks[i]);
			}
		}
	}
}

void matoclserv_sclass_delete(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t nleng;
	const uint8_t *name;
	uint8_t *ptr;
	uint8_t status;

	if (length<5) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_DELETE - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_ADMIN) {
		nleng = get8bit(&data);
		name = data;
		data += nleng;
		if (length!=5U+nleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_DELETE - wrong size (%"PRIu32":nleng=%"PRIu8")",length,nleng);
			eptr->mode = KILL;
			return;
		}
		status = sclass_delete_entry(nleng,name);
	} else {
		status = MFS_ERROR_EPERM_NOTADMIN;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_SCLASS_DELETE,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_sclass_duplicate(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t snleng,dnleng;
	const uint8_t *sname,*dname;
	uint8_t *ptr;
	uint8_t status;

	if (length<5) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_DUPLICATE - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_ADMIN) {
		snleng = get8bit(&data);
		sname = data;
		data += snleng;
		if (length<6U+snleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_DUPLICATE - wrong size (%"PRIu32":snleng=%"PRIu8")",length,snleng);
			eptr->mode = KILL;
			return;
		}
		dnleng = get8bit(&data);
		dname = data;
		data += dnleng;
		if (length!=6U+snleng+dnleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_DUPLICATE - wrong size (%"PRIu32":snleng=%"PRIu8":dnleng=%"PRIu8")",length,snleng,dnleng);
			eptr->mode = KILL;
			return;
		}
		status = sclass_duplicate_entry(snleng,sname,dnleng,dname);
	} else {
		status = MFS_ERROR_EPERM_NOTADMIN;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_SCLASS_DUPLICATE,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_sclass_rename(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t snleng,dnleng;
	const uint8_t *sname,*dname;
	uint8_t *ptr;
	uint8_t status;

	if (length<5) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_RENAME - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_ADMIN) {
		snleng = get8bit(&data);
		sname = data;
		data += snleng;
		if (length<6U+snleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_RENAME - wrong size (%"PRIu32":snleng=%"PRIu8")",length,snleng);
			eptr->mode = KILL;
			return;
		}
		dnleng = get8bit(&data);
		dname = data;
		data += dnleng;
		if (length!=6U+snleng+dnleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_RENAME - wrong size (%"PRIu32":snleng=%"PRIu8":dnleng=%"PRIu8")",length,snleng,dnleng);
			eptr->mode = KILL;
			return;
		}
		status = sclass_rename_entry(snleng,sname,dnleng,dname);
	} else {
		status = MFS_ERROR_EPERM_NOTADMIN;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_SCLASS_RENAME,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_sclass_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint32_t rsize;
	uint8_t fver;

	if (length!=5) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SCLASS_LIST - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	fver = get8bit(&data);
	rsize = sclass_list_entries(NULL,fver);
	ptr = matoclserv_create_packet(eptr,MATOCL_SCLASS_LIST,4+rsize);
	put32bit(&ptr,msgid);
	sclass_list_entries(ptr,fver);
}

void matoclserv_pattern_add(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t fver;
	uint8_t gnleng,scnleng;
	const uint8_t *gname,*scname;
	uint32_t euid,egid;
	uint16_t trashretention;
	uint8_t priority,omask,seteattr,clreattr;
	uint8_t *ptr;
	uint8_t status;

	if (length<21) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_ADD - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_ADMIN) {
		fver = get8bit(&data);
		if (fver!=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_ADD - wrong packet version (%"PRIu8")",fver);
			eptr->mode = KILL;
			return;
		}
		gnleng = get8bit(&data);
		if (length<21U+gnleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_ADD - wrong size (%"PRIu32":gnleng=%"PRIu8")",length,gnleng);
			eptr->mode = KILL;
			return;
		}
		gname = data;
		data += gnleng;
		euid = get32bit(&data);
		egid = get32bit(&data);
		priority = get8bit(&data);
		omask = get8bit(&data);
		scnleng = get8bit(&data);
		if (length!=21U+gnleng+scnleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_ADD - wrong size (%"PRIu32":gnleng=%"PRIu8":scnleng=%"PRIu8")",length,gnleng,scnleng);
			eptr->mode = KILL;
			return;
		}
		scname = data;
		data += scnleng;
		trashretention = get16bit(&data);
		seteattr = get8bit(&data);
		clreattr = get8bit(&data);
		status = patterns_add(gnleng,gname,euid,egid,priority,omask,scnleng,scname,trashretention,seteattr,clreattr);
	} else {
		status = MFS_ERROR_EPERM_NOTADMIN;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_PATTERN_ADD,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_pattern_delete(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t fver;
	uint8_t gnleng;
	const uint8_t *gname;
	uint32_t euid,egid;
	uint8_t *ptr;
	uint8_t status;

	if (length<14) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_DELETE - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	if (sessions_get_sesflags(eptr->sesdata)&SESFLAG_ADMIN) {
		fver = get8bit(&data);
		if (fver!=0) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_DELETE - wrong packet version (%"PRIu8")",fver);
			eptr->mode = KILL;
			return;
		}
		gnleng = get8bit(&data);
		if (length!=14U+gnleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_DELETE - wrong size (%"PRIu32":gnleng=%"PRIu8")",length,gnleng);
			eptr->mode = KILL;
			return;
		}
		gname = data;
		data += gnleng;
		euid = get32bit(&data);
		egid = get32bit(&data);
		status = patterns_delete(gnleng,gname,euid,egid);
	} else {
		status = MFS_ERROR_EPERM_NOTADMIN;
	}
	ptr = matoclserv_create_packet(eptr,MATOCL_PATTERN_DELETE,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_pattern_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t fver;
	uint8_t *ptr;
	uint32_t lsize;

	if (length!=5) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_LIST - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	fver = get8bit(&data);
	if (fver!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_LIST - wrong packet version (%"PRIu8")",fver);
		eptr->mode = KILL;
		return;
	}
	lsize = patterns_list(NULL);
	ptr = matoclserv_create_packet(eptr,MATOCL_PATTERN_LIST,4+lsize);
	put32bit(&ptr,msgid);
	patterns_list(ptr);
}

void matoclserv_pattern_info(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint32_t lsize;

	if (length!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_INFO - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	(void)data;
/* for future extensions
	fver = get8bit(&data);
	if (fver!=0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_PATTERN_INFO - wrong packet version (%"PRIu8")",fver);
		eptr->mode = KILL;
		return;
	}
*/
	lsize = patterns_list(NULL);
	ptr = matoclserv_create_packet(eptr,MATOCL_PATTERN_INFO,lsize);
	patterns_list(ptr);
}


void matoclserv_trash_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t partno,format;
	uint32_t uid,mints,maxts;
	uint8_t status;
	uint32_t dleng;
	uint8_t gnleng;
	const uint8_t *gname;
	uint8_t *ptr;

	if (length<18) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_TRASH_LIST - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	partno = get8bit(&data);
	format = get8bit(&data);
	uid = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	mints = get32bit(&data);
	maxts = get32bit(&data);
	if (length>18) {
		gnleng = get8bit(&data);
		if (length!=19U+gnleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_TRASH_LIST - wrong size (%"PRIu32";gnleng=%"PRIu8")",length,gnleng);
			eptr->mode = KILL;
			return;
		}
		gname = data;
		data += gnleng; // not necessary
	} else {
		gnleng = 0;
		gname = NULL;
	}
	dleng = fs_listtrash(partno,format,uid,mints,maxts,gnleng,gname,NULL);
	status = MFS_STATUS_OK; // for potential future use
	ptr = matoclserv_create_packet(eptr,MATOCL_TRASH_LIST,(status!=MFS_STATUS_OK)?5:(4+dleng));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_listtrash(partno,format,uid,mints,maxts,gnleng,gname,ptr);
	}
}

void matoclserv_trash_recover(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint32_t inode;
	uint32_t proot;
	uint32_t pleng;
	const uint8_t *path;
	uint16_t cumask;
	uint32_t uid,gids;
	uint32_t *gid;
	uint8_t copysgid;
	uint32_t i;
	uint32_t used_pleng;
	uint8_t used_path[MFS_PATH_MAX];
	uint8_t *ptr;
	uint8_t status;

	if (length<27U) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_TRASH_RECOVER - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	proot = get32bit(&data);
	pleng = get32bit(&data);
	if (length<27U+pleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_TRASH_RECOVER - wrong size (%"PRIu32":pleng=%"PRIu32")",length,pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	cumask = get16bit(&data);
	uid = get32bit(&data);
	gids = get32bit(&data);
	if (gids==0) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_TRASH_RECOVER - group ids missing");
		eptr->mode = KILL;
		return;
	}
	if (length!=27U+pleng+4*gids) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_TRASH_RECOVER - wrong size (%"PRIu32":pleng=%"PRIu32":gids=%"PRIu32")",length,pleng,gids);
		eptr->mode = KILL;
		return;
	}
	gid = matoclserv_gid_storage(gids);
	for (i=0 ; i<gids ; i++) {
		gid[i] = get32bit(&data);
	}
	sessions_ugid_remap(eptr->sesdata,&uid,gid);
	copysgid = get8bit(&data);
	status = fs_trash_recover(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,proot,pleng,path,cumask,uid,gids,gid,copysgid,&used_pleng,used_path);
	if (status!=MFS_STATUS_OK || used_pleng==0) {
		ptr = matoclserv_create_packet(eptr,MATOCL_TRASH_RECOVER,5);
		put32bit(&ptr,msgid);
		put8bit(&ptr,status);
	} else {
		ptr = matoclserv_create_packet(eptr,MATOCL_TRASH_RECOVER,8+used_pleng);
		put32bit(&ptr,msgid);
		put32bit(&ptr,used_pleng);
		memcpy(ptr,used_path,used_pleng);
	}
}

void matoclserv_trash_remove(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint32_t inode;
	uint32_t uid;
	uint8_t *ptr;
	uint8_t status;

	if (length!=12) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_TRASH_REMOVE - wrong size (%"PRIu32"/12)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	status = fs_trash_remove(sessions_get_sesflags(eptr->sesdata),inode,uid);
	ptr = matoclserv_create_packet(eptr,MATOCL_TRASH_REMOVE,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_sustained_list(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t partno;
	uint32_t uid;
	uint8_t status;
	uint32_t dleng;
	uint8_t gnleng;
	const uint8_t *gname;
	uint8_t *ptr;

	if (length<9) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SUSTAINED_LIST - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	partno = get8bit(&data);
	uid = get32bit(&data);
	sessions_ugid_remap(eptr->sesdata,&uid,NULL);
	if (length>9) {
		gnleng = get8bit(&data);
		if (length!=10U+gnleng) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_SUSTAINED_LIST - wrong size (%"PRIu32";gnleng=%"PRIu8")",length,gnleng);
			eptr->mode = KILL;
			return;
		}
		gname = data;
		data += gnleng; // pro forma
	} else {
		gnleng = 0;
		gname = NULL;
	}
	dleng = fs_listsustained(partno,uid,gnleng,gname,NULL);
	status = MFS_STATUS_OK;
	ptr = matoclserv_create_packet(eptr,MATOCL_SUSTAINED_LIST,(status!=MFS_STATUS_OK)?5:(4+dleng));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_listsustained(partno,uid,gnleng,gname,ptr);
	}
}

/*
void matoclserv_fuse_eattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t mode,eattr,fneattr;
	uint32_t msgid,inode,uid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=14) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_EATTR - wrong size (%"PRIu32")",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	uid = get32bit(&data);
	mode = get8bit(&data);
	eattr = get8bit(&data);
	status = fs_eattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,uid,mode,&eattr,&fneattr);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_EATTR,(status!=MFS_STATUS_OK)?5:6);
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put8bit(&ptr,eattr);
		put8bit(&ptr,fneattr);
	}
}
*/

void matoclserv_fuse_getdirstats_old(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inodes,files,dirs,chunks;
	uint64_t leng,size,rsize;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETDIRSTATS - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_dir_stats(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,&inodes,&dirs,&files,&chunks,&leng,&size,&rsize);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETDIRSTATS,(status!=MFS_STATUS_OK)?5:60);
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,inodes);
		put32bit(&ptr,dirs);
		put32bit(&ptr,files);
		put32bit(&ptr,0);
		put32bit(&ptr,0);
		put32bit(&ptr,chunks);
		put32bit(&ptr,0);
		put32bit(&ptr,0);
		put64bit(&ptr,leng);
		put64bit(&ptr,size);
		put64bit(&ptr,rsize);
	}
}

void matoclserv_fuse_getdirstats(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode,inodes,files,dirs,chunks;
	uint64_t leng,size,rsize;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETDIRSTATS - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_get_dir_stats(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,&inodes,&dirs,&files,&chunks,&leng,&size,&rsize);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETDIRSTATS,(status!=MFS_STATUS_OK)?5:44);
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,inodes);
		put32bit(&ptr,dirs);
		put32bit(&ptr,files);
		put32bit(&ptr,chunks);
		put64bit(&ptr,leng);
		put64bit(&ptr,size);
		put64bit(&ptr,rsize);
	}
}

void matoclserv_fuse_gettrash(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	uint32_t tid;
	if (length!=4 && length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETTRASH - wrong size (%"PRIu32"/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	if (length==8) {
		tid = get32bit(&data);
	} else {
		tid = 0xFFFFFFFF;
	}
	status = fs_readtrash_size(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),tid,&dleng);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETTRASH,(status!=MFS_STATUS_OK)?5:(4+dleng));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_readtrash_data(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),tid,ptr);
	}
}

void matoclserv_fuse_getdetachedattr(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint8_t attr[ATTR_RECORD_SIZE];
	uint32_t msgid;
	uint8_t dtype;
	uint8_t *ptr;
	uint8_t status;
	if (length<8 || length>9) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETDETACHEDATTR - wrong size (%"PRIu32"/8,9)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	if (length==9) {
		dtype = get8bit(&data);
	} else {
		dtype = DTYPE_UNKNOWN;
	}
	status = fs_getdetachedattr(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,attr,dtype);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETDETACHEDATTR,(status!=MFS_STATUS_OK)?5:(eptr->asize+4));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		memcpy(ptr,attr,eptr->asize);
	}
}

void matoclserv_fuse_gettrashpath(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t pleng;
	const uint8_t *path;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETTRASHPATH - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_gettrashpath(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,&pleng,&path);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETTRASHPATH,(status!=MFS_STATUS_OK)?5:8+pleng+1);
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		put32bit(&ptr,pleng+1);
		if (pleng>0) {
			memcpy(ptr,path,pleng);
		}
		ptr[pleng]=0;
	}
}

void matoclserv_fuse_settrashpath(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	const uint8_t *path;
	uint32_t pleng;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length<12) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETTRASHPATH - wrong size (%"PRIu32"/>=12)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	pleng = get32bit(&data);
	if (length!=12+pleng) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_SETTRASHPATH - wrong size (%"PRIu32"/%"PRIu32")",length,12+pleng);
		eptr->mode = KILL;
		return;
	}
	path = data;
	data += pleng;
	while (pleng>0 && path[pleng-1]==0) {
		pleng--;
	}
	status = fs_settrashpath(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode,pleng,path);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_SETTRASHPATH,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_undel(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t status;
	uint8_t *ptr;
	if (length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_UNDEL - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_undel(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_UNDEL,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}

void matoclserv_fuse_purge(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t inode;
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	if (length!=8) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_PURGE - wrong size (%"PRIu32"/8)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	inode = get32bit(&data);
	status = fs_purge(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),inode);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_PURGE,5);
	put32bit(&ptr,msgid);
	put8bit(&ptr,status);
}


void matoclserv_fuse_getsustained(matoclserventry *eptr,const uint8_t *data,uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint8_t status;
	uint32_t dleng;
	if (length!=4) {
		mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"CLTOMA_FUSE_GETSUSTAINED - wrong size (%"PRIu32"/4)",length);
		eptr->mode = KILL;
		return;
	}
	msgid = get32bit(&data);
	status = fs_readsustained_size(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),&dleng);
	ptr = matoclserv_create_packet(eptr,MATOCL_FUSE_GETSUSTAINED,(status!=MFS_STATUS_OK)?5:(4+dleng));
	put32bit(&ptr,msgid);
	if (status!=MFS_STATUS_OK) {
		put8bit(&ptr,status);
	} else {
		fs_readsustained_data(sessions_get_rootinode(eptr->sesdata),sessions_get_sesflags(eptr->sesdata),ptr);
	}
}

uint32_t matoclserv_get_min_cl_version(void) {
	matoclserventry *eptr;
	uint32_t minver = 0;
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode==DATA && (eptr->registered!=NOTREGISTERED) && eptr->sesdata!=NULL) {
			if (minver==0 || eptr->version < minver) {
				minver = eptr->version;
			}
		}
	}
	return minver;
}

void matoclserv_beforedisconnect(matoclserventry *eptr) {
	swchunks *swc,**pswc;
	lwchunks *lwc,**plwc;
	uint32_t i;

	for (i=0 ; i<CHUNKHASHSIZE ; i++) {
		pswc = swchunkshash + i;
		while ((swc = *pswc)) {
			if (swc->eptr == eptr) {
				fs_rollback(swc->inode,swc->indx,swc->prevchunkid,swc->chunkid);
				*pswc = swc->next;
				free(swc);
			} else {
				pswc = &(swc->next);
			}
		}
		plwc = lwchunkshashhead + i;
		while ((lwc = *plwc)) {
			if (lwc->eptr == eptr) {
				*plwc = lwc->next;
				free(lwc);
			} else {
				plwc = &(lwc->next);
			}
		}
		lwchunkshashtail[i] = plwc;
	}
	if (eptr->path!=NULL) {
		free(eptr->path);
		eptr->path = NULL;
	}
	if (eptr->info!=NULL) {
		free(eptr->info);
		eptr->info = NULL;
	}
	if (eptr->strip!=NULL) {
		free(eptr->strip);
		eptr->strip = NULL;
	}
	sessions_disconnection(eptr->sesdata);
	posix_lock_disconnected(eptr);
	flock_disconnected(eptr);
}

/* HA Protocol Handlers */
void matoclserv_ha_cluster_info(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint32_t response_size;
	uint32_t node_count = 1; // For now, just report this node
	uint32_t shard_count;
	uint32_t i;
	uint32_t magic, request_type;
	
	/* Check for HA metadata download request (new protocol) */
	if (length == 8) {
		magic = get32bit(&data);
		request_type = get32bit(&data);
		
		if (magic == 0x12345678 && request_type == 1) {
			/* This is a metadata download request from cluster sync */
			mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, "received HA metadata download request from peer");
			
			/* Send acknowledgment */
			ptr = matoclserv_create_packet(eptr, MATOCL_HA_CLUSTER_INFO, 4);
			put32bit(&ptr, 0x12345678); /* Confirm we support HA transfer */
			
			/* Mark this connection for metadata transfer - don't kill it yet */
			eptr->mode = DATA;
			eptr->lastread = eptr->lastwrite = main_time();
			
			/* Schedule metadata transfer on this socket */
			meta_sendall(eptr->sock);
			
			/* The connection will be closed by meta_sendall's child process */
			return;
		}
	}
	
	if (!ha_mode_enabled()) {
		// Not in HA mode, send error
		if (length >= 4) {
			msgid = get32bit(&data);
		} else {
			msgid = 0;
		}
		ptr = matoclserv_create_packet(eptr, MATOCL_HA_CLUSTER_INFO, 5);
		put32bit(&ptr, msgid);
		put8bit(&ptr, ENOTSUP); // Not supported
		return;
	}
	
	if (length < 4) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "CLTOMA_HA_CLUSTER_INFO - wrong size (%"PRIu32"/4)", length);
		eptr->mode = KILL;
		return;
	}
	
	msgid = get32bit(&data);
	shard_count = shardmgr_get_shard_count();
	
	// Calculate response size: nodecount + shardcount + nodeinfo + shardmappings
	response_size = 4 + 4 + 4; // msgid + nodecount + shardcount
	response_size += 4 + 4 + 9 + 2 + 1; // nodeid + hostnamelen + "localhost" + port + status
	response_size += shard_count * 4; // shard mappings
	
	ptr = matoclserv_create_packet(eptr, MATOCL_HA_CLUSTER_INFO, response_size);
	put32bit(&ptr, msgid);
	put32bit(&ptr, node_count);
	put32bit(&ptr, shard_count);
	
	// Node information
	put32bit(&ptr, ha_get_node_id()); // node_id
	put32bit(&ptr, 9); // hostname length
	memcpy(ptr, "localhost", 9);
	ptr += 9;
	put16bit(&ptr, 9421); // port
	put8bit(&ptr, 1); // status: up
	
	// Shard mappings (all shards map to this node for now)
	for (i = 0; i < shard_count; i++) {
		put32bit(&ptr, 0); // All shards owned by this node
	}
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "HA cluster info sent to client");
}

void matoclserv_ha_node_status(matoclserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t msgid;
	uint8_t *ptr;
	uint32_t shard_count;
	uint32_t i;
	
	if (!ha_mode_enabled()) {
		// Not in HA mode, send error
		if (length >= 4) {
			msgid = get32bit(&data);
		} else {
			msgid = 0;
		}
		ptr = matoclserv_create_packet(eptr, MATOCL_HA_NODE_STATUS, 5);
		put32bit(&ptr, msgid);
		put8bit(&ptr, ENOTSUP); // Not supported
		return;
	}
	
	if (length < 4) {
		mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "CLTOMA_HA_NODE_STATUS - wrong size (%"PRIu32"/4)", length);
		eptr->mode = KILL;
		return;
	}
	
	msgid = get32bit(&data);
	shard_count = shardmgr_get_shard_count();
	
	ptr = matoclserv_create_packet(eptr, MATOCL_HA_NODE_STATUS, 4 + 4 + 1 + 4 + shard_count * 4);
	put32bit(&ptr, msgid);
	put32bit(&ptr, ha_get_node_id()); // nodeid
	put8bit(&ptr, 1); // status: up
	put32bit(&ptr, shard_count); // shardcount
	
	// List all shards owned by this node
	for (i = 0; i < shard_count; i++) {
		put32bit(&ptr, i); // shard ID
	}
	
	mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, "HA node status sent to client");
}

void matoclserv_gotpacket(matoclserventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	if (type==ANTOAN_NOP) {
		return;
	}
	if (type==ANTOAN_UNKNOWN_COMMAND) { // for future use
		return;
	}
	if (type==ANTOAN_BAD_COMMAND_SIZE) { // for future use
		return;
	}
//	printf("AQQ\n");
	if (eptr->registered==NOTREGISTERED) {	// unregistered clients - beware that in this context sesdata is NULL
		switch (type) {
			case ANTOAN_GET_VERSION:
				matoclserv_get_version(eptr,data,length);
				break;
			case ANTOAN_GET_CONFIG:
				matoclserv_get_config(eptr,data,length);
				break;
			case ANTOAN_GET_CONFIG_FILE:
				matoclserv_get_config_file(eptr,data,length);
				break;
			case ANTOMA_SYSLOG:
				matoclserv_syslog(eptr,data,length);
				break;
			case CLTOMA_FUSE_REGISTER:
//				printf("REGISTER\n");
				matoclserv_fuse_register(eptr,data,length);
				break;
			case CLTOMA_CSERV_LIST:
				matoclserv_cserv_list(eptr,data,length);
				break;
			case CLTOMA_SESSION_LIST:
				matoclserv_session_list(eptr,data,length);
				break;
			case CLTOAN_CHART:
				matoclserv_chart(eptr,data,length);
				break;
			case CLTOAN_CHART_DATA:
				matoclserv_chart_data(eptr,data,length);
				break;
			case CLTOAN_MONOTONIC_DATA:
				matoclserv_monotonic_data(eptr,data,length);
				break;
			case CLTOMA_INFO:
				matoclserv_info(eptr,data,length);
				break;
			case CLTOMA_FSTEST_INFO:
				matoclserv_fstest_info(eptr,data,length);
				break;
			case CLTOMA_CHUNKSTEST_INFO:
				matoclserv_chunkstest_info(eptr,data,length);
				break;
			case CLTOMA_CHUNKS_MATRIX:
				matoclserv_chunks_matrix(eptr,data,length);
				break;
			case CLTOMA_QUOTA_INFO:
				matoclserv_quota_info(eptr,data,length);
				break;
			case CLTOMA_EXPORTS_INFO:
				matoclserv_exports_info(eptr,data,length);
				break;
			case CLTOMA_MLOG_LIST:
				matoclserv_mlog_list(eptr,data,length);
				break;
			case CLTOMA_CSSERV_COMMAND:
				matoclserv_cserv_command(eptr,data,length);
				break;
			case CLTOMA_SESSION_COMMAND:
				matoclserv_session_command(eptr,data,length);
				break;
			case CLTOMA_MEMORY_INFO:
				matoclserv_memory_info(eptr,data,length);
				break;
			case CLTOAN_MODULE_INFO:
				matoclserv_module_info(eptr,data,length);
				break;
			case CLTOMA_LIST_OPEN_FILES:
				matoclserv_list_open_files(eptr,data,length);
				break;
			case CLTOMA_LIST_ACQUIRED_LOCKS:
				matoclserv_list_acquired_locks(eptr,data,length);
				break;
			case CLTOMA_MASS_RESOLVE_PATHS:
				matoclserv_mass_resolve_paths(eptr,data,length);
				break;
			case CLTOMA_SCLASS_INFO:
				matoclserv_sclass_info(eptr,data,length);
				break;
			case CLTOMA_PATTERN_INFO:
				matoclserv_pattern_info(eptr,data,length);
				break;
			case CLTOMA_MISSING_CHUNKS:
				matoclserv_missing_chunks(eptr,data,length);
				break;
			case CLTOMA_NODE_INFO:
				matoclserv_node_info(eptr,data,length);
				break;
			case CLTOMA_INSTANCE_NAME:
				matoclserv_instance_name(eptr,data,length);
				break;
			case CLTOMA_HA_CLUSTER_INFO:
				matoclserv_ha_cluster_info(eptr,data,length);
				break;
			case CLTOMA_HA_NODE_STATUS:
				matoclserv_ha_node_status(eptr,data,length);
				break;
			default:
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"main master server module: got unknown message from unregistered (type:%"PRIu32")",type);
				eptr->mode=KILL;
		}
	} else if (eptr->registered==REGISTERED) {	// mounts and new tools
		if (eptr->sesdata==NULL) {
			mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"registered connection without sesdata !!!");
			eptr->mode=KILL;
			return;
		}
		switch (type) {
			case ANTOAN_GET_VERSION:
				matoclserv_get_version(eptr,data,length);
				break;
			case ANTOAN_GET_CONFIG:
				matoclserv_get_config(eptr,data,length);
				break;
			case ANTOAN_GET_CONFIG_FILE:
				matoclserv_get_config_file(eptr,data,length);
				break;
			case ANTOMA_SYSLOG:
				matoclserv_syslog(eptr,data,length);
				break;
			case CLTOMA_FUSE_REGISTER:
				matoclserv_fuse_register(eptr,data,length);
				break;
			case CLTOMA_FUSE_SUSTAINED_INODES_DEPRECATED:
			case CLTOMA_FUSE_SUSTAINED_INODES:
				matoclserv_fuse_sustained_inodes(eptr,data,length);
				break;
			case CLTOMA_FUSE_AMTIME_INODES:
				matoclserv_fuse_amtime_inodes(eptr,data,length);
				break;
			case CLTOMA_FUSE_OPDATA:
				matoclserv_fuse_opdata(eptr,data,length);
				break;
			case CLTOMA_FUSE_WFLAGS:
				matoclserv_fuse_wflags(eptr,data,length);
				break;
			case CLTOMA_FUSE_TIME_SYNC:
				matoclserv_fuse_time_sync(eptr,data,length);
				break;
			case CLTOMA_PATH_LOOKUP:
				matoclserv_path_lookup(eptr,data,length);
				break;
			case CLTOMA_FUSE_STATFS:
				matoclserv_fuse_statfs(eptr,data,length);
				break;
			case CLTOMA_FUSE_ACCESS:
				matoclserv_fuse_access(eptr,data,length);
				break;
			case CLTOMA_FUSE_LOOKUP:
				matoclserv_fuse_lookup(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETATTR:
				matoclserv_fuse_getattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETATTR:
				matoclserv_fuse_setattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_READLINK:
				matoclserv_fuse_readlink(eptr,data,length);
				break;
			case CLTOMA_FUSE_SYMLINK:
				matoclserv_fuse_symlink(eptr,data,length);
				break;
			case CLTOMA_FUSE_MKNOD:
				matoclserv_fuse_mknod(eptr,data,length);
				break;
			case CLTOMA_FUSE_MKDIR:
				matoclserv_fuse_mkdir(eptr,data,length);
				break;
			case CLTOMA_FUSE_UNLINK:
				matoclserv_fuse_unlink(eptr,data,length);
				break;
			case CLTOMA_FUSE_RMDIR:
				matoclserv_fuse_rmdir(eptr,data,length);
				break;
			case CLTOMA_FUSE_RENAME:
				matoclserv_fuse_rename(eptr,data,length);
				break;
			case CLTOMA_FUSE_LINK:
				matoclserv_fuse_link(eptr,data,length);
				break;
			case CLTOMA_FUSE_READDIR:
				matoclserv_fuse_readdir(eptr,data,length);
				break;
			case CLTOMA_FUSE_OPEN:
				matoclserv_fuse_open(eptr,data,length);
				break;
			case CLTOMA_FUSE_CREATE:
				matoclserv_fuse_create(eptr,data,length);
				break;
			case CLTOMA_FUSE_READ_CHUNK:
				matoclserv_fuse_read_chunk(eptr,data,length);
				break;
			case CLTOMA_FUSE_WRITE_CHUNK:
				matoclserv_fuse_write_chunk(eptr,data,length);
				break;
			case CLTOMA_FUSE_WRITE_CHUNK_END:
				matoclserv_fuse_write_chunk_end(eptr,data,length);
				break;
			case CLTOMA_FUSE_FLOCK:
				matoclserv_fuse_flock(eptr,data,length);
				break;
			case CLTOMA_FUSE_POSIX_LOCK:
				matoclserv_fuse_posix_lock(eptr,data,length);
				break;
// fuse - meta
			case CLTOMA_FUSE_GETTRASH:
				matoclserv_fuse_gettrash(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETDETACHEDATTR:
				matoclserv_fuse_getdetachedattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETTRASHPATH:
				matoclserv_fuse_gettrashpath(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETTRASHPATH:
				matoclserv_fuse_settrashpath(eptr,data,length);
				break;
			case CLTOMA_FUSE_UNDEL:
				matoclserv_fuse_undel(eptr,data,length);
				break;
			case CLTOMA_FUSE_PURGE:
				matoclserv_fuse_purge(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETSUSTAINED:
				matoclserv_fuse_getsustained(eptr,data,length);
				break;
			case CLTOMA_FUSE_CHECK:
				matoclserv_fuse_check(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETTRASHRETENTION:
				matoclserv_fuse_gettrashretention(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETTRASHRETENTION:
				matoclserv_fuse_settrashretention(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETSCLASS:
				matoclserv_fuse_getsclass(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETSCLASS:
				matoclserv_fuse_setsclass(eptr,data,length);
				break;
			case CLTOMA_FUSE_APPEND_SLICE:
				matoclserv_fuse_append_slice(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETDIRSTATS:
				matoclserv_fuse_getdirstats(eptr,data,length);
				break;
			case CLTOMA_FUSE_TRUNCATE:
				matoclserv_fuse_truncate(eptr,data,length);
				break;
			case CLTOMA_FUSE_REPAIR:
				matoclserv_fuse_repair(eptr,data,length);
				break;
			case CLTOMA_FUSE_SNAPSHOT:
				matoclserv_fuse_snapshot(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETEATTR:
				matoclserv_fuse_geteattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETEATTR:
				matoclserv_fuse_seteattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_PARENTS:
				matoclserv_fuse_parents(eptr,data,length);
				break;
			case CLTOMA_FUSE_PATHS:
				matoclserv_fuse_paths(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETXATTR:
				matoclserv_fuse_getxattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETXATTR:
				matoclserv_fuse_setxattr(eptr,data,length);
				break;
			case CLTOMA_FUSE_GETFACL:
				matoclserv_fuse_getfacl(eptr,data,length);
				break;
			case CLTOMA_FUSE_SETFACL:
				matoclserv_fuse_setfacl(eptr,data,length);
				break;
			case CLTOMA_FUSE_QUOTACONTROL:
				matoclserv_fuse_quotacontrol(eptr,data,length);
				break;
			case CLTOMA_FUSE_ARCHCTL:
				matoclserv_fuse_archctl(eptr,data,length);
				break;
			case CLTOMA_SCLASS_CREATE:
				matoclserv_sclass_create(eptr,data,length);
				break;
			case CLTOMA_SCLASS_CHANGE:
				matoclserv_sclass_change(eptr,data,length);
				break;
			case CLTOMA_SCLASS_DELETE:
				matoclserv_sclass_delete(eptr,data,length);
				break;
			case CLTOMA_SCLASS_DUPLICATE:
				matoclserv_sclass_duplicate(eptr,data,length);
				break;
			case CLTOMA_SCLASS_RENAME:
				matoclserv_sclass_rename(eptr,data,length);
				break;
			case CLTOMA_SCLASS_LIST:
				matoclserv_sclass_list(eptr,data,length);
				break;
			case CLTOMA_PATTERN_ADD:
				matoclserv_pattern_add(eptr,data,length);
				break;
			case CLTOMA_PATTERN_DELETE:
				matoclserv_pattern_delete(eptr,data,length);
				break;
			case CLTOMA_PATTERN_LIST:
				matoclserv_pattern_list(eptr,data,length);
				break;
			case CLTOMA_TRASH_LIST:
				matoclserv_trash_list(eptr,data,length);
				break;
			case CLTOMA_TRASH_RECOVER:
				matoclserv_trash_recover(eptr,data,length);
				break;
			case CLTOMA_TRASH_REMOVE:
				matoclserv_trash_remove(eptr,data,length);
				break;
			case CLTOMA_SUSTAINED_LIST:
				matoclserv_sustained_list(eptr,data,length);
				break;
			case CLTOMA_CSERV_LIST:
				matoclserv_cserv_list(eptr,data,length);
				break;
			case CLTOMA_SESSION_LIST:
				matoclserv_session_list(eptr,data,length);
				break;
			case CLTOAN_CHART:
				matoclserv_chart(eptr,data,length);
				break;
			case CLTOAN_CHART_DATA:
				matoclserv_chart_data(eptr,data,length);
				break;
			case CLTOAN_MONOTONIC_DATA:
				matoclserv_monotonic_data(eptr,data,length);
				break;
			case CLTOMA_INFO:
				matoclserv_info(eptr,data,length);
				break;
			case CLTOMA_FSTEST_INFO:
				matoclserv_fstest_info(eptr,data,length);
				break;
			case CLTOMA_CHUNKSTEST_INFO:
				matoclserv_chunkstest_info(eptr,data,length);
				break;
			case CLTOMA_CHUNKS_MATRIX:
				matoclserv_chunks_matrix(eptr,data,length);
				break;
			case CLTOMA_QUOTA_INFO:
				matoclserv_quota_info(eptr,data,length);
				break;
			case CLTOMA_EXPORTS_INFO:
				matoclserv_exports_info(eptr,data,length);
				break;
			case CLTOMA_MLOG_LIST:
				matoclserv_mlog_list(eptr,data,length);
				break;
			case CLTOMA_CSSERV_COMMAND:
				matoclserv_cserv_command(eptr,data,length);
				break;
			case CLTOMA_SESSION_COMMAND:
				matoclserv_session_command(eptr,data,length);
				break;
			case CLTOMA_MEMORY_INFO:
				matoclserv_memory_info(eptr,data,length);
				break;
			case CLTOAN_MODULE_INFO:
				matoclserv_module_info(eptr,data,length);
				break;
			case CLTOMA_LIST_OPEN_FILES:
				matoclserv_list_open_files(eptr,data,length);
				break;
			case CLTOMA_LIST_ACQUIRED_LOCKS:
				matoclserv_list_acquired_locks(eptr,data,length);
				break;
			case CLTOMA_MASS_RESOLVE_PATHS:
				matoclserv_mass_resolve_paths(eptr,data,length);
				break;
			case CLTOMA_SCLASS_INFO:
				matoclserv_sclass_info(eptr,data,length);
				break;
			case CLTOMA_MISSING_CHUNKS:
				matoclserv_missing_chunks(eptr,data,length);
				break;
			case CLTOMA_NODE_INFO:
				matoclserv_node_info(eptr,data,length);
				break;
			case CLTOMA_FULL_DIRECTORY_DATA:
				matoclserv_full_directory_data(eptr,data,length);
				break;
			case CLTOMA_SET_ALL_NODE_ATTRIBUTES:
				matoclserv_set_all_node_attributes(eptr,data,length);
				break;
			case CLTOMA_INSTANCE_NAME:
				matoclserv_instance_name(eptr,data,length);
				break;
			case CLTOMA_HA_CLUSTER_INFO:
				matoclserv_ha_cluster_info(eptr,data,length);
				break;
			case CLTOMA_HA_NODE_STATUS:
				matoclserv_ha_node_status(eptr,data,length);
				break;
			default:
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"main master server module: got unknown message from mfsmount (type:%"PRIu32")",type);
				eptr->mode=KILL;
		}
	}
}

void matoclserv_read(matoclserventry *eptr,double now) {
	int32_t i;
	uint32_t type,leng;
	const uint8_t *ptr;
	uint32_t rbleng,rbpos;
	uint8_t err,hup,errmsg;
	static uint8_t *readbuff = NULL;
	static uint32_t readbuffsize = 0;

	if (eptr == NULL) {
		if (readbuff != NULL) {
			free(readbuff);
		}
		readbuff = NULL;
		readbuffsize = 0;
		return;
	}

	if (readbuffsize==0) {
		readbuffsize = 65536;
		readbuff = malloc(readbuffsize);
		passert(readbuff);
	}

	rbleng = 0;
	err = 0;
	hup = 0;
	errmsg = 0;
	for (;;) {
		i = read(eptr->sock,readbuff+rbleng,readbuffsize-rbleng);
		if (i==0) {
			hup = 1;
			break;
		} else if (i<0) {
			if (ERRNO_ERROR) {
				err = 1;
				errmsg = 1;
			}
			break;
		} else {
			stats_brcvd += i;
			rbleng += i;
			if (rbleng==readbuffsize) {
				readbuffsize*=2;
				readbuff = mfsrealloc(readbuff,readbuffsize);
				passert(readbuff);
			} else {
				break;
			}
		}
	}

	if (rbleng>0) {
		eptr->lastread = now;
	}

	rbpos = 0;
	while (rbpos<rbleng) {
		if ((rbleng-rbpos)>=eptr->input_bytesleft) {
			memcpy(eptr->input_startptr,readbuff+rbpos,eptr->input_bytesleft);
			i = eptr->input_bytesleft;
		} else {
			memcpy(eptr->input_startptr,readbuff+rbpos,rbleng-rbpos);
			i = rbleng-rbpos;
		}
		rbpos += i;
		eptr->input_startptr+=i;
		eptr->input_bytesleft-=i;

		if (eptr->input_bytesleft>0) {
			break;
		}

		if (eptr->input_packet == NULL) {
			ptr = eptr->input_hdr;
			type = get32bit(&ptr);
			leng = get32bit(&ptr);

			if (leng>MaxPacketSize) {
				mfs_log(MFSLOG_SYSLOG,MFSLOG_WARNING,"main master server module: packet too long (%"PRIu32"/%u) ; command:%"PRIu32,leng,MaxPacketSize,type);
				eptr->input_end = 1;
				return;
			}

			stats_prcvd++;
			eptr->input_packet = malloc(offsetof(in_packetstruct,data)+leng);
			passert(eptr->input_packet);
			eptr->input_packet->next = NULL;
			eptr->input_packet->type = type;
			eptr->input_packet->leng = leng;

			eptr->input_startptr = eptr->input_packet->data;
			eptr->input_bytesleft = leng;
		}

		if (eptr->input_bytesleft>0) {
			continue;
		}

		if (eptr->input_packet != NULL) {
			*(eptr->inputtail) = eptr->input_packet;
			eptr->inputtail = &(eptr->input_packet->next);
			eptr->input_packet = NULL;
			eptr->input_bytesleft = 8;
			eptr->input_startptr = eptr->input_hdr;
		}
	}

	if (hup) {
		if (eptr->registered!=NOTREGISTERED) {	// show this message only for standard, registered clients
			mfs_log(MFSLOG_SYSLOG,MFSLOG_NOTICE,"connection with client (ip:%s) has been closed by peer",eptr->strip);
		}
		eptr->input_end = 1;
	} else if (err) {
		if (errmsg) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"main master server module: client (ip:%s) read error",eptr->strip);
		}
		eptr->input_end = 1;
	}
}

void matoclserv_parse(matoclserventry *eptr) {
	in_packetstruct *ipack;
	uint64_t starttime;
	uint64_t currtime;

	starttime = monotonic_useconds();
	currtime = starttime;
	while (eptr->mode==DATA && (ipack = eptr->inputhead)!=NULL && starttime+10000>currtime) {
		matoclserv_gotpacket(eptr,ipack->type,ipack->data,ipack->leng);
		eptr->inputhead = ipack->next;
		free(ipack);
		if (eptr->inputhead==NULL) {
			eptr->inputtail = &(eptr->inputhead);
		} else {
			currtime = monotonic_useconds();
		}
	}
	if (eptr->mode==DATA && eptr->inputhead==NULL && eptr->input_end) {
		eptr->mode = KILL;
	}
}

void matoclserv_write(matoclserventry *eptr,double now) {
	out_packetstruct *opack;
	int32_t i;
#ifdef HAVE_WRITEV
	struct iovec iovtab[100];
	uint32_t iovdata;
	uint32_t leng;
	uint32_t left;

	for (;;) {
		leng = 0;
		for (iovdata=0,opack=eptr->outputhead ; iovdata<100 && opack!=NULL ; iovdata++,opack=opack->next) {
			iovtab[iovdata].iov_base = opack->startptr;
			iovtab[iovdata].iov_len = opack->bytesleft;
			leng += opack->bytesleft;
		}
		if (iovdata==0) {
			return;
		}
		i = writev(eptr->sock,iovtab,iovdata);
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"main master server module: client (ip:%s) write error",eptr->strip);
				eptr->mode = KILL;
			}
			return;
		}
		if (i>0) {
			eptr->lastwrite = now;
		}
		stats_bsent+=i;
		left = i;
		while (left>0 && eptr->outputhead!=NULL) {
			opack = eptr->outputhead;
			if (opack->bytesleft>left) {
				opack->startptr+=left;
				opack->bytesleft-=left;
				left = 0;
			} else {
				left -= opack->bytesleft;
				eptr->outputhead = opack->next;
				if (eptr->outputhead==NULL) {
					eptr->outputtail = &(eptr->outputhead);
				}
				free(opack);
				stats_psent++;
			}
		}
		if ((uint32_t)i < leng) {
			return;
		}
	}
#else
	for (;;) {
		opack = eptr->outputhead;
		if (opack==NULL) {
			return;
		}
		i=write(eptr->sock,opack->startptr,opack->bytesleft);
		if (i<0) {
			if (ERRNO_ERROR) {
				mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"main master server module: client (ip:%s) write error",eptr->strip);
				eptr->mode = KILL;
			}
			return;
		}
		if (i>0) {
			eptr->lastwrite = now;
		}
		opack->startptr+=i;
		opack->bytesleft-=i;
		stats_bsent+=i;
		if (opack->bytesleft>0) {
			return;
		}
		stats_psent++;
		eptr->outputhead = opack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(opack);
	}
#endif
}

void matoclserv_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	matoclserventry *eptr;

	pdesc[pos].fd = lsock;
	pdesc[pos].events = POLLIN;
	lsockpdescpos = pos;
	pos++;
//		FD_SET(lsock,rset);
//		max = lsock;
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		pdesc[pos].fd = eptr->sock;
		pdesc[pos].events = 0;
		eptr->pdescpos = pos;
//		i=eptr->sock;
		if (eptr->input_end==0) {
			pdesc[pos].events |= POLLIN;
//			FD_SET(i,rset);
//			if (i>max) {
//				max=i;
//			}
		}
		if (eptr->outputhead!=NULL) {
			pdesc[pos].events |= POLLOUT;
//			FD_SET(i,wset);
//			if (i>max) {
//				max=i;
//			}
		}
		pos++;
	}
	*ndesc = pos;
//	return max;
}

void matoclserv_disconnection_loop(void) {
	matoclserventry *eptr,**kptr;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;

	kptr = &matoclservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			matoclserv_beforedisconnect(eptr);
			tcpclose(eptr->sock);
			if (eptr->input_packet) {
				free(eptr->input_packet);
			}
			ipptr = eptr->inputhead;
			while (ipptr) {
				ipaptr = ipptr;
				ipptr = ipptr->next;
				free(ipaptr);
			}
			opptr = eptr->outputhead;
			while (opptr) {
				opaptr = opptr;
				opptr = opptr->next;
				free(opaptr);
			}
			*kptr = eptr->next;
			free(eptr);
		} else {
			kptr = &(eptr->next);
		}
	}
}

void matoclserv_serve(struct pollfd *pdesc) {
	double now;
	matoclserventry *eptr;
	int ns;
	static double lastaction = 0.0;
	double timeoutadd;

	now = monotonic_seconds();
// timeout fix
	if (lastaction>0.0) {
		timeoutadd = now-lastaction;
		if (timeoutadd>1.0) {
			for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
				eptr->lastread += timeoutadd;
			}
		}
	}
	lastaction = now;

	if (lsockpdescpos>=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
//	if (FD_ISSET(lsock,rset)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_WARNING,"main master server module: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(matoclserventry));
			passert(eptr);
			eptr->next = matoclservhead;
			matoclservhead = eptr;
			eptr->sock = ns;
			eptr->pdescpos = -1;
			tcpgetpeer(ns,&(eptr->peerip),NULL);
			eptr->strip = univallocstrip(eptr->peerip);
			eptr->registered = NOTREGISTERED;
			eptr->version = 0;
			eptr->asize = 0;
			eptr->mode = DATA;
			eptr->lastread = now;
			eptr->lastwrite = now;
			eptr->input_bytesleft = 8;
			eptr->input_startptr = eptr->input_hdr;
			eptr->input_end = 0;
			eptr->input_packet = NULL;
			eptr->inputhead = NULL;
			eptr->inputtail = &(eptr->inputhead);
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);
			if (ForceTimeout>0) {
				eptr->timeout = ForceTimeout;
			} else {
				eptr->timeout = DefaultTimeout;
			}

			eptr->path = NULL;
			eptr->info = NULL;
			eptr->ileng = 0;
			eptr->usepassword = 0;

			eptr->working_flags = 0;

			eptr->sesdata = NULL;
			memset(eptr->passwordrnd,0,32);
		}
	}

// read
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if ((pdesc[eptr->pdescpos].revents & (POLLERR|POLLIN))==POLLIN && eptr->mode!=KILL) {
				matoclserv_read(eptr,now);
			}
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->input_end = 1;
			}
		}
		matoclserv_parse(eptr);
	}

// write
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->lastwrite+1.0<now && eptr->outputhead==NULL) {
			uint8_t *ptr = matoclserv_create_packet(eptr,ANTOAN_NOP,4);	// 4 byte length because of 'msgid'
			*((uint32_t*)ptr) = 0;
		}
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outputhead)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				matoclserv_write(eptr,now);
			}
		}
		if (eptr->lastread+eptr->timeout<now) {
			eptr->mode = KILL;
		}
		if (eptr->mode==FINISH && eptr->outputhead==NULL) {
			eptr->mode = KILL;
		}
	}

	matoclserv_disconnection_loop();
}

void matoclserv_keep_alive(void) {
	double now;
	matoclserventry *eptr;

	now = monotonic_seconds();
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode == DATA && eptr->input_end==0) {
			matoclserv_read(eptr,now);
		}
	}
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->lastwrite+1.0<now && eptr->outputhead==NULL) {
			uint8_t *ptr = matoclserv_create_packet(eptr,ANTOAN_NOP,4);	// 4 byte length because of 'msgid'
			*((uint32_t*)ptr) = 0;
		}
		if (eptr->mode == DATA && eptr->outputhead) {
			matoclserv_write(eptr,now);
		}
	}
}

void matoclserv_close_lsock(void) { // after fork
	if (lsock>=0) {
		close(lsock);
	}
}

void matoclserv_term(void) {
	matoclserventry *eptr,*eaptr;
	in_packetstruct *ipptr,*ipaptr;
	out_packetstruct *opptr,*opaptr;
	swchunks *swc,*swcn;
	lwchunks *lwc,*lwcn;
	uint32_t i;

	mfs_log(MFSLOG_SYSLOG,MFSLOG_INFO,"main master server module: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	eptr = matoclservhead;
	while (eptr) {
		if (eptr->input_packet) {
			free(eptr->input_packet);
		}
		ipptr = eptr->inputhead;
		while (ipptr) {
			ipaptr = ipptr;
			ipptr = ipptr->next;
			free(ipaptr);
		}
		opptr = eptr->outputhead;
		while (opptr) {
			opaptr = opptr;
			opptr = opptr->next;
			free(opaptr);
		}
		eaptr = eptr;
		eptr = eptr->next;
		free(eaptr);
	}
	matoclservhead=NULL;

	for (i=0 ; i<CHUNKHASHSIZE ; i++) {
		for (swc = swchunkshash[i] ; swc ; swc = swcn) {
//			fs_rollback(swc->inode,swc->indx,swc->prevchunkid,swc->chunkid);
			swcn = swc->next;
			free(swc);
		}
		for (lwc = lwchunkshashhead[i] ; lwc ; lwc = lwcn) {
			lwcn = lwc->next;
			free(lwc);
		}
		swchunkshash[i] = NULL;
		lwchunkshashhead[i] = NULL;
		lwchunkshashtail[i] = NULL;
	}

	matoclserv_read(NULL,0.0); // free internal read buffer
	matoclserv_gid_storage(0); // free supplementary groups buffer

	free(ListenHost);
	free(ListenPort);
}

int matoclserv_no_more_pending_jobs(void) {
	matoclserventry *eptr;
	uint32_t i;
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		if (eptr->outputhead!=NULL) {
			return 0;
		}
	}
	for (i=0 ; i<CHUNKHASHSIZE ; i++) {
		if (swchunkshash[i]!=NULL) {
			return 0;
		}
	}
	return 1;
}

void matoclserv_disconnect_all(void) {
	matoclserventry *eptr;
	for (eptr=matoclservhead ; eptr ; eptr=eptr->next) {
		eptr->mode = KILL;
	}
	matoclserv_disconnection_loop();
}

void matoclserv_reload_common(void) {
	if (InstanceName!=NULL) {
		free(InstanceName);
	}
	InstanceName = cfg_getstr("INSTANCE_NAME","");
	InstanceNameLeng = strlen(InstanceName);
	if (InstanceNameLeng>255) {
		InstanceNameLeng=255;
	}

	RestrictIncompatibleClientVersions = cfg_getuint32("RESTRICT_INCOMPATIBLE_CLIENT_VERSIONS",1)?1:0;

	DefaultTimeout = cfg_getuint32("MATOCL_TIMEOUT",10);
	if (DefaultTimeout>65535) {
		DefaultTimeout=65535;
	} else if (DefaultTimeout<10) {
		DefaultTimeout=10;
	}

	ForceTimeout = cfg_getuint32("MATOCL_FORCE_TIMEOUT",0);
	if (ForceTimeout>0 && ForceTimeout<10) {
		ForceTimeout=10;
	}
	if (ForceTimeout>65535) {
		ForceTimeout=65535;
	}
}

void matoclserv_reload(void) {
	char *oldListenHost,*oldListenPort;
	uint32_t oldlistenip;
	uint16_t oldlistenport;
	int newlsock;

	matoclserv_reload_sessions();

	matoclserv_reload_common();

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	oldlistenip = listenip;
	oldlistenport = listenport;

	if (cfg_isdefined("MATOCL_LISTEN_HOST") || cfg_isdefined("MATOCL_LISTEN_PORT") || !(cfg_isdefined("MATOCU_LISTEN_HOST") || cfg_isdefined("MATOCU_LISTEN_PORT"))) {
		ListenHost = cfg_getstr("MATOCL_LISTEN_HOST","*");
		ListenPort = cfg_getstr("MATOCL_LISTEN_PORT",DEFAULT_MASTER_CLIENT_PORT);
	} else {
		ListenHost = cfg_getstr("MATOCU_LISTEN_HOST","*"); // deprecated option
		ListenPort = cfg_getstr("MATOCU_LISTEN_PORT",DEFAULT_MASTER_CLIENT_PORT); // deprecated option
	}
	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"main master server module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"main master server module: socket address has changed, but can't create new socket");
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	if (tcpresolve(ListenHost,ListenPort,&listenip,&listenport,1)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"main master server module: socket address has changed, but can't be resolved (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		listenip = oldlistenip;
		listenport = oldlistenport;
		tcpclose(newlsock);
		return;
	}
	if (tcpnumlisten(newlsock,listenip,listenport,100)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_WARNING,"main master server module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		listenip = oldlistenip;
		listenport = oldlistenport;
		tcpclose(newlsock);
		return;
	}
	if (tcpsetacceptfilter(newlsock)<0 && errno!=ENOTSUP) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"main master server module: can't set accept filter");
	}
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"main master server module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

int matoclserv_init(void) {
	InstanceName = NULL;
	InstanceNameLeng = 0;

	master_processid = time(NULL);
	master_processid <<= 32;
	master_processid |= random();

	matoclserv_reload_common();

	if (cfg_isdefined("MATOCL_LISTEN_HOST") || cfg_isdefined("MATOCL_LISTEN_PORT") || !(cfg_isdefined("MATOCU_LISTEN_HOST") || cfg_isdefined("MATOCU_LISTEN_HOST"))) {
		ListenHost = cfg_getstr("MATOCL_LISTEN_HOST","*");
		ListenPort = cfg_getstr("MATOCL_LISTEN_PORT",DEFAULT_MASTER_CLIENT_PORT);
	} else {
		fprintf(stderr,"change MATOCU_LISTEN_* option names to MATOCL_LISTEN_* !!!\n");
		ListenHost = cfg_getstr("MATOCU_LISTEN_HOST","*"); // deprecated option
		ListenPort = cfg_getstr("MATOCU_LISTEN_PORT",DEFAULT_MASTER_CLIENT_PORT); // deprecated option
	}

	CreateFirstChunk = 0;

	lsock = tcpsocket();
	if (lsock<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"main master server module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpresolve(ListenHost,ListenPort,&listenip,&listenport,1)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"main master server module: can't resolve %s:%s",ListenHost,ListenPort);
		return -1;
	}
	if (tcpnumlisten(lsock,listenip,listenport,100)<0) {
		mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR,MFSLOG_ERR,"main master server module: can't listen on %s:%s",ListenHost,ListenPort);
		return -1;
	}
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_NOTICE,"main master server module: can't set accept filter");
	}
	mfs_log(MFSLOG_SYSLOG_STDERR,MFSLOG_INFO,"main master server module: listen on %s:%s",ListenHost,ListenPort);

	matoclservhead = NULL;

	main_time_register(1,0,matoclserv_timeout_waiting_ops);
	main_reload_register(matoclserv_reload);
	main_destruct_register(matoclserv_term);
	main_poll_register(matoclserv_desc,matoclserv_serve);
	main_keepalive_register(matoclserv_keep_alive);
	main_time_register(10,0,matoclserv_broadcast_timeout);
	return 0;
}
