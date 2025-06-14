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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>

#include "crdtstore.h"
#include "raftconsensus.h"
#include "shardmgr.h"
#include "metasyncer.h"
#include "gossip.h"
#include "haconn.h"
#include "random.h"

#define MODULE_OPTIONS_GETOPT ""
#define MODULE_OPTIONS_SWITCH 
#define MODULE_OPTIONS_SYNOPIS ""
#define MODULE_OPTIONS_DESC ""

/* Module function prototypes */
int mfsha_init(void);
void mfsha_term(void);

/* Run Tab */
typedef int (*runfn)(void);
struct {
	runfn fn;
	char *name;
} RunTab[]={
	{rnd_init,"random generator"},
	{crdtstore_init,"CRDT store"},
	{raftconsensus_init,"Raft consensus"},
	{shardmgr_init,"Shard manager"},
	{gossip_init,"Gossip protocol"},
	{metasyncer_init,"Metadata synchronizer"},
	{haconn_init,"HA connections"},
	{mfsha_init,"mfsha main"},
	{(runfn)0,"****"}
},LateRunTab[]={
	{(runfn)0,"****"}
},RestoreRunTab[]={
	{(runfn)0,"****"}
};
