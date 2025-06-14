#!/usr/bin/env python3

# Copyright (C) 2025 Jakub Kruszona-Zawadzki, Saglabs SA
# 
# This file is part of MooseFS.
# 
# MooseFS is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 2 (only).
# 
# MooseFS is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with MooseFS; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02111-1301, USA
# or visit http://www.gnu.org/licenses/gpl-2.0.html

import os
import sys
cgimode = 1 if 'GATEWAY_INTERFACE' in os.environ else 0
if sys.version_info[0]<3 or (sys.version_info[0]==3 and sys.version_info[1]<4):
	if cgimode:
		print("Content-Type: text/html; charset=UTF-8")
		print("")
		print("""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">""")
		print("""<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">""")
		print("""<head>""")
		print("""<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />""")
		print("""<title>MFS Info</title>""")
		print("""</head>""")
		print("""<body>""")
		print("""<h2>MooseFS GUI: Unsupported python version, minimum required version is 3.4</h2>""")
		print("""</body>""")
		print("""</html>""")
	else:
		print("Unsupported python version, minimum required version is 3.4")
	sys.exit(1)


PROTO_BASE = 0

VERSION = "4.57.6"

# some constants from MFSCommunication.h
ANTOAN_NOP = 0

ANTOCS_CLEAR_ERRORS = (PROTO_BASE+306)
CSTOAN_CLEAR_ERRORS = (PROTO_BASE+307)

CLTOMA_CSERV_LIST = (PROTO_BASE+500)
MATOCL_CSERV_LIST = (PROTO_BASE+501)
CLTOAN_CHART_DATA = (PROTO_BASE+506)
ANTOCL_CHART_DATA = (PROTO_BASE+507)
CLTOMA_SESSION_LIST = (PROTO_BASE+508)
MATOCL_SESSION_LIST = (PROTO_BASE+509)
CLTOMA_INFO = (PROTO_BASE+510)
MATOCL_INFO = (PROTO_BASE+511)
CLTOMA_FSTEST_INFO = (PROTO_BASE+512)
MATOCL_FSTEST_INFO = (PROTO_BASE+513)
CLTOMA_CHUNKSTEST_INFO = (PROTO_BASE+514)
MATOCL_CHUNKSTEST_INFO = (PROTO_BASE+515)
CLTOMA_CHUNKS_MATRIX = (PROTO_BASE+516)
MATOCL_CHUNKS_MATRIX = (PROTO_BASE+517)
CLTOMA_QUOTA_INFO = (PROTO_BASE+518)
MATOCL_QUOTA_INFO = (PROTO_BASE+519)
CLTOMA_EXPORTS_INFO = (PROTO_BASE+520)
MATOCL_EXPORTS_INFO = (PROTO_BASE+521)
CLTOMA_MLOG_LIST = (PROTO_BASE+522)
MATOCL_MLOG_LIST = (PROTO_BASE+523)
CLTOMA_CSSERV_COMMAND = (PROTO_BASE+524)
MATOCL_CSSERV_COMMAND = (PROTO_BASE+525)
CLTOMA_SESSION_COMMAND = (PROTO_BASE+526)
MATOCL_SESSION_COMMAND = (PROTO_BASE+527)
CLTOMA_MEMORY_INFO = (PROTO_BASE+528)
MATOCL_MEMORY_INFO = (PROTO_BASE+529)
CLTOMA_LIST_OPEN_FILES = (PROTO_BASE+532)
MATOCL_LIST_OPEN_FILES = (PROTO_BASE+533)
CLTOMA_LIST_ACQUIRED_LOCKS = (PROTO_BASE+534)
MATOCL_LIST_ACQUIRED_LOCKS = (PROTO_BASE+535)
CLTOMA_MASS_RESOLVE_PATHS = (PROTO_BASE+536)
MATOCL_MASS_RESOLVE_PATHS = (PROTO_BASE+537)
CLTOMA_SCLASS_INFO = (PROTO_BASE+542)
MATOCL_SCLASS_INFO = (PROTO_BASE+543)
CLTOMA_MISSING_CHUNKS = (PROTO_BASE+544)
MATOCL_MISSING_CHUNKS = (PROTO_BASE+545)
CLTOMA_PATTERN_INFO = (PROTO_BASE+548)
MATOCL_PATTERN_INFO = (PROTO_BASE+549)
CLTOMA_INSTANCE_NAME = (PROTO_BASE+550)
MATOCL_INSTANCE_NAME = (PROTO_BASE+551)

CLTOCS_HDD_LIST = (PROTO_BASE+600)
CSTOCL_HDD_LIST = (PROTO_BASE+601)

MFS_MESSAGE = 1

FEATURE_EXPORT_UMASK        = 0
FEATURE_EXPORT_DISABLES     = 1
FEATURE_SESSION_STATS_28    = 2
FEATURE_INSTANCE_NAME       = 4
FEATURE_CSLIST_MODE         = 5
FEATURE_SCLASS_IN_MATRIX    = 7
FEATURE_DEFAULT_GRACEPERIOD = 8
FEATURE_LABELMODE_OVERRIDES = 9
FEATURE_SCLASSGROUPS        = 10

MASKORGROUP = 4
SCLASS_EXPR_MAX_SIZE = 128

UNIQ_MASK_IP = 1 << (1+ord('Z')-ord('A'))
UNIQ_MASK_RACK = 1 << (2+ord('Z')-ord('A'))

MFS_CSSERV_COMMAND_REMOVE         = 0
MFS_CSSERV_COMMAND_BACKTOWORK     = 1
MFS_CSSERV_COMMAND_MAINTENANCEON  = 2
MFS_CSSERV_COMMAND_MAINTENANCEOFF = 3
MFS_CSSERV_COMMAND_TMPREMOVE      = 4

MFS_SESSION_COMMAND_REMOVE = 0

PATTERN_EUGID_ANY            = 0xFFFFFFFF
PATTERN_OMASK_SCLASS         = 0x01
PATTERN_OMASK_TRASHRETENTION = 0x02
PATTERN_OMASK_EATTR          = 0x04

SCLASS_ARCH_MODE_CTIME      = 0x01
SCLASS_ARCH_MODE_MTIME      = 0x02
SCLASS_ARCH_MODE_ATIME      = 0x04
SCLASS_ARCH_MODE_REVERSIBLE = 0x08
SCLASS_ARCH_MODE_FAST       = 0x10
SCLASS_ARCH_MODE_CHUNK      = 0x20

STATUS_OK      = 0
ERROR_NOTFOUND = 41
ERROR_ACTIVE   = 42

STATE_DUMMY    = 0
STATE_LEADER   = 1
STATE_ELECT    = 2
STATE_FOLLOWER = 3
STATE_USURPER  = 4
STATE_DEPUTY   = 5

#CSTOCL_HDD_LIST flags constants
CS_HDD_MFR      = 0x01
CS_HDD_DAMAGED  = 0x02
CS_HDD_SCANNING = 0x04
CS_HDD_INVALID  = 0x08
CS_HDD_TOO_OLD  = 0x100 #too old cs to get info on its discs
CS_HDD_UNREACHABLE  = 0x200 #can't connect to cs and get info on its discs

MFRSTATUS_VALIDATING = 0 #Unknown state unknown after disconnect or creation or unknown, loop in progress
MFRSTATUS_INPROGRESS = 1 #chunks still needs to be replicated, can't be removed 
MFRSTATUS_READY      = 2 #can be removed, whole loop has passed

UNRESOLVED = "(unresolved)"

import math
import socket
import struct
import time
import traceback
import codecs
import json
import asyncio 
from datetime import datetime


ajax_request = False #is it ajax request?
readonly = False     #if readonly - don't render CGI links for commands
selectable = True    #if not selectable - don't render drop-downs and other selectors (to switch views with page reload)

try:
	xrange
except NameError:
	xrange = range

instancename = "My MooseFS"

def myunicode(x):
	return str(x)

def gettzoff(t):
	return time.localtime(t).tm_gmtoff

def shiftts(t):
	return t - gettzoff(t)

#parse parameters and auxiliary functions
if cgimode:
	# in CGI mode set default output encoding to utf-8 (our html page encoding)
	if sys.version_info[1]<7:
		sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
	else:
		sys.stdout.reconfigure(encoding='utf-8')

	try:
		import urllib.parse as xurllib
	except ImportError:
		import urllib as xurllib

	class MFSFieldStorage:
		def __init__(self):
			self.data = {}
		def __repr__(self):
			return repr(self.data)
		def __str__(self):
			return str(self.data)
		def __contains__(self,key):
			return key in self.data
		def __iter__(self):
			return iter(self.data.keys())
		def initFromURL(self):
			for k,v in xurllib.parse_qsl(os.environ["QUERY_STRING"]):
				if k in self.data:
					if type(self.data[k])==str:
						x = self.data[k]
						self.data[k] = [x]
					self.data[k].append(v)
				else:
					self.data[k] = v
		def getvalue(self,key):
			if key in self.data:
				return self.data[key]
			return None
		def pop(self,key):
			return self.data.pop(key)
		def append(self, key, value):
			self.data[key]=value

	fields = MFSFieldStorage()
	fields.initFromURL()		

	ajax_request = fields.getvalue("ajax")=="container"
	if fields.getvalue("ajax")!=None:
		fields.pop("ajax") #prevent from including ajax URL param any further

	try:
		if "masterhost" in fields:
			masterhost = fields.getvalue("masterhost")
			if type(masterhost) is list:
				masterhost = ";".join(masterhost)
			masterhost = masterhost.replace('"','').replace('<','').replace('>','').replace("'",'').replace('&','').replace('%','')
		else:
			masterhost = 'mfsmaster'
	except Exception:
		masterhost = 'mfsmaster'
	try:
		masterport = int(fields.getvalue("masterport"))
	except Exception:
		masterport = 9421
	try:
		mastercontrolport = int(fields.getvalue("mastercontrolport"))
	except Exception:
		try:
			mastercontrolport = int(fields.getvalue("masterport"))-2
		except Exception:
			mastercontrolport = 9419
	try:
		if "mastername" in fields:
			mastername = str(fields.getvalue("mastername"))
		else:
			mastername = 'MooseFS'
	except Exception:
		mastername = 'MooseFS'


	def htmlentities(str):
		return str.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace("'",'&apos;').replace('"','&quot;')

	def urlescape(str):
		return xurllib.quote_plus(str)

	def resolve(strip):
		try:
			return (socket.gethostbyaddr(strip))[0]
		except Exception:
			return UNRESOLVED

	def createhtmllink(update):
		c = []
		for k in fields:
			if k not in update:
				f = fields.getvalue(k)
				if type(f) is list:
					for el in f:
						c.append("%s=%s" % (k,urlescape(el)))
				elif type(f) is str:
					c.append("%s=%s" % (k,urlescape(f)))
		for k,v in update.items():
			if v!="":
				c.append("%s=%s" % (k,urlescape(v)))
		return "mfs.cgi?%s" % ("&amp;".join(c))

	def createrawlink(update):
		c = []
		for k in fields:
			if k not in update:
				f = fields.getvalue(k)
				if type(f) is list:
					for el in f:
						c.append("%s=%s" % (k,urlescape(el)))
				elif type(f) is str:
					c.append("%s=%s" % (k,urlescape(f)))
		for k,v in update.items():
			if v!="":
				c.append("%s=%s" % (k,urlescape(v)))
		return ("mfs.cgi?%s" % ("&".join(c))).replace('"','').replace("'","")

	def createorderlink(prefix,columnid):
		ordername = "%sorder" % prefix
		revname = "%srev" % prefix
		try:
			orderval = int(fields.getvalue(ordername))
		except Exception:
			orderval = 0
		try:
			revval = int(fields.getvalue(revname))
		except Exception:
			revval = 0
		return createhtmllink({revname:"1"}) if orderval==columnid and revval==0 else createhtmllink({ordername:str(columnid),revname:"0"})

	def createinputs(ignorefields):
		for k in fields:
			if k not in ignorefields:
				f = fields.getvalue(k)
				if type(f) is list:
					for el in f:
						yield """<input type="hidden" name="%s" value="%s">""" % (k,htmlentities(el))
				elif type(f) is str:
					yield """<input type="hidden" name="%s" value="%s">""" % (k,htmlentities(f))
		return

else: # CLI mode
	import getopt

	masterhost = 'mfsmaster'
	masterport = 9421
	mastercontrolport = 9419
	mastername = 'MooseFS'
	frameset = -1
	plaintextseparator = "\t"
	forceplaintext = 0
	jsonmode = 0
	colormode = 0
	donotresolve = 0
	sectionset = []
	sectionsubset = []
	clicommands = []

# order and data parameters
	ICsclassid = -1
	ICmatrix = 0
	IMorder = 0
	IMrev = 0
	MForder = 0
	MFrev = 0
	CSorder = 0
	CSrev = 0
	MBorder = 0
	MBrev = 0
	HDdata = ""
	HDorder = 0
	HDrev = 0
	HDperiod = 0
	HDtime = 0
	HDaddrname = 1
	EXorder = 0
	EXrev = 0
	MSorder = 0
	MSrev = 0
	SCorder = 0
	SCrev = 0
	SCdata = 1
	PAorder = 0
	PArev = 0
	OForder = 0
	OFrev = 0
	OFsessionid = 0
	ALorder = 0
	ALrev = 0
	ALinode = 0
	MOorder = 0
	MOrev = 0
	MOdata = 0
	QUorder = 0
	QUrev = 0
	MCrange = 0
	MCcount = 25
	MCchdata = []
	CCrange = 0
	CCcount = 25
	CCchdata = []

	time_s = time.time()

# modes:
#  0 - percent (sum - cpu)
#  1 - ops/s (operations)
#  2 - humanized format in bytes (memory/disk space)
#  3,4 - not used in master
#  5 - in MB/s (data bytes read/written)
#  6 - raw data (number of chunks/files etc.)
#  7 - seconds
#  8 - percent (max - udiff)
	mcchartslist = [
			('ucpu',0,0,'User cpu usage'),
			('scpu',1,0,'System cpu usage'),
			('delete',2,1,'Number of chunk deletion attempts'),
			('replicate',3,1,'Number of chunk replication attempts'),
			('statfs',4,1,'Number of statfs operations'),
			('getattr',5,1,'Number of getattr operations'),
			('setattr',6,1,'Number of setattr operations'),
			('lookup',7,1,'Number of lookup operations'),
			('mkdir',8,1,'Number of mkdir operations'),
			('rmdir',9,1,'Number of rmdir operations'),
			('symlink',10,1,'Number of symlink operations'),
			('readlink',11,1,'Number of readlink operations'),
			('mknod',12,1,'Number of mknod operations'),
			('unlink',13,1,'Number of unlink operations'),
			('rename',14,1,'Number of rename operations'),
			('link',15,1,'Number of link operations'),
			('readdir',16,1,'Number of readdir operations'),
			('open',17,1,'Number of open operations'),
			('read_chunk',18,1,'Number of chunk_read operations'),
			('write_chunk',19,1,'Number of chunk_write operations'),
			('memoryrss',20,2,'Resident memory usage'),
			('prcvd',21,1,'Packets received by master'),
			('psent',22,1,'Packets sent by master'),
			('brcvd',23,5,'Bytes received by master'),
			('bsent',24,5,'Bytes sent by master'),
			('memoryvirt',25,2,'Virtual memory usage'),
			('usedspace',26,2,'RAW disk space usage'),
			('totalspace',27,2,'RAW disk space connected'),
			('create',28,1,'Number of chunk creation attempts'),
			('change',29,1,'Number of chunk internal operation attempts'),
			('delete_ok',30,1,'Number of successful chunk deletions'),
			('delete_err',31,1,'Number of unsuccessful chunk deletions'),
			('replicate_ok',32,1,'Number of successful chunk replications'),
			('replicate_err',33,1,'Number of unsuccessful chunk replications'),
			('create_ok',34,1,'Number of successful chunk creations'),
			('create_err',35,1,'Number of unsuccessful chunk creations'),
			('change_ok',36,1,'Number of successful chunk internal operations'),
			('change_err',37,1,'Number of unsuccessful chunk internal operations'),
			('split_ok',38,1,'Number of successful chunk split operations'),
			('split_err',39,1,'Number of unsuccessful chunk split operations'),
			('fileobjects',40,6,'Number of file object'),
			('metaobjects',41,6,'Number of non-file objects (directories,symlinks,etc.)'),
			('chunksec8',42,6,'Total number of chunks stored in EC8 format'),
			('chunksec4',43,6,'Total number of chunks stored in EC4 format'),
			('chunkscopy',44,6,'Total number of chunks stored in COPY format'),
			('chregdanger',45,6,'Number of endangered chunks (mark for removal excluded)'),
			('chregunder',46,6,'Number of undergoal chunks (mark for removal excluded)'),
			('challdanger',47,6,'Number of endangered chunks (mark for removal included)'),
			('challunder',48,6,'Number of undergoal chunks (mark for removal included)'),
			('bytesread',49,5,'Traffic from cluster (data + overhead), bytes per second'),
			('byteswrite',50,5,'Traffic to cluster (data + overhead), bytes per second'),
			('read',51,1,'Number of read operations'),
			('write',52,1,'Number of write operations'),
			('fsync',53,1,'Number of fsync operations'),
			('lock',54,1,'Number of lock operations'),
			('snapshot',55,1,'Number of snapshot operations'),
			('truncate',56,1,'Number of truncate operations'),
			('getxattr',57,1,'Number of getxattr operations'),
			('setxattr',58,1,'Number of setxattr operations'),
			('getfacl',59,1,'Number of getfacl operations'),
			('setfacl',60,1,'Number of setfacl operations'),
			('create',61,1,'Number of create operations'),
			('meta',62,1,'Number of extra metadata operations (sclass,trashretention,eattr etc.)'),
			('servers',64,6,'Number of all registered chunk servers (both connected and disconnected)'),
			('mdservers',65,6,'Number of disconnected chunk servers that are in maintenance mode'),
			('dservers',66,6,'Number of disconnected chunk servers that are not in maintenance mode'),
			('udiff',67,8,'Difference in space usage percent between the most and least used chunk server'),
			('mountbytrcvd',68,5,'Traffic from cluster (data only), bytes per second'),
			('mountbytsent',69,5,'Traffic to cluster (data only), bytes per second'),
			('cpu',100,0,'Cpu usage (total sys+user)')
	]
	mcchartsabr = {
			'delete':['del'],
			'replicate':['rep','repl'],
			'memoryrss':['memrss','rmem','mem'],
			'memoryvirt':['memvirt','vmem']
	}

# modes:
#  0 - percent (sum - cpu)
#  1 - ops/s (operations)
#  2 - humanized format in bytes (memory/disk space)
#  3 - threads (load)
#  4 - time
#  5 - in MB/s (data bytes read/written)
#  6 - raw data (number of chunks/files etc.)
#  7 - not used in CS
#  8 - percent (max - udiff)
	ccchartslist = [
			('ucpu',0,0,'User cpu usage'),
			('scpu',1,0,'System cpu usage'),
			('masterin',2,5,'Data received from master'),
			('masterout',3,5,'Data sent to master'),
			('csrepin',4,5,'Data received by replicator'),
			('csrepout',5,5,'Data sent by replicator'),
			('csservin',6,5,'Data received by csserv'),
			('csservout',7,5,'Data sent by csserv'),
			('hdrbytesr',8,5,'Bytes read (headers)'),
			('hdrbytesw',9,5,'Bytes written (headers)'),
			('hdrllopr',10,1,'Low level reads (headers)'),
			('hdrllopw',11,1,'Low level writes (headers)'),
			('databytesr',12,5,'Bytes read (data)'),
			('databytesw',13,5,'Bytes written (data)'),
			('datallopr',14,1,'Low level reads (data)'),
			('datallopw',15,1,'Low level writes (data)'),
			('hlopr',16,1,'High level reads'),
			('hlopw',17,1,'High level writes'),
			('rtime',18,4,'Read time'),
			('wtime',19,4,'Write time'),
			('repl',20,1,'Replicate chunk ops'),
			('create',21,1,'Create chunk ops'),
			('delete',22,1,'Delete chunk ops'),
			('version',23,1,'Set version ops'),
			('duplicate',24,1,'Duplicate ops'),
			('truncate',25,1,'Truncate ops'),
			('duptrunc',26,1,'Duptrunc (duplicate+truncate) ops'),
			('test',27,1,'Test chunk ops'),
			('load',28,3,'Server load'),
			('memoryrss',29,2,'Resident memory usage'),
			('memoryvirt',30,2,'Virtual memory usage'),
			('movels',31,1,'Low speed move ops'),
			('movehs',32,1,'High speed move ops'),
			('split',34,1,'Split ops'),
			('usedspace',35,2,'Used HDD space in bytes (mark for removal excluded)'),
			('totalspace',36,2,'Total HDD space in bytes (mark for removal excluded)'),
			('chunkcount',37,6,'Number of stored chunks (mark for removal excluded)'),
			('tdusedspace',38,2,'Used HDD space in bytes on disks marked for removal'),
			('tdtotalspace',39,2,'Total HDD space in bytes on disks marked for removal'),
			('tdchunkcount',40,6,'Number of chunks stored on disks marked for removal'),
			('copychunks',41,6,'Number of stored chunks (all disks)'),
			('ec4chunks',42,6,'Number of stored chunk parts in EC4 format (all disks)'),
			('ec8chunks',43,6,'Number of stored chunk parts in EC8 format (all disks)'),
			('hddok',44,6,'Number of valid folders (hard drives)'),
			('hddmfr',45,6,'Number of folders (hard drives) that are marked for removal'),
			('hdddmg',46,6,'Number of folders (hard drives) that are marked as damaged'),
			('udiff',47,8,'Difference in usage percent between the most and least used disk'),
			('cpu',100,0,'Cpu usage (total sys+user)')
	]
	ccchartsabr = {
			'memoryrss':['memrss','rmem','mem'],
			'memoryvirt':['memvirt','vmem']
	}

	jcollect = {
			"version": "4.57.6",
			"timestamp": time_s,
			"shifted_timestamp": shiftts(time_s),
			"timestamp_str": time.ctime(time_s),
			"dataset":{},
			"errors":[]
	}

	mccharts = {}
	cccharts = {}
	for name,no,mode,desc in mcchartslist:
		mccharts[name] = (no,mode,desc)
	for name,abrlist in mcchartsabr.items():
		for abr in abrlist:
			mccharts[abr] = mccharts[name]
	for name,no,mode,desc in ccchartslist:
		cccharts[name] = (no,mode,desc)
	for name,abrlist in ccchartsabr.items():
		for abr in abrlist:
			cccharts[abr] = cccharts[name]

	lastsval = ''
	lastorder = None
	lastrev = None
	lastid = None
	lastmode = None
	try:
		opts,args = getopt.getopt(sys.argv[1:],"hjvH:P:S:C:f:ps:no:rm:i:a:b:c:d:28")
	except Exception:
		opts = [('-h',None)]
	for opt,val in opts:
		if val==None:
			val=""
		if opt=='-h':
			print("usage:")
			print("\t%s [-hjpn28] [-H master_host] [-P master_port] [-f 0..3] -S(IN|IM|IG|MU|IC|IL|MF|CS|MB|HD|EX|MS|RS|SC|PA|OF|AL|MO|QU|MC|CC) [-s separator] [-o order_id [-r]] [-m mode_id] [i id] [-a master_data_count] [-b master_data_desc] [-c chunkserver_data_count] [-d chunkserver_data_desc]" % sys.argv[0])
			print("\t%s [-hjpn28] [-H master_host] [-P master_port] [-f 0..3] -C(RC/ip/port|TR/ip/port|BW/ip/port|M[01]/ip/port|RS/sessionid)" % sys.argv[0])
			print("\t%s -v" % sys.argv[0])
			print("\ncommon:\n")
			print("\t-h : print this message and exit")
			print("\t-v : print version number and exit")
			print("\t-j : print result in JSON format")
			print("\t-p : force plain text format on tty devices")
			print("\t-s separator : field separator to use in plain text format on tty devices (forces -p)")
			print("\t-2 : force 256-color terminal color codes")
			print("\t-8 : force 8-color terminal color codes")
			print("\t-H master_host : master address (default: mfsmaster)")
			print("\t-P master_port : master client port (default: 9421)")
			print("\t-n : do not resolve ip addresses (default when output device is not tty)")
			print("\t-f frame charset number : set frame charset to be displayed as table frames in ttymode")
			print("\t\t-f0 : use simple ascii frames '+','-','|' (default for non utf-8 encodings)")
			if (sys.stdout.encoding=='UTF-8' or sys.stdout.encoding=='utf-8'):
				print("\t\t-f1 : use utf-8 frames: \u250f\u2533\u2513\u2523\u254b\u252b\u2517\u253b\u251b\u2501\u2503\u2578\u2579\u257a\u257b")
				print("\t\t-f2 : use utf-8 frames: \u250c\u252c\u2510\u251c\u253c\u2524\u2514\u2534\u2518\u2500\u2502\u2574\u2575\u2576\u2577")
				print("\t\t-f3 : use utf-8 frames: \u2554\u2566\u2557\u2560\u256c\u2563\u255a\u2569\u255d\u2550\u2551 (default for utf-8 encodings)")
			else:
				print("\t\t-f1 : use utf-8 frames (thick single)")
				print("\t\t-f2 : use utf-8 frames (thin single)")
				print("\t\t-f3 : use utf-8 frames (double - default for utf-8 encodings)")
			print("\nmonitoring:\n")
			print("\t-S data set : defines data set to be displayed")
			print("\t\t-SIN : show full master info")
			print("\t\t-SIM : show only masters states")
			print("\t\t-SIG : show only general master info")
			print("\t\t-SMU : show only master memory usage")
			print("\t\t-SIC : show only chunks info (target/current redundancy level matrices)")
			print("\t\t-SIL : show only loop info (with messages)")
			print("\t\t-SMF : show only missing chunks/files")
			print("\t\t-SCS : show connected chunk servers")
			print("\t\t-SMB : show connected metadata backup servers")
			print("\t\t-SHD : show hdd data")
			print("\t\t-SEX : show exports")
			print("\t\t-SMS : show active mounts")
			print("\t\t-SRS : show resources (storage classes,open files,acquired locks)")
			print("\t\t-SSC : show storage classes")
			print("\t\t-SPA : show patterns override data")
			print("\t\t-SOF : show only open files")
			print("\t\t-SAL : show only acquired locks")
			print("\t\t-SMO : show operation counters")
			print("\t\t-SQU : show quota info")
			print("\t\t-SMC : show master charts data")
			print("\t\t-SCC : show chunkserver charts data")
			print("\t-o order_id : sort data by column specified by 'order id' (depends on data set)")
			print("\t-r : reverse order")
			print("\t-m mode_id : show data specified by 'mode id' (depends on data set)")
			print("\t-i id : storage class id for -SIN/SIC, sessionid for -SOF or inode for -SAL")
			print("\t-a master_data_count : how many master data entries should be shown")
			print("\t-b master_data_desc : define master data columns (prefix with '+' for raw data, prefix with 'ip:[port:]' for server choice, use 'all' for all available data)")
			print("\t-c chunkserver_data_count : how many chunkserver data entries should be shown")
			print("\t-d chunkserver_data_desc : define chunkserver data columns (prefix with '+' for raw data, prefix with 'ip:[port:]' for server choice, use 'all' for all available data)")
			print("\t\tmaster data columns:")
			for name,no,mode,desc in mcchartslist:
				if name in mcchartsabr:
					name = "%s,%s" % (name,",".join(mcchartsabr[name]))
				print("\t\t\t%s - %s" % (name,desc))
			print("\t\tchunkserver data columns:")
			for name,no,mode,desc in ccchartslist:
				if name in ccchartsabr:
					name = "%s,%s" % (name,",".join(ccchartsabr[name]))
				print("\t\t\t%s - %s" % (name,desc))
			print("\ncommands:\n")
			print("\t-C command : perform particular command")
			print("\t\t-CRC/ip/port : remove given chunkserver from list of active chunkservers")
			print("\t\t-CTR/ip/port : temporarily remove given chunkserver from list of active chunkservers (master elect only)")
			print("\t\t-CBW/ip/port : send given chunkserver back to work (from grace state)")
			print("\t\t-CM1/ip/port : switch given chunkserver to maintenance mode")
			print("\t\t-CM0/ip/port : switch given chunkserver to standard mode (from maintenance mode)")
			print("\t\t-CRS/sessionid : remove given session")
			sys.exit(0)
		elif opt=='-v':
			print("version: %s" % VERSION)
			sys.exit(0)
		elif opt=='-2':
			colormode = 2
		elif opt=='-8':
			colormode = 1
		elif opt=='-j':
			jsonmode = 1
		elif opt=='-p':
			forceplaintext = 1
		elif opt=='-s':
			plaintextseparator = val
			forceplaintext = 1
		elif opt=='-n':
			donotresolve = 1
		elif opt=='-f':
			try:
				frameset = int(val)
			except Exception:
				print("-f: wrong value")
				sys.exit(1)
		elif opt=='-H':
			masterhost = val
		elif opt=='-P':
			try:
				masterport = int(val)
			except Exception:
				print("-P: wrong value")
				sys.exit(1)
		elif opt=='-S':
			lastsval = val
			if 'IN' in val:
				sectionset.append("IN")
				sectionsubset.append("IM")
				sectionsubset.append("IG")
				sectionsubset.append("MU")
				sectionsubset.append("IC")
				sectionsubset.append("IL")
				sectionsubset.append("MF")
				if lastmode!=None:
					ICmatrix = lastmode
				if lastid!=None:
					ICsclassid = lastid
				if lastorder!=None:
					IMorder = lastorder
				if lastrev:
					IMrev = 1
			if 'IM' in val:
				sectionset.append("IN")
				sectionsubset.append("IM")
				if lastorder!=None:
					IMorder = lastorder
				if lastrev:
					IMrev = 1
			if 'IG' in val:
				sectionset.append("IN")
				sectionsubset.append("IG")
			if 'MU' in val:
				sectionset.append("IN")
				sectionsubset.append("MU")
			if 'IC' in val:
				sectionset.append("IN")
				sectionsubset.append("IC")
				if lastmode!=None:
					ICmatrix = lastmode
				if lastid!=None:
					ICsclassid = lastid
			if 'IL' in val:
				sectionset.append("IN")
				sectionsubset.append("IL")
			if 'MF' in val:
				sectionset.append("IN")
				sectionsubset.append("MF")
				if lastorder!=None:
					MForder = lastorder
				if lastrev:
					MFrev = 1
			if 'CS' in val:
				sectionset.append("CS")
				sectionsubset.append("CS")
				if lastorder!=None:
					CSorder = lastorder
				if lastrev:
					CSrev = 1
			if 'MB' in val:
				sectionset.append("CS")
				sectionsubset.append("MB")
				if lastorder!=None:
					MBorder = lastorder
				if lastrev:
					MBrev = 1
			if 'HD' in val:
				sectionset.append("HD")
				if lastorder!=None:
					HDorder = lastorder
				if lastrev:
					HDrev = 1
				if lastmode!=None:
					if lastmode>=0 and lastmode<6:
						HDperiod,HDtime = divmod(lastmode,2)
			if 'EX' in val:
				sectionset.append("EX")
				if lastorder!=None:
					EXorder = lastorder
				if lastrev:
					EXrev = 1
			if 'MS' in val:
				sectionset.append("MS")
				if lastorder!=None:
					MSorder = lastorder
				if lastrev:
					MSrev = 1
			if 'MO' in val:
				sectionset.append("MO")
				if lastorder!=None:
					MOorder = lastorder
				if lastrev:
					MOrev = 1
				if lastmode!=None:
					MOdata = lastmode
			if 'RS' in val:
				sectionset.append("RS")
				sectionsubset.append("SC")
				sectionsubset.append("PA")
				sectionsubset.append("OF")
				sectionsubset.append("AL")
			if 'SC' in val:
				sectionset.append("RS")
				sectionsubset.append("SC")
				if lastmode!=None:
					SCdata = lastmode
				if lastorder!=None:
					SCorder = lastorder
				if lastrev:
					SCrev = 1
			if 'PA' in val:
				sectionset.append("RS")
				sectionsubset.append("PA")
				if lastorder!=None:
					PAorder = lastorder
				if lastrev:
					PArev = 1
			if 'OF' in val:
				sectionset.append("RS")
				sectionsubset.append("OF")
				if lastorder!=None:
					OForder = lastorder
				if lastrev!=None:
					OFrev = 1
				if lastid!=None:
					OFsessionid = lastid
			if 'AL' in val:
				sectionset.append("RS")
				sectionsubset.append("AL")
				if lastorder!=None:
					ALorder = lastorder
				if lastrev!=None:
					ALrev = 1
				if lastid!=None:
					ALinode = lastid
			if 'QU' in val:
				sectionset.append("QU")
				if lastorder!=None:
					QUorder = lastorder
				if lastrev:
					QUrev = 1
			if 'MC' in val:
				sectionset.append("MC")
				if lastmode!=None:
					MCrange = lastmode
			if 'CC' in val:
				sectionset.append("CC")
				if lastmode!=None:
					CCrange = lastmode
			lastorder = None
			lastrev = 0
			lastmode = None
		elif opt=='-o':
			try:
				ival = int(val)
			except Exception:
				print("-o: wrong value")
				sys.exit(1)
			if 'IM' in lastsval:
				IMorder = ival
			if 'MF' in lastsval:
				MForder = ival
			if 'CS' in lastsval:
				CSorder = ival
			if 'MB' in lastsval:
				MBorder = ival
			if 'HD' in lastsval:
				HDorder = ival
			if 'EX' in lastsval:
				EXorder = ival
			if 'MS' in lastsval:
				MSorder = ival
			if 'MO' in lastsval:
				MOorder = ival
			if 'SC' in lastsval:
				SCorder = ival
			if 'PA' in lastsval:
				PAorder = ival
			if 'OF' in lastsval:
				OForder = ival
			if 'AL' in lastsval:
				ALorder = ival
			if 'QU' in lastsval:
				QUorder = ival
			if lastsval=='':
				lastorder = ival
		elif opt=='-r':
			if 'IM' in lastsval:
				IMrev = 1
			if 'MF' in lastsval:
				MFrev = 1
			if 'CS' in lastsval:
				CSrev = 1
			if 'MB' in lastsval:
				MBrev = 1
			if 'HD' in lastsval:
				HDrev = 1
			if 'EX' in lastsval:
				EXrev = 1
			if 'MS' in lastsval:
				MSrev = 1
			if 'MO' in lastsval:
				MOrev = 1
			if 'SC' in lastsval:
				SCrev = 1
			if 'PA' in lastsval:
				PArev = 1
			if 'OF' in lastsval:
				OFrev = 1
			if 'AL' in lastsval:
				ALrev = 1
			if 'QU' in lastsval:
				QUrev = 1
			if lastsval=='':
				lastrev = 1
		elif opt=='-m':
			try:
				ival = int(val)
			except Exception:
				print("-m: wrong value")
				sys.exit(1)
			if 'HD' in lastsval:
				if ival>=0 and ival<6:
					HDperiod,HDtime = divmod(ival,2)
				else:
					print("-m: wrong value")
					sys.exit(1)
			if 'MO' in lastsval:
				MOdata = ival
			if 'IN' in lastsval or 'IC' in lastsval:
				ICmatrix = ival
			if 'MC' in lastsval:
				MCrange = ival
			if 'CC' in lastsval:
				CCrange = ival
			if 'SC' in lastsval:
				SCdata = ival
			if lastsval=='':
				lastmode = ival
		elif opt=='-i':
			try:
				ival = int(val)
			except Exception:
				print("-i: wrong value")
				sys.exit(1)
			if 'OF' in lastsval:
				OFsessionid = ival
			if 'AL' in lastsval:
				ALinode = ival
			if 'IN' in lastsval or 'IC' in lastsval:
				ICsclassid = ival
			if lastsval=='':
				lastid = ival
		elif opt=='-a':
			try:
				MCcount = int(val)
			except Exception:
				print("-a: wrong value")
				sys.exit(1)
		elif opt=='-b':
			for x in val.split(','):
				x = x.strip()
				if ':' in x:
					xs = x.split(':')
					if len(xs)==2:
						chhost = xs[0]
						chport = 9421
						x = xs[1]
					elif len(xs)==3:
						chhost = xs[0]
						try:
							chport = int(xs[1])
						except Exception:
							print("master port: wrong value")
							sys.exit(1)
						x = xs[2]
					else:
						print("Wrong chart definition: %s" % x)
						sys.exit(0)
				else:
					chhost = None
					chport = None
				if x!='' and x[0]=='+':
					x = x[1:]
					rawmode = 1
				else:
					rawmode = 0
				if x in mccharts:
					MCchdata.append((chhost,chport,mccharts[x][0],mccharts[x][1],mccharts[x][2],x,rawmode))
				elif x=='all':
					for name,no,mode,desc in mcchartslist:
						if (no<100):
							MCchdata.append((chhost,chport,no,mode,desc,name,rawmode))
				else:
					print("Unknown master chart name: %s" % x)
					sys.exit(0)
		elif opt=='-c':
			try:
				CCcount = int(val)
			except Exception:
				print("-c: wrong value")
				sys.exit(1)
		elif opt=='-d':
			for x in val.split(','):
				x = x.strip()
				if ':' in x:
					xs = x.split(':')
					if len(xs)==2:
						chhost = xs[0]
						chport = 9422
						x = xs[1]
					elif len(xs)==3:
						chhost = xs[0]
						try:
							chport = int(xs[1])
						except Exception:
							print("chunkserver port: wrong value")
							sys.exit(1)
						x = xs[2]
					else:
						print("Unknown chart name: %s" % x)
						sys.exit(0)
				else:
					chhost = None
					chport = None
				if x!='' and x[0]=='+':
					x = x[1:]
					rawmode = 1
				else:
					rawmode = 0
#				if chhost==None or chport==None:
#					print("in chunkserver chart data server ip/host must be specified")
#					sys.exit(0)
				if x in cccharts:
					CCchdata.append((chhost,chport,cccharts[x][0],cccharts[x][1],cccharts[x][2],x,rawmode))
				elif x=='all':
					for name,no,mode,desc in ccchartslist:
						if (no<100):
							CCchdata.append((chhost,chport,no,mode,desc,name,rawmode))
				else:
					print("Unknown chunkserver chart name: %s" % x)
					sys.exit(0)
		elif opt=='-C':
			clicommands.append(val)

	if sectionset==[] and clicommands==[]:
		print("Specify data to be shown (option -S) or command (option -C). Use '-h' for help.")
		sys.exit(0)

	ttymode = 1 if forceplaintext==0 and os.isatty(1) and jsonmode==0 else 0
	if ttymode:
		try:
			import curses
			curses.setupterm()
			if curses.tigetnum("colors")>=256:
				colors256 = 1
			else:
				colors256 = 0
		except Exception:
			colors256 = 1 if 'TERM' in os.environ and '256' in os.environ['TERM'] else 0
		# colors: 0 - white,1 - red,2 - orange,3 - yellow,4 - green,5 - cyan,6 - blue,7 - violet,8 - gray
		CSI="\x1B["
		if colors256:
			ttyreset=CSI+"0m"
			colorcode=[CSI+"38;5;196m",CSI+"38;5;208m",CSI+"38;5;226m",CSI+"38;5;34m",CSI+"38;5;30m",CSI+"38;5;19m",CSI+"38;5;55m",CSI+"38;5;244m"]
		else:
			ttysetred=CSI+"31m"
			ttysetyellow=CSI+"33m"
			ttysetgreen=CSI+"32m"
			ttysetcyan=CSI+"36m"
			ttysetblue=CSI+"34m"
			ttysetmagenta=CSI+"35m"
			ttyreset=CSI+"0m"
			# no orange - use red, no gray - use white
			colorcode=[ttysetred,ttysetred,ttysetyellow,ttysetgreen,ttysetcyan,ttysetblue,ttysetmagenta,""]
	else:
		colorcode=["","","","","","","",""]

	if ttymode and (sys.stdout.encoding=='UTF-8' or sys.stdout.encoding=='utf-8'):
		if frameset>=0 and frameset<=3:
			tableframes=frameset
		else:
			tableframes=0
	else:
		tableframes=0

	# terminal encoding mambo jumbo (mainly replace unicode chars that can't be printed with '?' instead of throwing exception)
	term_encoding = sys.stdout.encoding
	if term_encoding==None:
		term_encoding = 'utf-8'
	if sys.version_info[1]<7:
		sys.stdout = codecs.getwriter(term_encoding)(sys.stdout.detach(),'replace')
		sys.stdout.encoding = term_encoding
	else:
		sys.stdout.reconfigure(errors='replace')

	# lines prepared for JSON output (force utf-8):
	#if sys.version_info[1]<7:
	#	sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
	#else:
	#	sys.stdout.reconfigure(encoding='utf-8')

	# frames:
	#  +-----+-----+-----+-----+
	#  |     |     |     |     |
	#  |     |     |     |  |  |
	#  |  +- | -+- | -+  |  +- |
	#  |  |  |  |  |  |  |  |  |
	#  |     |     |     |     |
	#  +-----+-----+-----+-----+
	#  |     |     |     |     |
	#  |  |  |  |  |  |  |  |  |
	#  | -+- | -+  |  +- | -+- |
	#  |  |  |  |  |     |     |
	#  |     |     |     |     |
	#  +-----+-----+-----+-----+
	#  |     |     |     |     |
	#  |  |  |     |  |  |     |
	#  | -+  | -+- |  +  | -+  |
	#  |     |     |  |  |     |
	#  |     |     |     |     |
	#  +-----+-----+-----+-----+
	#  |     |     |     |     |
	#  |  |  |     |     |     |
	#  |  +  |  +- |  +  |  +  |
	#  |     |     |  |  |     |
	#  |     |     |     |     |
	#  +-----+-----+-----+-----+
	#  

	class Table:
		Needseparator = 0
		def __init__(self,title,ccnt,defattr=""):
			if tableframes==1:
				self.frames = ['\u250f', '\u2533', '\u2513', '\u2523', '\u254b', '\u252b', '\u2517', '\u253b', '\u251b', '\u2501', '\u2503', '\u2578', '\u2579', '\u257a', '\u257b', ' ']
			elif tableframes==2:
				self.frames = ['\u250c', '\u252c', '\u2510', '\u251c', '\u253c', '\u2524', '\u2514', '\u2534', '\u2518', '\u2500', '\u2502', '\u2574', '\u2575', '\u2576', '\u2577', ' ']
			elif tableframes==3:
				self.frames = ['\u2554', '\u2566', '\u2557', '\u2560', '\u256c', '\u2563', '\u255a', '\u2569', '\u255d', '\u2550', '\u2551', ' ', '\u2551', ' ', '\u2551', ' ']
			else:
				self.frames = ['+','+','+','+','+','+','+','+','+','-','|',' ','|',' ','|',' ']
			self.title = title
			self.ccnt = ccnt
			self.head = []
			self.body = []
			self.defattrs = []
			self.cwidth = []
			for _ in range(ccnt):
				self.defattrs.append(defattr)
				self.cwidth.append(0)
		def combineattr(self,attr,defattr):
			attrcolor = ""
			for c in ("0","1","2","3","4","5","6","7","8"):
				if c in defattr:
					attrcolor = c
			for c in ("0","1","2","3","4","5","6","7","8"):
				if c in attr:
					attrcolor = c
			attrjust = ""
			for c in ("l","L","r","R","c","C"):
				if c in defattr:
					attrjust = c
			for c in ("l","L","r","R","c","C"):
				if c in attr:
					attrjust = c
			return attrcolor+attrjust
		def header(self,*rowdata):
			ccnt = 0
			for celldata in rowdata:
				if type(celldata) is tuple:
					if len(celldata)==3:
						ccnt+=celldata[2]
					else:
						if celldata[0]!=None:
							cstr = myunicode(celldata[0])
							if len(cstr) > self.cwidth[ccnt]:
								self.cwidth[ccnt] = len(cstr)
						ccnt+=1
				else:
					if celldata!=None:
						cstr = myunicode(celldata)
						if len(cstr) > self.cwidth[ccnt]:
							self.cwidth[ccnt] = len(cstr)
					ccnt+=1
			if ccnt != self.ccnt:
				raise IndexError
			self.head.append(rowdata)
		def defattr(self,*rowdata):
			if len(rowdata) != self.ccnt:
				raise IndexError
			self.defattrs = rowdata
		def append(self,*rowdata):
			ccnt = 0
			rdata = []
			for celldata in rowdata:
				if type(celldata) is tuple:
					if celldata[0]!=None:
						cstr = myunicode(celldata[0])
					else:
						cstr = ""
					if len(celldata)==3:
						rdata.append((cstr,self.combineattr(celldata[1],self.defattrs[ccnt]),celldata[2]))
						ccnt+=celldata[2]
					else:
						if len(cstr) > self.cwidth[ccnt]:
							self.cwidth[ccnt] = len(cstr)
						if len(celldata)==2:
							rdata.append((cstr,self.combineattr(celldata[1],self.defattrs[ccnt])))
						else:
							rdata.append((cstr,self.defattrs[ccnt]))
						ccnt+=1
				else:
					if celldata!=None:
						cstr = myunicode(celldata)
						if ccnt >= len(self.cwidth):
							raise IndexError("ccnt: %u, self.ccnt: %u, len(self.cwidth): %u" % (ccnt, self.ccnt, len(self.cwidth)))
						if len(cstr) > self.cwidth[ccnt]:
							self.cwidth[ccnt] = len(cstr)
						rdata.append((cstr,self.defattrs[ccnt]))
					else:
						rdata.append(celldata)
					ccnt+=1
			if ccnt != self.ccnt:
				raise IndexError("ccnt: %u, self.ccnt: %u" % (ccnt, self.ccnt))
			self.body.append(rdata)
		def attrdata(self,cstr,cattr,cwidth):
			retstr = ""
			if "1" in cattr:
				retstr += colorcode[0]
				needreset = 1
			elif "2" in cattr:
				retstr += colorcode[1]
				needreset = 1
			elif "3" in cattr:
				retstr += colorcode[2]
				needreset = 1
			elif "4" in cattr:
				retstr += colorcode[3]
				needreset = 1
			elif "5" in cattr:
				retstr += colorcode[4]
				needreset = 1
			elif "6" in cattr:
				retstr += colorcode[5]
				needreset = 1
			elif "7" in cattr:
				retstr += colorcode[6]
				needreset = 1
			elif "8" in cattr:
				retstr += colorcode[7]
				needreset = 1
			else:
				needreset = 0
			if cstr=="--":
				retstr += " "+"-"*cwidth+" "
			elif cstr=="---":
				retstr += "-"*(cwidth+2)
			elif "L" in cattr or "l" in cattr:
				retstr += " "+cstr.ljust(cwidth)+" "
			elif "R" in cattr or "r" in cattr:
				retstr += " "+cstr.rjust(cwidth)+" "
			else:
				retstr += " "+cstr.center(cwidth)+" "
			if needreset:
				retstr += ttyreset
			return retstr
		def lines(self):
			outstrtab = []
			if ttymode:
				tabdata = []
				# upper frame
				tabdata.append((("---","",self.ccnt),))
				# title
				tabdata.append(((self.title,"",self.ccnt),))
				# header
				if len(self.head)>0:
					tabdata.append((("---","",self.ccnt),))
					tabdata.extend(self.head)
				# head and data separator
				tabdata.append((("---","",self.ccnt),))
				# data
				if len(self.body)==0:
					tabdata.append((("no data","",self.ccnt),))
				else:
					tabdata.extend(self.body)
				# bottom frame
				tabdata.append((("---","",self.ccnt),))
				# check col-spaned headers and adjust column widths if necessary
				for rowdata in tabdata:
					ccnt = 0
					for celldata in rowdata:
						if type(celldata) is tuple and len(celldata)==3 and celldata[0]!=None:
							cstr = myunicode(celldata[0])
							clen = len(cstr)
							cwidth = sum(self.cwidth[ccnt:ccnt+celldata[2]])+3*(celldata[2]-1)
							if clen > cwidth:
								add = clen - cwidth
								adddm = divmod(add,celldata[2])
								cadd = adddm[0]
								if adddm[1]>0:
									cadd+=1
								for i in range(celldata[2]):
									self.cwidth[ccnt+i] += cadd
							ccnt += celldata[2]
						else:
							ccnt += 1
				separators = []
				# before tab - no separators
				seplist = []
				for i in range(self.ccnt+1):
					seplist.append(0)
				separators.append(seplist)
				for rowdata in tabdata:
					seplist = [1]
					for celldata in rowdata:
						if type(celldata) is tuple and len(celldata)==3:
							for i in range(celldata[2]-1):
								seplist.append(1 if celldata[0]=='---' else 0)
						seplist.append(1)
					separators.append(seplist)
				# after tab - no separators
				seplist = []
				for i in range(self.ccnt+1):
					seplist.append(0)
				separators.append(seplist)
				# add upper and lower separators:
				updownsep = [[a*2 + b for (a,b) in zip(x,y)] for (x,y) in zip(separators[2:],separators[:-2])]
				# create table
				for (rowdata,sepdata) in zip(tabdata,updownsep):
	#				print rowdata,sepdata
					ccnt = 0
					line = ""
					nsep = 0 #self.frames[10]
					for celldata in rowdata:
						cpos = ccnt
						cattr = ""
						if type(celldata) is tuple:
							if celldata[1]!=None:
								cattr = celldata[1]
							if len(celldata)==3:
								cwidth = sum(self.cwidth[ccnt:ccnt+celldata[2]])+3*(celldata[2]-1)
								ccnt+=celldata[2]
							else:
								cwidth = self.cwidth[ccnt]
								ccnt+=1
							cstr = celldata[0]
						else:
							cstr = celldata
							cwidth = self.cwidth[ccnt]
							ccnt+=1
						if cstr==None:
							cstr = ""
						cstr = myunicode(cstr)
						if cstr=="---":
							if nsep==0:
								line += self.frames[(13,6,0,3)[sepdata[cpos]]]
								#line += self.frames[(15,6,0,3)[sepdata[cpos]]]
							else:
								line += self.frames[(9,7,1,4)[sepdata[cpos]]]
							nsep = 1 #self.frames[4]
							for ci in range(cpos,ccnt-1):
								line += self.frames[9]*(self.cwidth[ci]+2)
								line += self.frames[(9,7,1,4)[sepdata[ci+1]]]
							line += self.frames[9]*(self.cwidth[ccnt-1]+2)
						else:
							if nsep==0:
								line += self.frames[(15,12,14,10)[sepdata[cpos]]]
								#line += self.frames[(15,10,10,10)[sepdata[cpos]]]
							else:
								line += self.frames[(11,8,2,5)[sepdata[cpos]]]
								#line += self.frames[(15,8,2,5)[sepdata[cpos]]]
							nsep = 0
							line += self.attrdata(cstr,cattr,cwidth)
					if nsep==0:
						line += self.frames[(15,12,14,10)[sepdata[ccnt]]]
						#line += self.frames[(15,10,10,10)[sepdata[ccnt]]]
					else:
						line += self.frames[(11,8,2,5)[sepdata[ccnt]]]
						#line += self.frames[(15,8,2,5)[sepdata[ccnt]]]
					outstrtab.append(line)
			else:
				for rowdata in self.body:
					row = []
					for celldata in rowdata:
						if type(celldata) is tuple:
							cstr = myunicode(celldata[0])
						elif celldata!=None:
							cstr = myunicode(celldata)
						else:
							cstr = ""
						row.append(cstr)
					outstrtab.append("%s:%s%s" % (self.title,plaintextseparator,plaintextseparator.join(row)))
			return outstrtab
		def __str__(self):
			if Table.Needseparator:
				sep = "\n"
			else:
				sep = ""
				Table.Needseparator = 1
			return sep+("\n".join(self.lines()))

	#x = Table("Test title",4)
	#x.header("column1","column2","column3","column4")
	#x.append("t1","t2","very long entry","test")
	#x.append(("r","r3"),("l","l2"),"also long entry","test")
	#print x
	#
	#x = Table("Very long table title",2)
	#x.defattr("l","r")
	#x.append("key","value")
	#x.append("other key",123)
	#y = []
	#y.append(("first","1"))
	#y.append(("second","4"))
	#x.append(*y)
	#print x
	#
	#x = Table("Table with complicated header",15,"r")
	#x.header(("","",4),("I/O stats last min","",8),("","",3))
	#x.header(("info","",4),("---","",8),("space","",3))
	#x.header(("","",4),("transfer","",2),("max time","",3),("# of ops","",3),("","",3))
	#x.header(("---","",15))
	#x.header("IP path","chunks","last error","status","read","write","read","write","fsync","read","write","fsync","used","total","used %")
	#x.append("192.168.1.102:9422:/mnt/hd4/",66908,"no errors","ok","19 MiB/s","27 MiB/s","263625 us","43116 us","262545 us",3837,3295,401,"1.0 TiB","1.3 TiB","76.41%")
	#x.append("192.168.1.102:9422:/mnt/hd5/",67469,"no errors","ok","25 MiB/s","29 MiB/s","340303 us","89168 us","223610 us",2487,2593,366,"1.0 TiB","1.3 TiB","75.93%")
	#x.append("192.168.1.111:9422:/mnt/hd5/",109345,("2012-10-12 07:27","2"),("damaged","1"),"-","-","-","-","-","-","-","-","1.2 TiB","1.3 TiB","87.18%")
	#x.append("192.168.1.211:9422:/mnt/hd5/",49128,"no errors",("marked for removal","4"),"-","-","-","-","-","-","-","-","501 GiB","1.3 TiB","36.46%")
	##x.append("192.168.1.111:9422:/mnt/hd5/",109345,("2012-10-12 07:27","2"),("damaged","1"),("","-",8),"1.2 TiB","1.3 TiB","87.18%")
	##x.append("192.168.1.211:9422:/mnt/hd5/",49128,"no errors",("marked for removal","4"),("","-",8),"501 GiB","1.3 TiB","36.46%")
	#x.append("192.168.1.229:9422:/mnt/hd10/","67969","no errors","ok","17 MiB/s","11 MiB/s","417292 us","76333 us","1171903 us","2299","2730","149","1.0 TiB","1.3 TiB","76.61%")
	#print x
	#
	#x = Table("Colors",1,"r")
	#x.append(("white","0"))
	#x.append(("red","1"))
	#x.append(("orange","2"))
	#x.append(("yellow","3"))
	#x.append(("green","4"))
	#x.append(("cyan","5"))
	#x.append(("blue","6"))
	#x.append(("magenta","7"))
	#x.append(("gray","8"))
	#print x
	#
	#x = Table("Adjustments",1)
	#x.append(("left","l"))
	#x.append(("right","r"))
	#x.append(("center","c"))
	#print x
	#
	#x = Table("Special entries",3)
	#x.defattr("l","r","r")
	#x.header("entry","effect","extra column")
	#x.append("-- ","--","")
	#x.append("--- ","---","")
	#x.append("('--','',2)",('--','',2))
	#x.append("('','',2)",('','',2))
	#x.append("('---','',2)",('---','',2))
	#x.append("('red','1')",('red','1'),'')
	#x.append("('orange','2')",('orange','2'),'')
	#x.append("('yellow','3')",('yellow','3'),'')
	#x.append("('green','4')",('green','4'),'')
	#x.append("('cyan','5')",('cyan','5'),'')
	#x.append("('blue','6')",('blue','6'),'')
	#x.append("('magenta','7')",('magenta','7'),'')
	#x.append("('gray','8')",('gray','8'),'')
	#x.append(('---','',3))
	#x.append("('left','l',2)",('left','l',2))
	#x.append("('right','r',2)",('right','r',2))
	#x.append("('center','c',2)",('center','c',2))
	#print x

	def resolve(strip):
		if donotresolve:
			return strip
		try:
			return (socket.gethostbyaddr(strip))[0]
		except Exception:
			return strip


# common auxiliary functions

def getmasteraddresses():
	m = []
	for mhost in masterhost.replace(';',' ').replace(',',' ').split():
		try:
			for i in socket.getaddrinfo(mhost,masterport,socket.AF_INET,socket.SOCK_STREAM,socket.SOL_TCP):
				if i[0]==socket.AF_INET and i[1]==socket.SOCK_STREAM and i[2]==socket.SOL_TCP:
					m.append(i[4])
		except Exception:
			pass
	return m

def disablesmask_to_string_list(disables_mask):
	cmds = ["chown","chmod","symlink","mkfifo","mkdev","mksock","mkdir","unlink","rmdir","rename","move","link","create","readdir","read","write","truncate","setlength","appendchunks","snapshot","settrash","setsclass","seteattr","setxattr","setfacl"]
	l = []
	m = 1
	for cmd in cmds:
		if disables_mask & m:
			l.append(cmd)
		m <<= 1
	return l

def disablesmask_to_string(disables_mask):
	return ",".join(disablesmask_to_string_list(disables_mask))

def state_name(stateid):
	if stateid==STATE_DUMMY:
		return "DUMMY"
	elif stateid==STATE_USURPER:
		return "USURPER"
	elif stateid==STATE_FOLLOWER:
		return "FOLLOWER"
	elif stateid==STATE_ELECT:
		return "ELECT"
	elif stateid==STATE_DEPUTY:
		return "DEPUTY"
	elif stateid==STATE_LEADER:
		return "LEADER"
	else:
		return "???"

def state_color(stateid,sync):
	if stateid==STATE_DUMMY:
		return 8
	elif stateid==STATE_FOLLOWER or stateid==STATE_USURPER:
		if sync:
			return 5
		else:
			return 6
	elif stateid==STATE_ELECT:
		return 3
	elif stateid==STATE_DEPUTY:
		return 2
	elif stateid==STATE_LEADER:
		return 4
	else:
		return 1

def gracetime2txt(gracetime):
	load_state_has_link = 0
	if gracetime>=0xC0000000:
		load_state = "Overloaded"
		load_state_msg = "Overloaded"
		load_state_info = "server queue is heavy loaded (overloaded)"
	elif gracetime>=0x80000000:
		load_state = "Rebalance"
		load_state_msg = "Rebalancing"
		load_state_info = "internal rebalance in progress"
	elif gracetime>=0x40000000:
		load_state = "Fast rebalance"
		load_state_msg = "Fast rebalancing"
		load_state_info = "high speed rebalance in progress"
	elif gracetime>0:
		load_state = "G(%u)" % gracetime
		load_state_msg = "%u secs graceful" % gracetime
		load_state_info = "server in graceful period - back to normal after %u seconds" % gracetime
		load_state_has_link = 1
	else:
		load_state = "Normal"
		load_state_msg = "Normal load"
		load_state_info = "server is responsive, working with low load"
	return (load_state, load_state_msg, load_state_info, load_state_has_link)

def decimal_number(number,sep=' '):
	parts = []
	while number>=1000:
		number,rest = divmod(number,1000)
		parts.append("%03u" % rest)
	parts.append(str(number))
	parts.reverse()
	return sep.join(parts)

def decimal_number_html(number):
	return decimal_number(number,"&#8239;")

def humanize_number(number,sep='',suff='B'):
	number*=100
	scale=0
	while number>=99950:
		number = number//1024
		scale+=1
	if number<995 and scale>0:
		b = (number+5)//10
		nstr = "%u.%u" % divmod(b,10)
	else:
		b = (number+50)//100
		nstr = "%u" % b
	if scale>0:
		return "%s%s%si%s" % (nstr,sep,"-KMGTPEZY"[scale],suff)
	else:
		return "%s%s%s" % (nstr,sep,suff)

def timeduration_to_shortstr(timeduration, sep=''):
	for l,s in ((604800,'w'),(86400,'d'),(3600,'h'),(60,'m'),(0,'s')):
		if timeduration>=l:
			if l>0:
				n = float(timeduration)/float(l)
			else:
				n = float(timeduration)
			rn = round(n,1)
			if n==round(n,0):
				return "%.0f%s%s" % (n,sep,s)
			else:
				return "%s%.1f%s%s" % (("~"+sep if n!=rn else ""),rn,sep,s)
	return "???"

def timeduration_to_fullstr(timeduration):
	if timeduration>=86400:
		days,dayseconds = divmod(timeduration,86400)
		daysstr = "%u day%s, " % (days,("s" if days!=1 else ""))
	else:
		dayseconds = timeduration
		daysstr = ""
	hours,hourseconds = divmod(dayseconds,3600)
	minutes,seconds = divmod(hourseconds,60)
	if seconds==round(seconds,0):
		return "%u second%s (%s%u:%02u:%02u)" % (timeduration,("" if timeduration==1 else "s"),daysstr,hours,minutes,seconds)
	else:
		seconds,fracsec = divmod(seconds,1)
		return "%.3f seconds (%s%u:%02u:%02u.%03u)" % (timeduration,daysstr,hours,minutes,seconds,round(1000*fracsec,0))

def hours_to_str(hours):
	days,hoursinday = divmod(hours,24)
	if days>0:
		if hoursinday>0:
			return "%ud %uh" % (days,hoursinday)
		else:
			return "%ud" % days
	else:
		return "%uh" % hours

def time_to_str(tm):
	return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(tm))

def label_id_to_char(id):
	return chr(ord('A')+id)

def labelmask_to_str(labelmask):
	str = ""
	m = 1
	for i in xrange(26):
		if labelmask & m:
			str += label_id_to_char(i)
		m <<= 1
	return str

def labelmasks_to_str(labelmasks):
	if labelmasks[0]==0:
		return "*"
	r = []
	for labelmask in labelmasks:
		if labelmask==0:
			break
		r.append(labelmask_to_str(labelmask))
	return "+".join(r)

def labelexpr_to_str(labelexpr):
	stack = []
	if labelexpr[0]==0:
		return "*"
	for i in labelexpr:
		if i==255:
			stack.append((0,'*'))
		elif i>=192 and i<255:
			n = (i-192)
			stack.append((0,chr(ord('A')+n)))
		elif i>=128 and i<192:
			n = (i-128)+2
			if n>len(stack):
				return 'EXPR ERROR'
			m = []
			for _ in xrange(n):
				l,s = stack.pop()
				if l>1:
					m.append("(%s)" % s)
				else:
					m.append(s)
			m.reverse()
			stack.append((1,"&".join(m)))
		elif i>=64 and i<128:
			n = (i-64)+2
			if n>len(stack):
				return 'EXPR ERROR'
			m = []
			for _ in xrange(n):
				l,s = stack.pop()
				if l>2:
					m.append("(%s)" % s)
				else:
					m.append(s)
			m.reverse()
			stack.append((2,"|".join(m)))
		elif i==1:
			if len(stack)==0:
				return 'EXPR ERROR'
			l,s = stack.pop()
			if l>0:
				stack.append((0,"~(%s)" % s))
			else:
				stack.append((0,"~%s" % s))
		elif i==0:
			break
		else:
			return 'EXPR ERROR'
	if len(stack)!=1:
		return 'EXPR ERROR'
	l,s = stack.pop()
	return s

def labellist_fold(labellist):
	ll = []
	prev_data = None
	count = 0
	for data in labellist:
		if (data != prev_data):
			if count>0:
				if count>1:
					ll.append(('%u%s' % (count,prev_data[0]),prev_data[1]))
				else:
					ll.append(prev_data)
			prev_data = data
			count = 1
		else:
			count = count+1
	if count>0:
		if count>1:
			ll.append(('%u%s' % (count,prev_data[0]),prev_data[1]))
		else:
			ll.append(prev_data)
	return ll

def uniqmask_to_str(uniqmask):
	if uniqmask==0:
		return "-"
	if uniqmask & UNIQ_MASK_IP:
		return "[IP]"
	if uniqmask & UNIQ_MASK_RACK:
		return "[RACK]"
	rstr = ""
	inrange = 0
	for i in xrange(26):
		if uniqmask & (1<<i):
			if inrange==0:
				rstr += chr(ord('A')+i)
				if i<24 and ((uniqmask>>i)&7)==7:
					inrange = 1
		else:
			if inrange==1:
				rstr += '-'
				rstr += chr(ord('A')+(i-1))
				inrange = 0
	if inrange:
		rstr += '-'
		rstr += chr(ord('A')+25)
	return rstr

def eattr_to_str(seteattr,clreattr):
	eattrs = ["noowner","noattrcache","noentrycache","nodatacache","snapshot","undeletable","appendonly","immutable"]
	outlist = []
	mask = 1
	for eattrname in eattrs:
		if seteattr&mask:
			outlist.append("+%s" % eattrname)
		if clreattr&mask:
			outlist.append("-%s" % eattrname)
		mask = mask << 1
	if len(outlist)>0:
		return ",".join(outlist)
	else:
		return "-"

def get_string_from_packet(data,pos,err):
	rstr = ""
	shift = 0
	if 1+pos > len(data):
		err = 1;
	if err==0:
		shift = data[pos]
		if shift+1+pos > len(data):
			err = 1
		else:
			rstr = data[pos+1:pos+1+shift]
		rstr = rstr.decode('utf-8','replace')
	return (rstr,pos+shift+1,err)

def get_longstring_from_packet(data,pos,err):
	rstr = ""
	shift = 0
	if 2+pos > len(data):
		err = 1;
	if err==0:
		shift = data[pos]*256+data[pos+1]
		if shift+2+pos > len(data):
			err = 1
		else:
			rstr = data[pos+2:pos+2+shift]
		rstr = rstr.decode('utf-8','replace')
	return (rstr,pos+shift+2,err)

def print_error(msg):
	if cgimode:
		out = []
		out.append("""<div class="tab_title ERROR">Oops!</div>""")
		out.append("""<table class="FR MESSAGE" cellspacing="0">""")
		out.append("""	<tr><td align="left"><span class="ERROR">An error has occurred:</span> %s</td></tr>""" % msg)
		out.append("""</table>""")
		print("\n".join(out))
	elif jsonmode:
		json_er_dict = {}
		json_er_errors = []
		json_er_errors.append({"message":msg})
		json_er_dict["errors"] = json_er_errors
		jcollect["errors"].append(json_er_dict)
	elif ttymode:
		tab = Table("Error",1)
		tab.header("message")
		tab.defattr("l")
		tab.append(msg)
		print("%s%s%s" % (colorcode[1],tab,ttyreset))
	else:
		print("""Oops, an error has occurred:""")
		print(msg)

def print_exception():
	exc_type, exc_value, exc_traceback = sys.exc_info()
	try:
		if cgimode:
			print("""<div class="tab_title ERROR">Oops!</div>""")
			print("""<table class="FR MESSAGE">""")
			print("""<tr><td align="left"><span class="ERROR">An error has occurred. Check your MooseFS configuration and network connections. </span><br/>If you decide to seek support because of this error, please include the following traceback:""")
			print("""<pre>""")
			print(traceback.format_exc().strip())
			print("""</pre></td></tr>""")
			print("""</table>""")
		elif jsonmode:
			json_er_dict = {}
			json_er_traceback = []
			for d in traceback.extract_tb(exc_traceback):
				json_er_traceback.append({"file":d[0] , "line":d[1] , "in":d[2] , "text":repr(d[3])})
			json_er_dict["traceback"] = json_er_traceback
			json_er_errors = []
			for d in traceback.format_exception_only(exc_type, exc_value):
				json_er_errors.append(repr(d.strip()))
			json_er_dict["errors"] = json_er_errors
			jcollect["errors"].append(json_er_dict)
		elif ttymode:
			tab = Table("Exception Traceback",4)
			tab.header("file","line","in","text")
			tab.defattr("l","r","l","l")
			for d in traceback.extract_tb(exc_traceback):
				tab.append(d[0],d[1],d[2],repr(d[3]))
			tab.append(("---","",4))
			tab.append(("Error","c",4))
			tab.append(("---","",4))
			for d in traceback.format_exception_only(exc_type, exc_value):
				tab.append((repr(d.strip()),"",4))
			print("%s%s%s" % (colorcode[1],tab,ttyreset))
		else:
			print("""---------------------------------------------------------------- error -----------------------------------------------------------------""")
			print(traceback.format_exc().strip())
			print("""----------------------------------------------------------------------------------------------------------------------------------------""")
	except Exception:
		print(traceback.format_exc().strip())

def version_convert(version):
	if version>=(4,0,0) and version<(4,11,0):
		return (version,0)
	elif version>=(2,0,0):
		return ((version[0],version[1],version[2]//2),version[2]&1)
	elif version>=(1,7,0):
		return (version,1)
	elif version>(0,0,0):
		return (version,0)
	else:
		return (version,-1)

def version_str_and_sort(version):
	version,pro = version_convert(version)
	strver = "%u.%u.%u" % version
	sortver = "%05u_%03u_%03u" % version
	if pro==1:
		strver += " PRO"
		sortver += "_2"
	elif pro==0:
		sortver += "_1"
	else:
		sortver += "_0"
	if strver == '0.0.0':
		strver = ''
	return (strver,sortver)

#Compares (stringified) versions ("4.56.3 PRO"), returns 1 if ver1>ver2, -1 if ver1<ver2, 0 if ver1==ver2, doesn't take into account "PRO" on not "PRO"
def cmp_ver(ver1, ver2):
	(ver1_1, ver1_2, ver1_3)=list(map(int, ver1.upper().replace("PRO","").split('.')))
	(ver2_1, ver2_2, ver2_3)=list(map(int, ver2.upper().replace("PRO","").split('.')))
	if (ver1_1>ver2_1):
		return 1
	elif (ver1_1<ver2_1):
		return -1
	if (ver1_2>ver2_2):
		return 1
	elif (ver1_2<ver2_2):
		return -1
	if (ver1_3>ver2_3):
		return 1
	elif (ver1_3<ver2_3):
		return -1
	return 0

# Generates a simple vertical table with a title row (if provided) and individual td styling (if provided)
def html_table_vertical(title, tdata, tablecls=""):
	out=[]
	out.append("""<table class="FR vertical no-hover %s">""" % tablecls)
	if title:
		out.append(""" <thead><tr><th colspan="2">%s</th></tr></thead>""" % title)
	out.append(""" <tbody>""")
	for row in tdata:
		out.append("""		<tr><th>%s</th>""" % str(row[0]))
		out.append("""		<td class="%s">%s</td></tr>""" % ("center" if len(row)<3 else row[2], str(row[1])))
	out.append(""" </tbody>""")
	out.append("""</table>""")
	return "\n".join(out)

# Generates a SVG-based round knob for selecting (from 2 to 6) different options
# id - string knob unique identifier
# r - circle radius
# wh - tuple (width, height) of svg picture
# cxy - tuple (cx,cy) center of knob circle 
# opts - a list of exactly 4 options, each option is a tuple: (degrees (0=north), option_title, option_subtitle, onclick_function)
# store - flag if knob position should be stored in the browser session storage
def html_knob_selector(id, r, wh, cxy, opts, store=True):
	w=wh[0]
	h=wh[1]
	cx=cxy[0]
	cy=cxy[1]	
	rx = r+4  #x-offset from circle center to line break
	tx = 12   #x-text offset from line break
	store_cls = "" if store else "dont-store"

	degrees_arr = []
	fun_arr = []
	out =[]

	# prepare onclick-s for all options
	for i in range(len(opts)): #display titles, subtitles
		opt=opts[i]
		degrees=opt[0]
		name=opt[1]
		if len(name)<4:
			name= name+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" if degrees>0 else "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" + name
		anchor = "start" if degrees>0 else "end"
		x = cx+rx+tx if degrees>0 else cx-rx-tx #text x-pos
		y = cy + math.tan((90+abs(degrees)) * math.pi / 180)*rx + 3 #text y-pos
		y = max(y,10)
		#setup a function (w/timeout) to call on a one of knob's title click
		out.append("""<g onclick="rotateKnob('%s',%u);setTimeout(() => {%s;}, 300);"><text class="option-name" style="text-anchor: %s;" x="%f" y="%f">%s</text>""" % (id,degrees,opt[3],anchor,x,y,name))
		if opt[2]!=None: #display option's subtitle if there is any
			out.append("""<text class="option-sub-name" style="text-anchor: %s;" x="%f" y="%f">%s</text>""" % (anchor,x,y+13,opt[2]))
		out.append("""</g>""")
		degrees_arr.append(str(degrees))
		#setup a function (w/timeout) to call on a knob circle click (auto rotate) 
		fun_arr.append("setTimeout(() => {%s;}, 300);" % opt[3].replace("'", '&quot;')) 
	
	# knob's circle onclick rotation
	knob_onclick = """rotateNextKnob('""" +id+ """',["""
	knob_onclick += ",".join(degrees_arr)
	knob_onclick += """],[' """ 
	knob_onclick += "', '".join(fun_arr)
	knob_onclick += """ '])"""
	out.append("""<g transform="translate(%f,%f)" onclick="%s">""" % (cx, cy, knob_onclick))

	#draw lines
	for i in range(len(opts)):
		opt=opts[i]
		degrees=opt[0]
		anchor = "start" if degrees>0 else "end"
		x = rx if degrees>0 else -rx #line break x-pos
		x1 = x+8 if degrees>0 else x-8
		y = math.tan((90+abs(degrees)) * math.pi / 180)*rx #line break y-pos
		out.append("""<polyline class="line" points="0 0 %f %f %f %f"/>""" % (x,y,x1,y))		

	out.append("""<circle cx="0" cy="0" r="%f" class="circle-background" /><circle cx="0" cy="0" r="%f" class="circle-foreground" />""" % (r,r))
	out.append("""<g id="%s" class="knob-arrow %s" style="transform: rotate(var(--%s-knob-rotation));" data-initial-rotation="%u"><polygon points="0 2 2 1 2 -%f 0 -%f -2 -%f -2 1" /></g>""" % (id,store_cls,id,opts[0][0],r+1,r+3,r+1))
	out.append("""</g></svg>""")
	out.insert(0,"""<svg viewbox="0 0 %f %f" width="%f" height="%f" class="knob-selector" xmlns="http://www.w3.org/2000/svg">""" % (w,h,w,h))
	return "\n".join(out)

#Helper for generating server chart's time-selector knob
def html_knob_selector_chart_range(id):
	options=[(-135,"2 days",  None,"AcidChartSetRange('%s',0)" % id),
			(-45,"2 weeks", None,"AcidChartSetRange('%s',1)" % id),
			(45,  "3 months",None,"AcidChartSetRange('%s',2)" % id),
			(135, "10 years",None,"AcidChartSetRange('%s',3)" % id)]
	return html_knob_selector(id,9,(170,34),(85,16),options,False)

# Generates a full span+svg tag with an icon
def html_icon(id, scale=1, cls=''):
	return '<span class="icon"><svg height="12px" width="12px"><use class="%s" transform="scale(%f)" xlink:href="#%s"/></svg></span>' % (cls,scale,id)

# Hangles multiple, concurrent commands to many servers at once
class MFSMultiConn:
	# Initializes class, sets timeout for a single task
	def __init__(self,timeout=5):
		self.timeout = timeout
		self.addresses = []

	# Registers a server (host,port), to send a command to
	def register(self,host,port):
		self.addresses.append((host,port))

	# Sends a given command asynchrously to all (previously) registered servers and applies the global timeout on all of them.
	# It is ensured that this function completes within give timeout
	# returns a dictionary hostkey->(datain, length) where hostkey is 'ip:port' (eg. '10.10.10.12:9422), 
	# if a given server can't be reached or gives no valid answer its hostkey will be absent from the dictionary, thus dictinary may be empty if no command completed successfuly 
	def command(self,cmdout,cmdin,dataout=None):
		if (sys.version_info[0]==3 and sys.version_info[1]<7):
			loop = asyncio.get_event_loop()
			return loop.run_until_complete(self.async_command(cmdout,cmdin,dataout))
		else:
			return asyncio.run(self.async_command(cmdout,cmdin,dataout))

	# Internal: coordinates async tasks and collects results
	async def async_command(self,cmdout,cmdin,dataout):
		results = []
		# Create a list of tasks
		tasks = [self.single_command(host, port, cmdout,cmdin,dataout) for host, port in self.addresses]
		# Schedule tasks
		if (sys.version_info[0]==3 and sys.version_info[1]<7):
			loop = asyncio.get_event_loop()
			tasks = [loop.create_task(task) for task in tasks]
		else:
			tasks = [asyncio.create_task(task) for task in tasks]
		try:
			# Attempt to wait for all tasks with a timeout
			done, pending = await asyncio.wait(tasks, timeout=self.timeout)
			for task in done:
				if not task.cancelled() and not task.exception():
					results.append(task.result()) # Collect a single task output
		except asyncio.TimeoutError:
			raise Exception("Timeout during executing single_command")  # This line will not be hit using this structure.
		for task in pending:
			task.cancel() # Cancel pending tasks after timeout
			try:
				await task
			except asyncio.CancelledError:
				pass #do nothing about it
		return dict(results) # Convert list of tuples (successfull task results) to dictionary
	
	# Internal: perform a single mfs i/o command 
	async def single_command(self,host,port,cmdout,cmdin,dataout):
		if dataout:
			l = len(dataout)
			msg = struct.pack(">LL",cmdout,l) + dataout
		else:
			msg = struct.pack(">LL",cmdout,0)
		reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port),timeout=2)
		cmdok = 0
		errcnt = 0
		try: 
			while cmdok==0:
				badans = 0
				try:
					writer.write(msg)
					await writer.drain()
					while cmdok==0:
						header = await reader.readexactly(8)
						if header:
							cmd,length = struct.unpack(">LL",header)
							if cmd==cmdin:
								datain = await reader.readexactly(length)
								cmdok = 1
							elif cmd!=ANTOAN_NOP:
								badans = 1
								raise Exception
						else:
							raise Exception
				except Exception:
					if errcnt<3:
						writer.close()
						if not (sys.version_info[0]==3 and sys.version_info[1]<7):
							await writer.wait_closed()
						reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port),timeout=3)
						errcnt+=1
					else:
						if badans:
							raise RuntimeError("MFS communication error - bad answer")
						else:
							raise RuntimeError("MFS communication error")
		finally: #clean up whatever happened
			writer.close()
			if not (sys.version_info[0]==3 and sys.version_info[1]<7):
				await writer.wait_closed()
		hostkey = "%s:%u" % (host,port)
		return hostkey,(datain,length)

class MFSConn:
	def __init__(self,host,port):
		self.host = host
		self.port = port
		self.socket = None
		self.connect()
	def __del__(self):
		try:
			if self.socket:
				self.socket.close()
#				print "connection closed with: %s:%u" % (self.host,self.port)
			self.socket = None
		except AttributeError:
			pass
	def connect(self):
		cnt = 0
		while self.socket == None and cnt<3:
			self.socket = socket.socket()
			self.socket.settimeout(1)
			try:
				self.socket.connect((self.host,self.port))
			except Exception:
				self.socket.close()
				self.socket = None
				cnt += 1
		if self.socket==None:
			self.socket = socket.socket()
			self.socket.settimeout(1)
			self.socket.connect((self.host,self.port))
#		else:
#			print "connected to: %s:%u" % (self.host,self.port)
	def close(self):
		if self.socket:
			self.socket.close()
			self.socket = None
	def mysend(self,msg):
		if self.socket == None:
			self.connect()
		totalsent = 0
		while totalsent < len(msg):
			sent = self.socket.send(msg[totalsent:])
			if sent == 0:
				raise RuntimeError("socket connection broken")
			totalsent = totalsent + sent
	def myrecv(self,leng):
		msg = bytes(0)
		while len(msg) < leng:
			chunk = self.socket.recv(leng-len(msg))
			if len(chunk) == 0:
				raise RuntimeError("socket connection broken")
			msg = msg + chunk
		return msg
	def command(self,cmdout,cmdin,dataout=None):
		if dataout:
			l = len(dataout)
			msg = struct.pack(">LL",cmdout,l) + dataout
		else:
			msg = struct.pack(">LL",cmdout,0)
		cmdok = 0
		errcnt = 0
		while cmdok==0:
			badans = 0
			try:
				self.mysend(msg)
				while cmdok==0:
					header = self.myrecv(8)
					cmd,length = struct.unpack(">LL",header)
					if cmd==cmdin:
						datain = self.myrecv(length)
						cmdok = 1
					elif cmd!=ANTOAN_NOP:
						badans = 1
						raise Exception
			except Exception:
				if errcnt<3:
					self.close()
					self.connect()
					errcnt+=1
				else:
					if badans:
						raise RuntimeError("MFS communication error - bad answer")
					else:
						raise RuntimeError("MFS communication error")
		return datain,length

class Master(MFSConn):
	def __init__(self,host,port):
		MFSConn.__init__(self,host,port)
		self.version = (0,0,0)
		self.pro = -1
		self.featuremask = 0
	def set_version(self,version):
		self.version,self.pro = version_convert(version)
		if self.version>=(3,0,72):
			self.featuremask |= (1<<FEATURE_EXPORT_UMASK)
		if (self.version>=(3,0,112) and self.version[0]==3) or self.version>=(4,21,0):
			self.featuremask |= (1<<FEATURE_EXPORT_DISABLES)
		if self.version>=(4,27,0):
			self.featuremask |= (1<<FEATURE_SESSION_STATS_28)
		if self.version>=(4,29,0):
			self.featuremask |= (1<<FEATURE_INSTANCE_NAME)
		if self.version>=(4,35,0):
			self.featuremask |= (1<<FEATURE_CSLIST_MODE)
		if self.version>=(4,44,0):
			self.featuremask |= (1<<FEATURE_SCLASS_IN_MATRIX)
		if self.version>=(4,51,0):
			self.featuremask |= (1<<FEATURE_DEFAULT_GRACEPERIOD)
		if self.version>=(4,53,0):
			self.featuremask |= (1<<FEATURE_LABELMODE_OVERRIDES)
		if self.version>=(4,57,0):
			self.featuremask |= (1<<FEATURE_SCLASSGROUPS)
	def version_at_least(self,v1,v2,v3):
		return (self.version>=(v1,v2,v3))
	def version_less_than(self,v1,v2,v3):
		return (self.version<(v1,v2,v3))
	def version_is(self,v1,v2,v3):
		return (self.version==(v1,v2,v3))
	def version_unknown(self):
		return (self.version==(0,0,0))
	def is_pro(self):
		return self.pro
	def has_feature(self,featureid):
		return True if (self.featuremask & (1<<featureid)) else False
	def sort_ver(self):
		sortver = "%05u_%03u_%03u" % self.version
		if self.pro==1:
			sortver += "_2"
		elif self.pro==0:
			sortver += "_1"
		else:
			sortver += "_0"
		return sortver

class ExportsEntry:
	def __init__(self,fip1,fip2,fip3,fip4,tip1,tip2,tip3,tip4,path,meta,v1,v2,v3,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mingoal,maxgoal,mintrashretention,maxtrashretention,disables):
		self.ipfrom = (fip1,fip2,fip3,fip4)
		self.ipto = (tip1,tip2,tip3,tip4)
		self.version = (v1,v2,v3)
		self.stripfrom = "%u.%u.%u.%u" % (fip1,fip2,fip3,fip4)
		self.sortipfrom = "%03u_%03u_%03u_%03u" % (fip1,fip2,fip3,fip4)
		self.stripto = "%u.%u.%u.%u" % (tip1,tip2,tip3,tip4)
		self.sortipto = "%03u_%03u_%03u_%03u" % (tip1,tip2,tip3,tip4)
		self.strver,self.sortver = version_str_and_sort((v1,v2,v3))
		self.meta = meta
		self.path = path
		self.exportflags = exportflags
		self.sesflags = sesflags
		self.umaskval = umaskval
		self.rootuid = rootuid
		self.rootgid = rootgid
		self.mapalluid = mapalluid
		self.mapallgid = mapallgid
		if sclassgroups==None:
			if mingoal==None and maxgoal==None:
				self.sclassgroups = 0xFFFF
			else:
				self.sclassgroups = ((0xFFFF<<mingoal) & (((0xFFFF<<(maxgoal+1))&0xFFFF)^0xFFFF)) | 1
		else:
			self.sclassgroups = sclassgroups
		self.mintrashretention = mintrashretention
		self.maxtrashretention = maxtrashretention
		self.disables = disables

class Session:
	def __init__(self,sessionid,ip1,ip2,ip3,ip4,info,openfiles,nsocks,expire,v1,v2,v3,meta,path,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mingoal,maxgoal,mintrashretention,maxtrashretention,disables,stats_c,stats_l):
		self.ip = (ip1,ip2,ip3,ip4)
		self.version = (v1,v2,v3)
		self.strip = "%u.%u.%u.%u" % (ip1,ip2,ip3,ip4)
		self.sortip = "%03u_%03u_%03u_%03u" % (ip1,ip2,ip3,ip4)
		self.strver,self.sortver = version_str_and_sort((v1,v2,v3))
		self.host = resolve(self.strip)
		self.sessionid = sessionid
		self.info = info
		self.openfiles = openfiles
		self.nsocks = nsocks
		self.expire = expire
		self.meta = meta
		self.path = path
		self.sesflags = sesflags
		self.umaskval = umaskval
		self.rootuid = rootuid
		self.rootgid = rootgid
		self.mapalluid = mapalluid
		self.mapallgid = mapallgid
		if sclassgroups==None:
			if mingoal==None and maxgoal==None:
				self.sclassgroups = 0xFFFF
			else:
				self.sclassgroups = ((0xFFFF<<mingoal) & (((0xFFFF<<(maxgoal+1))&0xFFFF)^0xFFFF)) | 1
		else:
			self.sclassgroups = sclassgroups
		self.mintrashretention = mintrashretention
		self.maxtrashretention = maxtrashretention
		self.disables = disables
		self.stats_c = stats_c
		self.stats_l = stats_l

class ChunkServer:
	def __init__(self,oip1,oip2,oip3,oip4,ip1,ip2,ip3,ip4,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,labels,mfrstatus,maintenanceto):
		self.oip = (oip1,oip2,oip3,oip4)
		self.ip = (ip1,ip2,ip3,ip4)
		self.version = (v1,v2,v3)
		self.stroip = "%u.%u.%u.%u" % (oip1,oip2,oip3,oip4)
		self.strip = "%u.%u.%u.%u" % (ip1,ip2,ip3,ip4)
		self.sortip = "%03u_%03u_%03u_%03u" % (ip1,ip2,ip3,ip4)
		self.strver,self.sortver = version_str_and_sort((v1,v2,v3))
		self.host = resolve(self.strip)
		self.port = port
		self.csid = csid
		self.flags = flags
		self.used = used
		self.total = total
		self.chunks = chunks
		self.tdused = tdused
		self.tdtotal = tdtotal
		self.tdchunks = tdchunks
		self.errcnt = errcnt
		self.load = load
		self.gracetime = gracetime
		self.labels = labels
		self.mfrstatus = mfrstatus
		self.maintenanceto = maintenanceto
		self.hostkey = "%s:%s" % (self.strip,self.port)

class HDD:
	def __init__(self,hostkey,hoststr,hostip,port,hddpath,sortippath,ippath,hostpath,flags,clearerrorarg,errchunkid,errtime,used,total,chunkscnt,rbw,wbw,usecreadavg,usecwriteavg,usecfsyncavg,usecreadmax,usecwritemax,usecfsyncmax,rops,wops,fsyncops,rbytes,wbytes,mfrstatus):
		self.hostkey = hostkey
		self.hoststr = hoststr
		self.hostip = hostip
		self.port = port
		self.hddpath = hddpath
		self.sortippath = sortippath
		self.ippath = ippath
		self.hostpath = hostpath
		self.flags = flags
		self.clearerrorarg = clearerrorarg
		self.errchunkid = errchunkid
		self.errtime = errtime
		self.used = used
		self.total = total
		self.chunkscnt = chunkscnt
		self.rbw = rbw
		self.wbw = wbw
		self.usecreadavg = usecreadavg
		self.usecwriteavg = usecwriteavg
		self.usecfsyncavg = usecfsyncavg
		self.usecreadmax = usecreadmax
		self.usecwritemax = usecwritemax
		self.usecfsyncmax = usecfsyncmax
		self.rops = rops
		self.wops = wops
		self.fsyncops = fsyncops
		self.rbytes = rbytes
		self.wbytes = wbytes
		self.mfrstatus = mfrstatus
	def __lt__(self, other):
		return self.sortippath < other.sortippath
		
class StorageClass:
	def __init__(self,name,has_chunks):
		self.name = name
		self.has_chunks = has_chunks

class DataProvider:
	def __init__(self,masterconn):
		self.masterconn = masterconn
		self.sessions = None
		self.stats_to_show = 16
		self.chunkservers = None
		self.exports = None
		self.storage_classes = None
		self.matrices = {}
		if self.masterconn!=None and self.masterconn.has_feature(FEATURE_SESSION_STATS_28):
			self.stats_to_show = 28

	def get_storage_classes(self):
		if self.storage_classes==None:
			self.storage_classes = {}
			data,length = self.masterconn.command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",128))
			pos = 0
			while pos < length:
				sclassid,sclassnleng = struct.unpack_from(">BB",data,pos)
				pos += 2
				sclassname = data[pos:pos+sclassnleng]
				sclassname = sclassname.decode('utf-8','replace')
				pos += sclassnleng
				has_chunks = struct.unpack_from(">B",data,pos)[0]
				pos += 1
				sclassobj = StorageClass(sclassname,has_chunks)
				self.storage_classes[sclassid] = sclassobj
		return self.storage_classes
	
	def get_exports(self):
		if self.exports==None:
			self.exports=[]
			if self.masterconn.has_feature(FEATURE_SCLASSGROUPS):
				data,length = self.masterconn.command(CLTOMA_EXPORTS_INFO,MATOCL_EXPORTS_INFO,struct.pack(">B",4))
			elif self.masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				data,length = self.masterconn.command(CLTOMA_EXPORTS_INFO,MATOCL_EXPORTS_INFO,struct.pack(">B",3))
			elif self.masterconn.has_feature(FEATURE_EXPORT_UMASK):
				data,length = self.masterconn.command(CLTOMA_EXPORTS_INFO,MATOCL_EXPORTS_INFO,struct.pack(">B",2))
			elif self.masterconn.version_at_least(1,6,26):
				data,length = self.masterconn.command(CLTOMA_EXPORTS_INFO,MATOCL_EXPORTS_INFO,struct.pack(">B",1))
			else:
				data,length = self.masterconn.command(CLTOMA_EXPORTS_INFO,MATOCL_EXPORTS_INFO)
			pos = 0
			while pos<length:
				fip1,fip2,fip3,fip4,tip1,tip2,tip3,tip4,pleng = struct.unpack(">BBBBBBBBL",data[pos:pos+12])
				pos+=12
				path = data[pos:pos+pleng]
				path = path.decode('utf-8','replace')
				pos+=pleng
				if self.masterconn.has_feature(FEATURE_SCLASSGROUPS):
					v1,v2,v3,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables = struct.unpack(">HBBBBHLLLLHLLL",data[pos:pos+38])
					pos+=38
					mingoal = None
					maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				elif self.masterconn.has_feature(FEATURE_EXPORT_DISABLES):
					v1,v2,v3,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention,disables = struct.unpack(">HBBBBHLLLLBBLLL",data[pos:pos+38])
					pos+=38
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				elif self.masterconn.has_feature(FEATURE_EXPORT_UMASK):
					v1,v2,v3,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention = struct.unpack(">HBBBBHLLLLBBLL",data[pos:pos+34])
					pos+=34
					disables = 0
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				elif self.masterconn.version_at_least(1,6,26):
					v1,v2,v3,exportflags,sesflags,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention = struct.unpack(">HBBBBLLLLBBLL",data[pos:pos+32])
					pos+=32
					disables = 0
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
					umaskval = None
				else:
					v1,v2,v3,exportflags,sesflags,rootuid,rootgid,mapalluid,mapallgid = struct.unpack(">HBBBBLLLL",data[pos:pos+22])
					pos+=22
					disables = 0
					sclassgroups = None
					mingoal = None
					maxgoal = None
					mintrashretention = None
					maxtrashretention = None
					umaskval = None
				if path=='.':
					meta = 1
					umaskval = None
					disables = 0
				else:
					meta = 0
				expent = ExportsEntry(fip1,fip2,fip3,fip4,tip1,tip2,tip3,tip4,path,meta,v1,v2,v3,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mingoal,maxgoal,mintrashretention,maxtrashretention,disables)
				self.exports.append(expent)
		return self.exports
	
	def get_sessions(self):
		if self.sessions==None:
			self.sessions=[]
			if self.masterconn.has_feature(FEATURE_SCLASSGROUPS):
				data,length = self.masterconn.command(CLTOMA_SESSION_LIST,MATOCL_SESSION_LIST,struct.pack(">B",5))
			elif self.masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				data,length = self.masterconn.command(CLTOMA_SESSION_LIST,MATOCL_SESSION_LIST,struct.pack(">B",4))
			elif self.masterconn.has_feature(FEATURE_EXPORT_UMASK):
				data,length = self.masterconn.command(CLTOMA_SESSION_LIST,MATOCL_SESSION_LIST,struct.pack(">B",3))
			elif self.masterconn.version_at_least(1,7,8):
				data,length = self.masterconn.command(CLTOMA_SESSION_LIST,MATOCL_SESSION_LIST,struct.pack(">B",2))
			elif self.masterconn.version_at_least(1,6,26):
				data,length = self.masterconn.command(CLTOMA_SESSION_LIST,MATOCL_SESSION_LIST,struct.pack(">B",1))
			else:
				data,length = self.masterconn.command(CLTOMA_SESSION_LIST,MATOCL_SESSION_LIST)
			if self.masterconn.version_less_than(1,6,21):
				statscnt = 16
				pos = 0
			elif self.masterconn.version_is(1,6,21):
				statscnt = 21
				pos = 0
			else:
				statscnt = struct.unpack(">H",data[0:2])[0]
				pos = 2
			while pos<length:
				if self.masterconn.version_at_least(1,7,8):
					sessionid,ip1,ip2,ip3,ip4,v1,v2,v3,openfiles,nsocks,expire,ileng = struct.unpack(">LBBBBHBBLBLL",data[pos:pos+25])
					pos+=25
				else:
					sessionid,ip1,ip2,ip3,ip4,v1,v2,v3,ileng = struct.unpack(">LBBBBHBBL",data[pos:pos+16])
					pos+=16
					openfiles = 0
					nsocks = 1
					expire = 0
				info = data[pos:pos+ileng]
				pos+=ileng
				pleng = struct.unpack(">L",data[pos:pos+4])[0]
				pos+=4
				path = data[pos:pos+pleng]
				pos+=pleng
				info = info.decode('utf-8','replace')
				path = path.decode('utf-8','replace')
				if self.masterconn.has_feature(FEATURE_SCLASSGROUPS):
					sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables = struct.unpack(">BHLLLLHLLL",data[pos:pos+33])
					pos+=33
					mingoal = None
					maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				elif self.masterconn.has_feature(FEATURE_EXPORT_DISABLES):
					sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention,disables = struct.unpack(">BHLLLLBBLLL",data[pos:pos+33])
					pos+=33
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				elif self.masterconn.has_feature(FEATURE_EXPORT_UMASK):
					sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention = struct.unpack(">BHLLLLBBLL",data[pos:pos+29])
					pos+=29
					disables = 0
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
				elif self.masterconn.version_at_least(1,6,26):
					sesflags,rootuid,rootgid,mapalluid,mapallgid,mingoal,maxgoal,mintrashretention,maxtrashretention = struct.unpack(">BLLLLBBLL",data[pos:pos+27])
					pos+=27
					disables = 0
					sclassgroups = None
					if mingoal<=1 and maxgoal>=9:
						mingoal = None
						maxgoal = None
					if mintrashretention==0 and maxtrashretention==0xFFFFFFFF:
						mintrashretention = None
						maxtrashretention = None
					umaskval = None
				else:
					sesflags,rootuid,rootgid,mapalluid,mapallgid = struct.unpack(">BLLLL",data[pos:pos+17])
					pos+=17
					disables = 0
					sclassgroups = None
					mingoal = None
					maxgoal = None
					mintrashretention = None
					maxtrashretention = None
					umaskval = None
				if statscnt<self.stats_to_show:
					stats_c = struct.unpack(">"+"L"*statscnt,data[pos:pos+4*statscnt])+(0,)*(self.stats_to_show-statscnt)
					pos+=statscnt*4
					stats_l = struct.unpack(">"+"L"*statscnt,data[pos:pos+4*statscnt])+(0,)*(self.stats_to_show-statscnt)
					pos+=statscnt*4
				else:
					stats_c = struct.unpack(">"+"L"*self.stats_to_show,data[pos:pos+4*self.stats_to_show])
					pos+=statscnt*4
					stats_l = struct.unpack(">"+"L"*self.stats_to_show,data[pos:pos+4*self.stats_to_show])
					pos+=statscnt*4
				if path=='.':
					meta=1
				else:
					meta=0
				ses = Session(sessionid,ip1,ip2,ip3,ip4,info,openfiles,nsocks,expire,v1,v2,v3,meta,path,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mingoal,maxgoal,mintrashretention,maxtrashretention,disables,stats_c,stats_l)
				self.sessions.append(ses)
		return self.sessions
	
	def get_chunkservers(self, order=0, rev=False):
		if self.chunkservers==None:
			self.chunkservers=[]
			if self.masterconn.has_feature(FEATURE_CSLIST_MODE):
				data,length = self.masterconn.command(CLTOMA_CSERV_LIST,MATOCL_CSERV_LIST,struct.pack(">B",1))
			else:
				data,length = self.masterconn.command(CLTOMA_CSERV_LIST,MATOCL_CSERV_LIST)
			if self.masterconn.version_at_least(4,35,0) and (length%77)==0:
				n = length//77
				for i in range(n):
					d = data[i*77:(i+1)*77]
					flags,v1,v2,v3,oip1,oip2,oip3,oip4,ip1,ip2,ip3,ip4,port,csid,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,labels,mfrstatus,maintenanceto = struct.unpack(">BBBBBBBBBBBBHHQQLQQLLLLLBL",d)
					cs = ChunkServer(oip1,oip2,oip3,oip4,ip1,ip2,ip3,ip4,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,labels,mfrstatus,maintenanceto)
					self.chunkservers.append(cs)
			elif self.masterconn.version_at_least(4,26,0) and self.masterconn.version_less_than(4,35,0) and (length%73)==0:
				n = length//73
				for i in range(n):
					d = data[i*73:(i+1)*73]
					flags,v1,v2,v3,oip1,oip2,oip3,oip4,ip1,ip2,ip3,ip4,port,csid,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,labels,mfrstatus = struct.unpack(">BBBBBBBBBBBBHHQQLQQLLLLLB",d)
					cs = ChunkServer(oip1,oip2,oip3,oip4,ip1,ip2,ip3,ip4,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,labels,mfrstatus,None)
					self.chunkservers.append(cs)
			elif self.masterconn.version_at_least(3,0,38) and self.masterconn.version_less_than(4,26,0) and (length%69)==0:
				n = length//69
				for i in range(n):
					d = data[i*69:(i+1)*69]
					flags,v1,v2,v3,ip1,ip2,ip3,ip4,port,csid,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,labels,mfrstatus = struct.unpack(">BBBBBBBBHHQQLQQLLLLLB",d)
					cs = ChunkServer(ip1,ip2,ip3,ip4,ip1,ip2,ip3,ip4,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,labels,mfrstatus,None)
					self.chunkservers.append(cs)
			elif self.masterconn.version_at_least(2,1,0) and self.masterconn.version_less_than(3,0,38) and (length%68)==0:
				n = length//68
				for i in range(n):
					d = data[i*68:(i+1)*68]
					flags,v1,v2,v3,ip1,ip2,ip3,ip4,port,csid,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,labels = struct.unpack(">BBBBBBBBHHQQLQQLLLLL",d)
					cs = ChunkServer(ip1,ip2,ip3,ip4,ip1,ip2,ip3,ip4,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,labels,None,None)
					self.chunkservers.append(cs)
			elif self.masterconn.version_at_least(1,7,25) and self.masterconn.version_less_than(2,1,0) and (length%64)==0:
				n = length//64
				for i in range(n):
					d = data[i*64:(i+1)*64]
					flags,v1,v2,v3,ip1,ip2,ip3,ip4,port,csid,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime = struct.unpack(">BBBBBBBBHHQQLQQLLLL",d)
					cs = ChunkServer(ip1,ip2,ip3,ip4,ip1,ip2,ip3,ip4,port,csid,v1,v2,v3,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,None,None,None)
					self.chunkservers.append(cs)
			elif self.masterconn.version_at_least(1,6,28) and self.masterconn.version_less_than(1,7,25) and (length%62)==0:
				n = length//62
				for i in range(n):
					d = data[i*62:(i+1)*62]
					disconnected,v1,v2,v3,ip1,ip2,ip3,ip4,port,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime = struct.unpack(">BBBBBBBBHQQLQQLLLL",d)
					cs = ChunkServer(ip1,ip2,ip3,ip4,ip1,ip2,ip3,ip4,port,csid,v1,v2,v3,1 if disconnected else 0,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,None,None,None)
					self.chunkservers.append(cs)
			elif self.masterconn.version_less_than(1,6,28) and (length%54)==0:
				n = length//54
				for i in range(n):
					d = data[i*54:(i+1)*54]
					disconnected,v1,v2,v3,ip1,ip2,ip3,ip4,port,used,total,chunks,tdused,tdtotal,tdchunks,errcnt = struct.unpack(">BBBBBBBBHQQLQQLL",d)
					cs = ChunkServer(ip1,ip2,ip3,ip4,ip1,ip2,ip3,ip4,port,None,v1,v2,v3,1 if disconnected else 0,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,None,None,None,None,None)
					self.chunkservers.append(cs)
		if order>0:
			self.chunkservers.sort(key=lambda entry: (entry.sortip, entry.port))
			if rev:
				self.chunkservers.reverse()
		return self.chunkservers

	def get_metaloggers(self, order=1, rev=False):
		servers = []
		data,length = masterconn.command(CLTOMA_MLOG_LIST,MATOCL_MLOG_LIST)
		if (length%8)==0:
			n = length//8
			for i in range(n):
				d = data[i*8:(i+1)*8]
				v1,v2,v3,ip1,ip2,ip3,ip4 = struct.unpack(">HBBBBBB",d)
				strip = "%u.%u.%u.%u" % (ip1,ip2,ip3,ip4)
				host = resolve(strip)
				sortip = "%03u_%03u_%03u_%03u" % (ip1,ip2,ip3,ip4)
				strver,sortver = version_str_and_sort((v1,v2,v3))
				sf = (ip1,ip2,ip3,ip4)
				if order==1:
					sf = host
				elif order==2:
					sf = sortip
				elif order==3:
					sf = sortver
				servers.append((sf,host,sortip,strip,sortver,strver))
			servers.sort()
			if rev:
				servers.reverse()
		return servers
	
	def get_matrix(self, sclassid):
		if not sclassid in self.matrices:
			if masterconn.version_less_than(1,7,0):
				data,length = masterconn.command(CLTOMA_CHUNKS_MATRIX,MATOCL_CHUNKS_MATRIX,struct.pack(">B",0))
				if length==484:
					self.matrices[sclassid] = []
					self.matrices[sclassid].append([])
					for i in range(11):
						self.matrices[sclassid][0].append(list(struct.unpack(">LLLLLLLLLLL",data[i*44:i*44+44])))
					data,length = masterconn.command(CLTOMA_CHUNKS_MATRIX,MATOCL_CHUNKS_MATRIX,struct.pack(">B",1))
					if length==484:
						self.matrices[sclassid].append([])
						for i in range(11):
							self.matrices[sclassid][1].append(list(struct.unpack(">LLLLLLLLLLL",data[i*44:i*44+44])))
				self.progressstatus = 0
			else:
				if masterconn.has_feature(FEATURE_SCLASS_IN_MATRIX):
					if sclassid>=0:
						data,length = masterconn.command(CLTOMA_CHUNKS_MATRIX,MATOCL_CHUNKS_MATRIX,struct.pack(">BB",0,sclassid))
					else:
						data,length = masterconn.command(CLTOMA_CHUNKS_MATRIX,MATOCL_CHUNKS_MATRIX)
				else:
					if sclassid>=0:
						raise RuntimeError("storage class id in matrix is not supported by your master")
					data,length = masterconn.command(CLTOMA_CHUNKS_MATRIX,MATOCL_CHUNKS_MATRIX)
				if length==(1+484*2):
					self.progressstatus = struct.unpack(">B",data[0:1])[0]
					self.matrices[sclassid] = ([],[])
					for x in range(2):
						for i in range(11):
							self.matrices[sclassid][x].append(list(struct.unpack(">LLLLLLLLLLL",data[1+x*484+i*44:45+x*484+i*44])))
				elif length==(1+484*6):
					self.progressstatus = struct.unpack(">B",data[0:1])[0]
					self.matrices[sclassid] = ([],[],[],[],[],[])
					for x in range(6):
						for i in range(11):
							self.matrices[sclassid][x].append(list(struct.unpack(">LLLLLLLLLLL",data[1+x*484+i*44:45+x*484+i*44])))
				elif length==(1+484*8):
					self.progressstatus = struct.unpack(">B",data[0:1])[0]
					self.matrices[sclassid] = ([],[],[],[],[],[],[],[])
					for x in range(8):
						for i in range(11):
							self.matrices[sclassid][x].append(list(struct.unpack(">LLLLLLLLLLL",data[1+x*484+i*44:45+x*484+i*44])))
		return (self.matrices[sclassid], self.progressstatus)

	# Get list of hdds for chunk servers, 
	# HDdata - selects what disks should be returned: "ALL" - all, "ERR" - only with errors, "NOK" - only w/out ok status, "ip:port" - only for a given ip/port chunk server
	# returns tuple: (hdds,scanhdds)
	def get_hdds(self, HDdata, HDorder=0, HDrev=False):
		# get cs list
		hostlist = []
		multiconn = MFSMultiConn()
		for cs in self.get_chunkservers():
			if cs.port>0 and (cs.flags&1)==0:
				hostip = "%u.%u.%u.%u" % cs.ip
				hostkey = "%s:%u" % (hostip,cs.port)
				sortip = "%03u.%03u.%03u.%03u:%05u" % (cs.ip[0],cs.ip[1],cs.ip[2],cs.ip[3],cs.port)
				if HDdata=="ALL" or HDdata=="ERR" or HDdata=="NOK" or HDdata==hostkey:
					hostlist.append((hostkey,hostip,sortip,cs.port,cs.version,cs.mfrstatus))
					multiconn.register(hostip,cs.port) #register CS to send command to

		if len(hostlist)==0:
			return ([], [])

		# get hdd lists - from all cs at once
		answers=multiconn.command(CLTOCS_HDD_LIST,CSTOCL_HDD_LIST) #ask all registered chunk servers about hdds
		hdds = []
		scanhdds = []
		for hostkey,hostip,sortip,port,version,mfrstatus in hostlist:
			hoststr = resolve(hostip)
			if version<=(1,6,8):
				#HDD(hostkey,hoststr,hostip,port,hddpath,sortippath,ippath,hostpath,flags,clearerrorarg,errchunkid,errtime,used,total,chunkscnt,rbw,wbw,usecreadavg,usecwriteavg,usecfsyncavg,usecreadmax,usecwritemax,usecfsyncmax,rops,wops,fsyncops,rbytes,wbytes,mfrstatus)
				hdds.append((sortip,HDD(hostkey,hoststr,hostip,port,"",sortip,hostkey,hoststr,CS_HDD_TOO_OLD,0,0,0,0,0,0,[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],0)))
			elif hostkey not in answers:
				hdds.append((sortip,HDD(hostkey,hoststr,hostip,port,"",sortip,hostkey,hoststr,CS_HDD_UNREACHABLE,0,0,0,0,0,0,[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],0)))
			else:
				data,length = answers[hostkey]
				while length>0:
					entrysize = struct.unpack(">H",data[:2])[0]
					entry = data[2:2+entrysize]
					data = data[2+entrysize:]
					length -= 2+entrysize
					plen = entry[0]
					hddpathhex = entry[1:plen+1].hex()
					hddpath = entry[1:plen+1]
					hddpath = hddpath.decode('utf-8','replace')
					hostpath = "%s:%u:%s" % (hoststr,port,hddpath)
					ippath = "%s:%u:%s" % (hostip,port,hddpath)
					clearerrorarg = "%s:%u:%s" % (hostip,port,hddpathhex)
					sortippath = "%s:%s" % (sortip,hddpath)
					flags,errchunkid,errtime,used,total,chunkscnt = struct.unpack(">BQLQQL",entry[plen+1:plen+34])
					rbytes = [0,0,0]
					wbytes = [0,0,0]
					usecreadsum = [0,0,0]
					usecwritesum = [0,0,0]
					usecfsyncsum = [0,0,0]
					rops = [0,0,0]
					wops = [0,0,0]
					fsyncops = [0,0,0]
					usecreadmax = [0,0,0]
					usecwritemax = [0,0,0]
					usecfsyncmax = [0,0,0]
					if entrysize==plen+34+144:
						rbytes[0],wbytes[0],usecreadsum[0],usecwritesum[0],rops[0],wops[0],usecreadmax[0],usecwritemax[0] = struct.unpack(">QQQQLLLL",entry[plen+34:plen+34+48])
						rbytes[1],wbytes[1],usecreadsum[1],usecwritesum[1],rops[1],wops[1],usecreadmax[1],usecwritemax[1] = struct.unpack(">QQQQLLLL",entry[plen+34+48:plen+34+96])
						rbytes[2],wbytes[2],usecreadsum[2],usecwritesum[2],rops[2],wops[2],usecreadmax[2],usecwritemax[2] = struct.unpack(">QQQQLLLL",entry[plen+34+96:plen+34+144])
					elif entrysize==plen+34+192:
						rbytes[0],wbytes[0],usecreadsum[0],usecwritesum[0],usecfsyncsum[0],rops[0],wops[0],fsyncops[0],usecreadmax[0],usecwritemax[0],usecfsyncmax[0] = struct.unpack(">QQQQQLLLLLL",entry[plen+34:plen+34+64])
						rbytes[1],wbytes[1],usecreadsum[1],usecwritesum[1],usecfsyncsum[1],rops[1],wops[1],fsyncops[1],usecreadmax[1],usecwritemax[1],usecfsyncmax[1] = struct.unpack(">QQQQQLLLLLL",entry[plen+34+64:plen+34+128])
						rbytes[2],wbytes[2],usecreadsum[2],usecwritesum[2],usecfsyncsum[2],rops[2],wops[2],fsyncops[2],usecreadmax[2],usecwritemax[2],usecfsyncmax[2] = struct.unpack(">QQQQQLLLLLL",entry[plen+34+128:plen+34+192])
					rbw = [0,0,0]
					wbw = [0,0,0]
					usecreadavg = [0,0,0]
					usecwriteavg = [0,0,0]
					usecfsyncavg = [0,0,0]
					for i in range(3):
						if usecreadsum[i]>0:
							rbw[i] = rbytes[i]*1000000//usecreadsum[i]
						if usecwritesum[i]+usecfsyncsum[i]>0:
							wbw[i] = wbytes[i]*1000000//(usecwritesum[i]+usecfsyncsum[i])
						if rops[i]>0:
							usecreadavg[i] = usecreadsum[i]//rops[i]
						if wops[i]>0:
							usecwriteavg[i] = usecwritesum[i]//wops[i]
						if fsyncops[i]>0:
							usecfsyncavg[i] = usecfsyncsum[i]//fsyncops[i]
					sf = sortippath
					if HDorder==1:
						sf = sortippath
					elif HDorder==2:
						sf = chunkscnt
					elif HDorder==3:
						sf = errtime
					elif HDorder==4:
						sf = -flags
					elif HDorder==5:
						sf = rbw[HDperiod]
					elif HDorder==6:
						sf = wbw[HDperiod]
					elif HDorder==7:
						if HDtime==1:
							sf = usecreadavg[HDperiod]
						else:
							sf = usecreadmax[HDperiod]
					elif HDorder==8:
						if HDtime==1:
							sf = usecwriteavg[HDperiod]
						else:
							sf = usecwritemax[HDperiod]
					elif HDorder==9:
						if HDtime==1:
							sf = usecfsyncavg[HDperiod]
						else:
							sf = usecfsyncmax[HDperiod]
					elif HDorder==10:
						sf = rops[HDperiod]
					elif HDorder==11:
						sf = wops[HDperiod]
					elif HDorder==12:
						sf = fsyncops[HDperiod]
					elif HDorder==20:
						if flags&CS_HDD_SCANNING==0:
							sf = used
						else:
							sf = 0
					elif HDorder==21:
						if flags&CS_HDD_SCANNING==0:
							sf = total
						else:
							sf = 0
					elif HDorder==22:
						if flags&CS_HDD_SCANNING==0 and total>0:
							sf = (1.0*used)/total
						else:
							sf = 0
					if (HDdata=="ERR" and errtime>0) or (HDdata=="NOK" and flags!=0) or (HDdata!="ERR" and HDdata!="NOK"):
						hdd = HDD(hostkey,hoststr,hostip,port,hddpath,sortippath,ippath,hostpath,flags,clearerrorarg,errchunkid,errtime,used,total,chunkscnt,rbw,wbw,usecreadavg,usecwriteavg,usecfsyncavg,usecreadmax,usecwritemax,usecfsyncmax,rops,wops,fsyncops,rbytes,wbytes,mfrstatus)
						if flags&CS_HDD_SCANNING and not cgimode and ttymode:
							scanhdds.append((sf,hdd))
						else:
							hdds.append((sf,hdd))
		hdds.sort()
		scanhdds.sort()
		if HDrev:
			hdds.reverse()
			scanhdds.reverse()
		return (hdds, scanhdds)
	
	# Summarizes hdd info for given chunkserver,
	# returns tuple: string status of all disks (unknown or ok or warnings or errors) and a list of disks with a few basic fields only
	def cs_hdds_status(self, cs, hdds_all):
		hdds_out = []
		hdds_status = 'ok'
		for (_,hdd) in hdds_all:
			if cs.hostkey!=hdd.hostkey:
				continue
			if (hdd.flags&CS_HDD_INVALID):
				status = 'invalid'
				hdds_status = 'errors'
			elif hdd.flags&CS_HDD_SCANNING:
				status = 'scanning'
			elif (hdd.flags&CS_HDD_DAMAGED) and (hdd.flags&CS_HDD_SCANNING)==0 and (hdd.flags&CS_HDD_INVALID)==0:
				status = 'damaged'
				hdds_status = 'errors'
			elif hdd.flags&CS_HDD_MFR:
				status = 'marked for removal'
			elif hdd.flags==0:
				status = 'ok'
			else:
				status = 'unknown'
				if hdds_status!='errors':
					hdds_status = 'warnings' 
			if hdd.errtime>0:
				status = 'warning' 
				if hdds_status!='errors':
					hdds_status = 'warnings' 
			hdds_out.append({
				'flags': hdd.flags,
				'errtime': hdd.errtime,
				'status': status,
				'used': hdd.used,
				'total': hdd.total
				})
		return (hdds_status, hdds_out)
	#End of dataprovider
	####################	

def charts_convert_data(datalist,mul,div,raw):
	res = []
	nodata = (2**64)-1
	for v in datalist:
		if v==nodata:
			res.append(None)
		else:
			if raw:
				res.append(v)
			else:
				res.append((v*mul)/div)
	return res

def get_charts_multi_data(mfsconn,chartid,dataleng):
	data,length = mfsconn.command(CLTOAN_CHART_DATA,ANTOCL_CHART_DATA,struct.pack(">LLB",chartid,dataleng,1))
	if length>=8:
		ranges,series,entries,perc,base = struct.unpack(">BBLBB",data[:8])
		if length==8+ranges*(13+series*entries*8):
			res = {}
			unpackstr = ">%uQ" % entries
			for r in range(ranges):
				rpos = 8 + r * (13+series*entries*8)
				rng,ts,mul,div = struct.unpack(">BLLL",data[rpos:rpos+13])
				rpos += 13
				if series>3:
					series=3
				l1 = None
				l2 = None
				l3 = None
				if series>=1:
					l1 = list(struct.unpack(unpackstr,data[rpos:rpos+entries*8]))
				if series>=2:
					l2 = list(struct.unpack(unpackstr,data[rpos+entries*8:rpos+2*entries*8]))
				if series>=3:
					l3 = list(struct.unpack(unpackstr,data[rpos+2*entries*8:rpos+3*entries*8]))
				res[rng] = (l1,l2,l3,ts,mul,div)
			return perc,base,res
		else:
			return None,None,None
	else:
		return None,None,None

def create_chunks(list_name, n):
	listobj = list(list_name)
	for i in range(0, len(listobj), n):
		yield listobj[i:i + n]

def resolve_inodes_paths(masterconn,inodes):
	inodepaths = {}
	for chunk in create_chunks(inodes,100):
		if len(chunk)>0:
			data,length = masterconn.command(CLTOMA_MASS_RESOLVE_PATHS,MATOCL_MASS_RESOLVE_PATHS,struct.pack(">"+len(chunk)*"L",*chunk))
			pos = 0
			while pos+8<=length:
				inode,psize = struct.unpack(">LL",data[pos:pos+8])
				pos+=8
				if psize == 0:
					if inode not in inodepaths:
						inodepaths[inode] = []
					inodepaths[inode].append("./META")
				elif pos + psize <= length:
					while psize>=4:
						pleng = struct.unpack(">L",data[pos:pos+4])[0]
						pos+=4
						psize-=4
						path = data[pos:pos+pleng]
						pos+=pleng
						psize-=pleng
						path = path.decode('utf-8','replace')
						if inode not in inodepaths:
							inodepaths[inode] = []
						inodepaths[inode].append(path)
					if psize!=0:
						raise RuntimeError("MFS packet malformed")
			if pos!=length:
				raise RuntimeError("MFS packet malformed")
	return inodepaths


# find leader
leaderispro = 0
leaderfound = 0
deputyfound = 0
leaderinfo = None
leader_exportschecksum = None
leader_metaid = None
leader_usectime = None
leaderconn = None

electfound = 0
electinfo = None
elect_exportschecksum = None
elect_metaid = None
electconn = None

usurperfound = 0
usurperinfo = None
usurper_exportschecksum = None
usurper_metaid = None
usurperconn = None

followerfound = 0
followerinfo = None
follower_exportschecksum = None
follower_metaid = None
followerconn = None

dataprovider = None
masterlist = getmasteraddresses()
masterlistver = []
masterlistinfo = []

for mhost,mport in masterlist:
	conn = None
	version = (0,0,0)
	statestr = "???"
	statecolor = 1
	memusage = 0
	syscpu = 0
	usercpu = 0
	lastsuccessfulstore = 0
	lastsaveseconds = 0
	lastsavestatus = 0
	metaversion = 0
	exportschecksum = None
	metaid = None
	lastsavemetaversion = None
	lastsavemetachecksum = None
	usectime = None
	chlogtime = 0
	try:
		conn = Master(mhost,mport)
		try:
			data,length = conn.command(CLTOMA_INFO,MATOCL_INFO)
			if length==52:
				version = (1,4,0)
				conn.set_version(version)
				if leaderfound==0:
					leaderconn = conn
					leaderinfo = data
					leaderfound = 1
				statestr = "OLD MASTER (LEADER ONLY)"
				statecolor = 0
			elif length==60:
				version = (1,5,0)
				conn.set_version(version)
				if leaderfound==0:
					leaderconn = conn
					leaderinfo = data
					leaderfound = 1
				statestr = "OLD MASTER (LEADER ONLY)"
				statecolor = 0
			elif length==68 or length==76 or length==101:
				version = struct.unpack(">HBB",data[:4])
				conn.set_version(version)
				if leaderfound==0 and version<(1,7,0):
					leaderconn = conn
					leaderinfo = data
					leaderfound = 1
				if length==76:
					memusage = struct.unpack(">Q",data[4:12])[0]
				if length==101:
					memusage,syscpu,usercpu = struct.unpack(">QQQ",data[4:28])
					syscpu/=10000000.0
					usercpu/=10000000.0
					lastsuccessfulstore,lastsaveseconds,lastsavestatus = struct.unpack(">LLB",data[92:101])
				if version<(1,7,0):
					statestr = "OLD MASTER (LEADER ONLY)"
					statecolor = 0
				else:
					statestr = "UPGRADE THIS UNIT!"
					statecolor = 2
			elif length==121 or length==129 or length==137 or length==149 or length==173 or length==181 or length==193 or length==205:
				offset = 8 if (length>=137 and length!=173) else 0
				version = struct.unpack(">HBB",data[:4])
				conn.set_version(version)
				memusage,syscpu,usercpu = struct.unpack(">QQQ",data[4:28])
				syscpu/=10000000.0
				usercpu/=10000000.0
				if length==205:
					offset += 4
				lastsuccessfulstore,lastsaveseconds,lastsavestatus = struct.unpack(">LLB",data[offset+92:offset+101])
				if conn.version_at_least(2,0,14):
					lastsaveseconds = lastsaveseconds / 1000.0
				workingstate,nextstate,stablestate,sync,leaderip,changetime,metaversion = struct.unpack(">BBBBLLQ",data[offset+101:offset+121])
				if length>=129:
					exportschecksum = struct.unpack(">Q",data[offset+121:offset+129])[0]
				if length>=173:
					metaid,lastsavemetaversion,lastsavemetachecksum = struct.unpack(">QQL",data[offset+129:offset+149])
				if length==149 or length==193 or length==205:
					usectime,chlogtime = struct.unpack(">QL",data[length-12:length])
				if workingstate==0xFF and nextstate==0xFF and stablestate==0xFF and sync==0xFF:
					if leaderfound==0:
						leaderconn = conn
						leaderinfo = data
						leaderfound = 1
						leader_exportschecksum = exportschecksum
						leader_metaid = metaid
						leader_usectime = usectime
					statestr = "MASTER"
					statecolor = 0
				elif stablestate==0 or workingstate!=nextstate:
					statestr = "transition %s -> %s" % (state_name(workingstate),state_name(nextstate))
					statecolor = 8
				else:
					statestr = state_name(workingstate)
					statecolor = state_color(workingstate,sync)
					if workingstate==STATE_FOLLOWER:
						if sync==0:
							statestr += " (DESYNC)"
						if sync==2:
							statestr += " (DELAYED)"
						if sync==3:
							statestr += " (INIT)"
						followerfound = 1
						followerconn = conn
						followerinfo = data
						follower_exportschecksum = exportschecksum
						follower_metaid = metaid
					if workingstate==STATE_USURPER and usurperfound==0:
						usurperfound = 1
						usurperconn = conn
						usurperinfo = data
						usurper_exportschecksum = exportschecksum
						usurper_metaid = metaid
					if workingstate==STATE_ELECT and electfound==0:
						electfound = 1
						electconn = conn
						electinfo = data
						elect_exportschecksum = exportschecksum
						elect_metaid = metaid
					if (workingstate==STATE_LEADER or workingstate==STATE_DEPUTY) and leaderfound==0:
						leaderispro = 1
						leaderconn = conn
						leaderinfo = data
						leaderfound = 1
						if (workingstate==STATE_DEPUTY):
							deputyfound = 1
						leader_exportschecksum = exportschecksum
						leader_metaid = metaid
						leader_usectime = usectime
		except Exception:
			statestr = "BUSY"
			statecolor = 7
	except Exception:
		statestr = "UNREACHABLE"
	try:
		iptab = tuple(map(int,mhost.split('.')))
		strip = "%u.%u.%u.%u" % iptab
		sortip = "%03u_%03u_%03u_%03u" % iptab
	except Exception:
		strip = mhost
		sortip = mhost
	strver,sortver = version_str_and_sort(version)
	if conn and conn!=leaderconn and conn!=electconn and conn!=usurperconn and conn!=followerconn:
		del conn
	masterlistver.append((mhost,mport,version))
	masterlistinfo.append((sortip,strip,sortver,strver,statestr,statecolor,metaversion,memusage,syscpu,usercpu,lastsuccessfulstore,lastsaveseconds,lastsavestatus,exportschecksum,metaid,lastsavemetaversion,lastsavemetachecksum,usectime,chlogtime))

if leaderfound:
	masterconn = leaderconn
	masterinfo = leaderinfo
	masterispro = leaderispro
	master_exportschecksum = leader_exportschecksum
	master_metaid = leader_metaid
elif electfound:
	masterconn = electconn
	masterinfo = electinfo
	masterispro = 1
	master_exportschecksum = elect_exportschecksum
	master_metaid = elect_metaid
elif usurperfound:
	masterconn = usurperconn
	masterinfo = usurperinfo
	masterispro = 1
	master_exportschecksum = usurper_exportschecksum
	master_metaid = usurper_metaid
elif followerfound:
	masterconn = followerconn
	masterinfo = followerinfo
	masterispro = 1
	master_exportschecksum = follower_exportschecksum
	master_metaid = follower_metaid
else:
	masterconn = None
	master_exportschecksum = 0
	master_metaid = 0
	for sortip,strip,sortver,strver,statestr,statecolor,metaversion,memusage,syscpu,usercpu,lastsuccessfulstore,lastsaveseconds,lastsavestatus,exportschecksum,metaid,lastsavemetaversion,lastsavemetachecksum,usectime,chlogtime in masterlistinfo:
		if exportschecksum!=None:
			master_exportschecksum |= exportschecksum
		if metaid!=None:
			master_metaid |= metaid

master_minusectime = None
master_maxusectime = None
if leader_usectime==None or leader_usectime==0:
	for sortip,strip,sortver,strver,statestr,statecolor,metaversion,memusage,syscpu,usercpu,lastsuccessfulstore,lastsaveseconds,lastsavestatus,exportschecksum,metaid,lastsavemetaversion,lastsavemetachecksum,usectime,chlogtime in masterlistinfo:
		if usectime!=None and usectime>0:
			if master_minusectime==None or usectime<master_minusectime:
				master_minusectime = usectime
			if master_maxusectime==None or usectime>master_maxusectime:
				master_maxusectime = usectime

if leaderfound and masterconn.version_less_than(1,6,10):
	if cgimode:
		print("Content-Type: text/html; charset=UTF-8")
		print("")
		print("""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">""")
		print("""<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">""")
		print("""<head>""")
		print("""<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />""")
		print("""<title>MFS Info (%s)</title>""" % (htmlentities(mastername)))
		# leave this script a the begining to prevent screen blinking then using dark mode
		print("""<script type="text/javascript"><!--//--><![CDATA[//><!--
			if (localStorage.getItem('theme')===null || localStorage.getItem('theme')==='dark') { document.documentElement.setAttribute('data-theme', 'dark');}	
			//--><!]]></script>""")		
		print("""<link rel="stylesheet" href="mfs.css" type="text/css" />""")
		print("""</head>""")
		print("""<body>""")
		if masterconn.version_unknown():
			print("""<h1 align="center">Can't detect MFS master version</h1>""")
		else:
			print("""<h1 align="center">MFS master version not supported (pre 1.6.10)</h1>""")
		print("""</body>""")
		print("""</html>""")
	elif jsonmode:
		if masterconn.version_unknown():
			jcollect["errors"].append("Can't detect MFS master version")
		else:
			jcollect["errors"].append("MFS master version not supported (pre 1.6.10)")
	else:
		if masterconn.version_unknown():
			print("Can't detect MFS master version")
		else:
			print("MFS master version not supported (pre 1.6.10)")
	sys.exit(1)


dataprovider = DataProvider(masterconn)

# commands
if cgimode:
	# commands in CGI mode
	cmd_success = -1
	if "CSremove" in fields:
		cmd_success = 0
		tracedata = ""
		if leaderfound:
			try:
				serverdata = fields.getvalue("CSremove").split(":")
				if len(serverdata)==2:
					csip = list(map(int,serverdata[0].split(".")))
					csport = int(serverdata[1])
					if len(csip)==4:
						if masterconn.version_less_than(1,6,28):
							data,length = masterconn.command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBH",csip[0],csip[1],csip[2],csip[3],csport))
							if length==0:
								cmd_success = 1
								status = 0
						else:
							data,length = masterconn.command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_REMOVE,csip[0],csip[1],csip[2],csip[3],csport))
							if length==1:
								status = (struct.unpack(">B",data))[0]
								cmd_success = 1
			except Exception:
				tracedata = traceback.format_exc()
		url = createrawlink({"CSremove":""})
	elif "CSbacktowork" in fields:
		cmd_success = 0
		tracedata = ""
		if leaderfound and masterconn.version_at_least(1,6,28):
			try:
				serverdata = fields.getvalue("CSbacktowork").split(":")
				if len(serverdata)==2:
					csip = list(map(int,serverdata[0].split(".")))
					csport = int(serverdata[1])
					if len(csip)==4:
						data,length = masterconn.command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_BACKTOWORK,csip[0],csip[1],csip[2],csip[3],csport))
						if length==1:
							status = (struct.unpack(">B",data))[0]
							cmd_success = 1
			except Exception:
				tracedata = traceback.format_exc()
		url = createrawlink({"CSbacktowork":""})
	elif "CSmaintenanceon" in fields:
		cmd_success = 0
		tracedata = ""
		if leaderfound and masterconn.version_at_least(2,0,11):
			try:
				serverdata = fields.getvalue("CSmaintenanceon").split(":")
				if len(serverdata)==2:
					csip = list(map(int,serverdata[0].split(".")))
					csport = int(serverdata[1])
					if len(csip)==4:
						data,length = masterconn.command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_MAINTENANCEON,csip[0],csip[1],csip[2],csip[3],csport))
						if length==1:
							status = (struct.unpack(">B",data))[0]
							cmd_success = 1
			except Exception:
				tracedata = traceback.format_exc()
		url = createrawlink({"CSmaintenanceon":""})
	elif "CSmaintenanceoff" in fields:
		cmd_success = 0
		tracedata = ""
		if leaderfound and masterconn.version_at_least(2,0,11):
			try:
				serverdata = fields.getvalue("CSmaintenanceoff").split(":")
				if len(serverdata)==2:
					csip = list(map(int,serverdata[0].split(".")))
					csport = int(serverdata[1])
					if len(csip)==4:
						data,length = masterconn.command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_MAINTENANCEOFF,csip[0],csip[1],csip[2],csip[3],csport))
						if length==1:
							status = (struct.unpack(">B",data))[0]
							cmd_success = 1
			except Exception:
				tracedata = traceback.format_exc()
		url = createrawlink({"CSmaintenanceoff":""})
	elif "MSremove" in fields:
		cmd_success = 0
		tracedata = ""
		if leaderfound:
			try:
				sessionid = int(fields.getvalue("MSremove"))
				data,length = masterconn.command(CLTOMA_SESSION_COMMAND,MATOCL_SESSION_COMMAND,struct.pack(">BL",MFS_SESSION_COMMAND_REMOVE,sessionid))
				if length==1:
					status = (struct.unpack(">B",data))[0]
					cmd_success = 1
			except Exception:
				tracedata = traceback.format_exc()
		url = createrawlink({"MSremove":""})
	elif "CSclearerrors" in fields:
		cmd_success = 0
		tracedata = ""
		try:
			serverdata = fields.getvalue("CSclearerrors").split(":")
			csip = serverdata[0]
			csport = int(serverdata[1])
			pathhex = serverdata[2]
			pleng = len(pathhex)>>1
			pathlist = []
			for x in range(pleng):
				pathlist.append(int(pathhex[2*x:2*x+2],16))
			dout = struct.pack(">L%uB" % pleng,pleng,*pathlist)
			conn = MFSConn(csip,csport)
			data,length = conn.command(ANTOCS_CLEAR_ERRORS,CSTOAN_CLEAR_ERRORS,dout)
			del conn
			if length==1:
				res = (struct.unpack(">B",data))[0]
				if res>0:
					cmd_success = 1
		except Exception:
			tracedata = traceback.format_exc()
		url = createrawlink({"CSclearerrors":""})
	if cmd_success==1:
		print("Status: 302 Found")
		print("Location: %s" % url)
		print("Content-Type: text/html; charset=UTF-8")
		print("")
		print("""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">""")
		print("""<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">""")
		print("""<head>""")
		print("""<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />""")
		print("""<meta http-equiv="Refresh" content="0; url=%s" />""" % url.replace('&','&amp;'))
		print("""<title>MFS Info (%s)</title>""" % (htmlentities(mastername)))
		# leave this script a the begining to prevent screen blinking then using dark mode
		print("""<script type="text/javascript"><!--//--><![CDATA[//><!--
			if (localStorage.getItem('theme')===null || localStorage.getItem('theme')==='dark') { document.documentElement.setAttribute('data-theme', 'dark');}	
			//--><!]]></script>""")		
		print("""<link rel="stylesheet" href="mfs.css" type="text/css" />""")
		print("""</head>""")
		print("""<body>""")
		print("""<h1 align="center"><a href="%s">If you see this then it means that redirection didn't work, so click here</a></h1>""" % url)
		print("""</body>""")
		print("""</html>""")
		sys.exit(0)
	elif cmd_success==0:
		print("Content-Type: text/html; charset=UTF-8")
		print("")
		print("""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">""")
		print("""<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">""")
		print("""<head>""")
		print("""<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />""")
		print("""<meta http-equiv="Refresh" content="5; url=%s" />""" % url.replace('&','&amp;'))
		print("""<title>MFS Info (%s)</title>""" % (htmlentities(mastername)))
		# leave this script a the begining to prevent screen blinking when using dark mode
		print("""<script type="text/javascript"><!--//--><![CDATA[//><!--
			if (localStorage.getItem('theme')===null || localStorage.getItem('theme')==='dark') { document.documentElement.setAttribute('data-theme', 'dark');}	
			//--><!]]></script>""")		
		print("""<link rel="stylesheet" href="mfs.css" type="text/css" />""")
		print("""</head>""")
		print("""<body>""")
		print("""<h3 align="center">Can't perform command - wait 5 seconds for refresh</h3>""")
		if tracedata:
			print("""<hr />""")
			print("""<pre>%s</pre>""" % tracedata)
		print("""</body>""")
		print("""</html>""")
		sys.exit(0)
else:
	if leaderfound:
		for cmd in clicommands:
			cmddata = cmd.split('/')
			if cmddata[0]=='RC':
				cmd_success = 0
				try:
					csip = list(map(int,cmddata[1].split(".")))
					csport = int(cmddata[2])
					if len(csip)==4:
						if masterconn.version_less_than(1,6,28):
							data,length = masterconn.command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBH",csip[0],csip[1],csip[2],csip[3],csport))
							if length==0:
								cmd_success = 1
								status = 0
						else:
							data,length = masterconn.command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_REMOVE,csip[0],csip[1],csip[2],csip[3],csport))
							if length==1:
								status = (struct.unpack(">B",data))[0]
								cmd_success = 1
					if cmd_success:
						if status==STATUS_OK:
							print("Chunkserver %s/%s has been removed" % (cmddata[1],cmddata[2]))
						elif status==ERROR_NOTFOUND:
							print("Chunkserver %s/%s hasn't been found" % (cmddata[1],cmddata[2]))
						elif status==ERROR_ACTIVE:
							print("Chunkserver %s/%s can't be removed because is still active" % (cmddata[1],cmddata[2]))
						else:
							print("Can't remove chunkserver %s/%s (status:%u)" % (cmddata[1],cmddata[2],status))
					else:
						print("Can't remove chunkserver %s/%s" % (cmddata[1],cmddata[2]))
				except Exception:
					print_exception()
			if cmddata[0]=='BW':
				cmd_success = 0
				try:
					csip = list(map(int,cmddata[1].split(".")))
					csport = int(cmddata[2])
					if len(csip)==4:
						if masterconn.version_at_least(1,6,28):
							data,length = masterconn.command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_BACKTOWORK,csip[0],csip[1],csip[2],csip[3],csport))
							if length==1:
								status = (struct.unpack(">B",data))[0]
								cmd_success = 1
					if cmd_success:
						if status==STATUS_OK:
							print("Chunkserver %s/%s has back to work" % (cmddata[1],cmddata[2]))
						elif status==ERROR_NOTFOUND:
							print("Chunkserver %s/%s hasn't been found" % (cmddata[1],cmddata[2]))
						else:
							print("Can't turn chunkserver %s/%s back to work (status:%u)" % (cmddata[1],cmddata[2],status))
					else:
						print("Can't turn chunkserver %s/%s back to work" % (cmddata[1],cmddata[2]))
				except Exception:
					print_exception()
			if cmddata[0]=='M1':
				cmd_success = 0
				try:
					csip = list(map(int,cmddata[1].split(".")))
					csport = int(cmddata[2])
					if len(csip)==4:
						if masterconn.version_at_least(2,0,11):
							data,length = masterconn.command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_MAINTENANCEON,csip[0],csip[1],csip[2],csip[3],csport))
							if length==1:
								status = (struct.unpack(">B",data))[0]
								cmd_success = 1
					if cmd_success:
						if status==STATUS_OK:
							print("Chunkserver %s/%s has been switched to maintenance mode" % (cmddata[1],cmddata[2]))
						elif status==ERROR_NOTFOUND:
							print("Chunkserver %s/%s hasn't been found" % (cmddata[1],cmddata[2]))
						else:
							print("Can't switch chunkserver %s/%s to maintenance mode (status:%u)" % (cmddata[1],cmddata[2],status))
					else:
						print("Can't switch chunkserver %s/%s to maintenance mode" % (cmddata[1],cmddata[2]))
				except Exception:
					print_exception()
			if cmddata[0]=='M0':
				cmd_success = 0
				try:
					csip = list(map(int,cmddata[1].split(".")))
					csport = int(cmddata[2])
					if len(csip)==4:
						if masterconn.version_at_least(2,0,11):
							data,length = masterconn.command(CLTOMA_CSSERV_COMMAND,MATOCL_CSSERV_COMMAND,struct.pack(">BBBBBH",MFS_CSSERV_COMMAND_MAINTENANCEOFF,csip[0],csip[1],csip[2],csip[3],csport))
							if length==1:
								status = (struct.unpack(">B",data))[0]
								cmd_success = 1
					if cmd_success:
						if status==STATUS_OK:
							print("Chunkserver %s/%s has been switched to standard mode" % (cmddata[1],cmddata[2]))
						elif status==ERROR_NOTFOUND:
							print("Chunkserver %s/%s hasn't been found" % (cmddata[1],cmddata[2]))
						else:
							print("Can't switch chunkserver %s/%s to standard mode (status:%u)" % (cmddata[1],cmddata[2],status))
					else:
						print("Can't switch chunkserver %s/%s to standard mode" % (cmddata[1],cmddata[2]))
				except Exception:
					print_exception()
			if cmddata[0]=='RS':
				cmd_success = 0
				try:
					sessionid = int(cmddata[1])
					data,length = masterconn.command(CLTOMA_SESSION_COMMAND,MATOCL_SESSION_COMMAND,struct.pack(">BL",MFS_SESSION_COMMAND_REMOVE,sessionid))
					if length==1:
						status = (struct.unpack(">B",data))[0]
						cmd_success = 1
					if cmd_success:
						if status==STATUS_OK:
							print("Session %u has been removed" % (sessionid))
						elif status==ERROR_NOTFOUND:
							print("Session %u hasn't been found" % (sessionid))
						elif status==ERROR_ACTIVE:
							print("Session %u can't be removed because is still active" % (sessionid))
						else:
							print("Can't remove session %u (status:%u)" % (sessionid,status))
					else:
						print("Can't remove session %u" % (sessionid))
				except Exception:
					print_exception()
	elif len(clicommands)>0:
		print("Can't perform any operation because there is no leading master")

# decode URL, decide on sections/subsections, print header
if cgimode:
	if leaderfound:
		if masterconn.version_less_than(1,7,0):
			sectiondef={
				"IN":"Info",
				"CS":"Servers",
				"HD":"Disks",
				"EX":"Exports",
				"MS":"Mounts",
				"MO":"Operations",
				"MC":"Master Charts",
				"CC":"Server Charts"
			}
			sectionorder=["IN","CS","HD","EX","MS","MO","MC","CC"]
		elif masterconn.version_less_than(2,1,0):
			sectiondef={
				"IN":"Info",
				"CS":"Servers",
				"HD":"Disks",
				"EX":"Exports",
				"MS":"Mounts",
				"MO":"Operations",
				"QU":"Quotas",
				"MC":"Master Charts",
				"CC":"Server Charts"
			}
			sectionorder=["IN","CS","HD","EX","MS","MO","QU","MC","CC"]
		else:
			sectiondef={
				"ST":"Status",
				"IN":"Info",
				"CS":"Servers",
				"HD":"Disks",
				"EX":"Exports",
				"MS":"Mounts",
				"MO":"Operations",
				"RS":"Resources",
				"QU":"Quotas",
				"MC":"Master Charts",
				"CC":"Server Charts"
			}
			sectionorder=["ST","IN","CS","HD","EX","MS","MO","RS","QU","MC","CC"]
	elif electfound:
		sectiondef = {
			"ST":"Status",
			"IN":"Info",
			"CS":"Servers",
			"HD":"Disks",
			"EX":"Exports",
			"QU":"Quotas",
			"MC":"Master Charts",
			"CC":"Server Charts"
		}
		sectionorder=["ST","IN","CS","HD","EX","QU","MC","CC"]
	elif followerfound or usurperfound:
		sectiondef = {
			"IN":"Info",
			"MC":"Master Charts"
		}
		sectionorder=["IN","MC"]
	else:
		sectiondef = {
			"IN":"Info"
		}
		sectionorder=["IN"]
	
	#decode URL parameters
	if "readonly" in fields: #readonly=1 - don't show any command links (on/off maintenance, remove etc.)
		if fields.getvalue("readonly")=="1":
			readonly = True
	if "selectable" in fields: #selectable=0 - don't show any selectors requiring page refresh (knobs, drop-downs etc.)
		if fields.getvalue("selectable")=="0":
			selectable = False
	if "sections" in fields: #list of "|" separated sections to show
		sectionstr = fields.getvalue("sections")
		if type(sectionstr) is list:
			sectionstr = "|".join(sectionstr)
		sectionset = set(sectionstr.split("|"))
	else:
		sectionset = set((sectionorder[0],)) #show the 1st section if no section is defined in url
	if "subsections" in fields: #list of "|" separated subsections to show
		subsectionstr = fields.getvalue("subsections")
		if type(subsectionstr) is list:
			subsectionstr = "|".join(subsectionstr)
		sectionsubset = set(subsectionstr.split("|"))
	else:
		sectionsubset = ["IM","IG","MU","IC","IL","MF","CS","MB","SC","PA","OF","AL"] # used only in climode - in cgimode turn on all subsections

	print("Content-Type: text/html; charset=UTF-8")
	print("")

	if not ajax_request:
		# print """<!-- Put IE into quirks mode -->
		print("""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">""")
		print("""<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">""")
		print("""<head>""")
		print("""<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />""")
		print("""<title>MFS Info (%s)</title>""" % (htmlentities(mastername)))
		# leave this script a the begining to prevent screen blinking then using dark mode
		print("""<script type="text/javascript"><!--//--><![CDATA[//><!--
			if (localStorage.getItem('theme')===null || localStorage.getItem('theme')==='dark') { document.documentElement.setAttribute('data-theme', 'dark');}	
			//--><!]]></script>""")
		print("""<link rel="stylesheet" href="mfs.css" type="text/css" />""")
		print("""<script src="acidtab.js" type="text/javascript"></script>""")
		print("""<script src="acidchart.js" type="text/javascript"></script>""")
		print("""</head>""")
		print("""<body>""")
		print("""<svg xmlns="http://www.w3.org/2000/svg" display="none">
		<symbol id="icon-dark"><path d="M 9.148 0.416 C 9.101 0.2 8.92 0.044 8.701 0.025 C 8.504 0.009 8.307 0 8.107 0 C 4.251 0 1.126 3.134 1.126 7 C 1.126 10.866 4.251 14 8.11 14 C 10.004 14 11.72 13.244 12.979 12.019 C 13.135 11.866 13.176 11.628 13.076 11.434 C 12.976 11.241 12.76 11.131 12.545 11.169 C 12.239 11.222 11.926 11.25 11.604 11.25 C 8.576 11.25 6.12 8.787 6.12 5.75 C 6.12 3.694 7.245 1.903 8.91 0.959 C 9.101 0.85 9.198 0.631 9.151 0.419 L 9.148 0.416 Z"></path></symbol>
		<symbol id="icon-light"><path d="M 7 0 C 7.241 0 7.438 0.197 7.438 0.438 L 7.438 2.625 C 7.438 2.866 7.241 3.063 7 3.063 C 6.759 3.063 6.563 2.866 6.563 2.625 L 6.563 0.438 C 6.563 0.197 6.759 0 7 0 Z M 0 7 C 0 6.759 0.197 6.563 0.438 6.563 L 2.625 6.563 C 2.866 6.563 3.063 6.759 3.063 7 C 3.063 7.241 2.866 7.438 2.625 7.438 L 0.438 7.438 C 0.197 7.438 0 7.241 0 7 Z M 10.938 7 C 10.938 6.759 11.134 6.563 11.375 6.563 L 13.563 6.563 C 13.803 6.563 14 6.759 14 7 C 14 7.241 13.803 7.438 13.563 7.438 L 11.375 7.438 C 11.134 7.438 10.938 7.241 10.938 7 Z M 7 10.938 C 7.241 10.938 7.438 11.134 7.438 11.375 L 7.438 13.563 C 7.438 13.803 7.241 14 7 14 C 6.759 14 6.563 13.803 6.563 13.563 L 6.563 11.375 C 6.563 11.134 6.759 10.938 7 10.938 Z M 2.051 2.051 C 2.22 1.881 2.499 1.881 2.669 2.051 L 4.216 3.598 C 4.386 3.768 4.386 4.047 4.216 4.216 C 4.047 4.386 3.768 4.386 3.598 4.216 L 2.051 2.669 C 1.881 2.499 1.881 2.22 2.051 2.051 Z M 2.051 11.949 C 1.881 11.78 1.881 11.501 2.051 11.331 L 3.598 9.784 C 3.768 9.614 4.047 9.614 4.216 9.784 C 4.386 9.953 4.386 10.232 4.216 10.402 L 2.669 11.949 C 2.499 12.119 2.22 12.119 2.051 11.949 Z M 9.784 4.216 C 9.614 4.047 9.614 3.768 9.784 3.598 L 11.331 2.051 C 11.501 1.881 11.78 1.881 11.949 2.051 C 12.119 2.22 12.119 2.499 11.949 2.669 L 10.402 4.216 C 10.232 4.386 9.953 4.386 9.784 4.216 Z M 9.784 9.784 C 9.953 9.614 10.232 9.614 10.402 9.784 L 11.949 11.331 C 12.119 11.501 12.119 11.78 11.949 11.949 C 11.78 12.119 11.501 12.119 11.331 11.949 L 9.784 10.402 C 9.614 10.232 9.614 9.953 9.784 9.784 Z M 3.938 7 C 3.938 4.642 6.49 3.169 8.531 4.348 C 9.479 4.895 10.063 5.906 10.063 7 C 10.063 9.358 7.51 10.831 5.469 9.652 C 4.521 9.105 3.938 8.094 3.938 7 Z"></path></symbol>
		<symbol id="icon-thumbtack"><path d="M 5.501 8.249 L 2.126 8.249 C 1.894 8.249 1.676 8.145 1.534 7.961 C 1.392 7.778 1.341 7.541 1.397 7.318 L 1.544 6.734 C 1.785 5.765 2.373 4.957 3.154 4.427 L 3.438 0.749 L 2.875 0.749 C 2.807 0.749 2.744 0.732 2.69 0.701 C 2.577 0.636 2.501 0.514 2.501 0.374 C 2.501 0.168 2.669 0 2.875 0 L 8.875 0 C 9.081 0 9.25 0.168 9.25 0.374 C 9.25 0.514 9.175 0.636 9.06 0.701 C 9.006 0.732 8.944 0.749 8.875 0.749 L 8.313 0.749 L 8.597 4.427 C 9.377 4.957 9.965 5.765 10.206 6.734 L 10.352 7.318 C 10.409 7.541 10.359 7.778 10.216 7.961 C 10.075 8.145 9.857 8.249 9.626 8.249 L 6.25 8.249 L 6.25 11.626 C 6.25 11.831 6.081 12 5.854 12 C 5.669 12 5.479 11.831 5.479 11.626 L 5.501 8.249 Z M 3.876 4.847 L 3.576 5.049 C 2.943 5.477 2.467 6.131 2.272 6.914 L 2.126 7.5 L 5.501 7.5 L 5.501 5.249 C 5.501 5.043 5.669 4.874 5.875 4.874 C 6.081 4.874 6.25 5.043 6.25 5.249 L 6.25 7.5 L 9.626 7.5 L 9.48 6.914 C 9.283 6.131 8.807 5.477 8.175 5.049 L 7.877 4.847 L 7.56 0.749 L 4.19 0.749 L 3.876 4.847 Z"></path></symbol>
		<symbol id="icon-thumbtack-rot"><path d="M 3.573 7.857 L 1.187 5.47 C 1.023 5.306 0.942 5.079 0.972 4.848 C 1.001 4.618 1.132 4.415 1.33 4.297 L 1.846 3.988 C 2.702 3.473 3.689 3.317 4.616 3.495 L 7.418 1.095 L 7.02 0.697 C 6.972 0.649 6.939 0.592 6.923 0.532 C 6.889 0.406 6.921 0.266 7.02 0.167 C 7.166 0.021 7.404 0.021 7.549 0.167 L 11.792 4.41 C 11.938 4.555 11.938 4.794 11.793 4.939 C 11.694 5.038 11.554 5.072 11.427 5.036 C 11.367 5.02 11.311 4.988 11.262 4.939 L 10.865 4.542 L 8.465 7.344 C 8.642 8.27 8.486 9.257 7.971 10.113 L 7.662 10.629 C 7.544 10.827 7.341 10.959 7.111 10.987 C 6.881 11.018 6.653 10.937 6.49 10.774 L 4.103 8.387 L 1.715 10.774 C 1.57 10.919 1.331 10.919 1.17 10.759 C 1.04 10.628 1.025 10.374 1.17 10.229 L 3.573 7.857 Z M 4.83 4.302 L 4.475 4.233 C 3.725 4.088 2.926 4.214 2.234 4.63 L 1.716 4.941 L 4.103 7.327 L 5.695 5.736 C 5.84 5.59 6.079 5.589 6.224 5.735 C 6.37 5.881 6.37 6.12 6.224 6.265 L 4.632 7.857 L 7.02 10.244 L 7.331 9.726 C 7.745 9.034 7.871 8.235 7.727 7.485 L 7.659 7.131 L 10.332 4.01 L 7.95 1.627 L 4.83 4.302 Z"></path></symbol>
		<symbol id="icon-sort-up"><path d="M 1.101 6.601 L 9.899 6.601 C 10.874 6.601 11.368 5.415 10.677 4.723 L 6.307 0.322 C 5.877 -0.107 5.177 -0.107 4.748 0.322 L 0.319 4.724 C -0.368 5.415 0.126 6.601 1.101 6.601 Z M 5.5 1.099 L 9.87 5.501 L 1.072 5.501 L 5.5 1.099 Z"></path></symbol>
		<symbol id="icon-sort-down"><path d="M 9.896 0 L 1.1 0 C 0.125 0 -0.369 1.186 0.322 1.878 L 4.691 6.278 C 5.121 6.707 5.821 6.707 6.25 6.278 L 10.619 1.878 C 11.395 1.186 10.9 0 9.896 0 Z M 5.496 5.5 L 1.129 1.1 L 9.925 1.1 L 5.496 5.5 Z"></path></symbol>
		<symbol id="icon-progress-dot"><path d="M 5 0.832 C 5.118 0.832 5.214 0.942 5.214 1.075 L 5.214 2.288 C 5.214 2.423 5.118 2.532 5 2.532 C 4.881 2.532 4.785 2.423 4.785 2.288 L 4.785 1.075 C 4.785 0.942 4.881 0.832 5 0.832 Z"></path></symbol>
		<symbol id="icon-ok"><path d="M 5.697 11.982 C 10.309 11.982 13.191 6.99 10.885 2.996 C 9.815 1.142 7.837 0 5.697 0 C 1.085 0 -1.798 4.993 0.508 8.987 C 1.578 10.84 3.556 11.982 5.697 11.982 Z M 8.341 4.891 L 5.346 7.887 C 5.126 8.107 4.77 8.107 4.552 7.887 L 3.055 6.389 C 2.835 6.169 2.835 5.813 3.055 5.596 C 3.275 5.378 3.63 5.376 3.848 5.596 L 4.948 6.695 L 7.545 4.095 C 7.765 3.875 8.121 3.875 8.339 4.095 C 8.556 4.315 8.559 4.671 8.339 4.889 L 8.341 4.891 Z"></path></symbol>
		<symbol id="icon-ok-circle"><path d="M 5.714 7.964 C 5.459 8.22 5.041 8.22 4.786 7.964 L 3.286 6.464 C 3.03 6.209 3.03 5.791 3.286 5.536 C 3.541 5.28 3.959 5.28 4.214 5.536 L 5.25 6.572 L 7.786 4.036 C 8.041 3.78 8.459 3.78 8.714 4.036 C 8.97 4.291 8.97 4.709 8.714 4.964 L 5.714 7.964 Z M 12 6 C 12 9.314 9.314 12 6 12 C 2.686 12 0 9.314 0 6 C 0 2.686 2.686 0 6 0 C 9.314 0 12 2.686 12 6 Z M 6 1.125 C 3.307 1.125 1.125 3.307 1.125 6 C 1.125 8.693 3.307 10.875 6 10.875 C 8.693 10.875 10.875 8.693 10.875 6 C 10.875 3.307 8.693 1.125 6 1.125 Z"></path></symbol>
		<symbol id="icon-warning-2color-a"><polygon points="5.91 2.082 11.496 11.32 0.378 11.213" style="fill: rgb(222, 212, 68);"></polygon><path  d="M 11.867 10.525 L 6.871 2 C 6.679 1.672 6.342 1.508 6.005 1.508 C 5.667 1.508 5.33 1.672 5.117 2 L 0.124 10.525 C -0.239 11.178 0.24 12 1.01 12 L 11.001 12 C 11.768 12 12.248 11.18 11.867 10.525 Z M 1.241 10.876 L 5.983 2.745 L 10.768 10.876 L 1.241 10.876 Z M 6.005 8.653 C 5.598 8.653 5.268 8.983 5.268 9.39 C 5.268 9.796 5.599 10.126 6.006 10.126 C 6.412 10.126 6.741 9.796 6.741 9.39 C 6.74 8.984 6.412 8.653 6.005 8.653 Z M 5.443 5.068 L 5.443 7.316 C 5.443 7.628 5.695 7.878 6.005 7.878 C 6.314 7.878 6.567 7.626 6.567 7.316 L 6.567 5.068 C 6.567 4.759 6.316 4.506 6.005 4.506 C 5.693 4.506 5.443 4.759 5.443 5.068 Z"></path></symbol>
		<symbol id="icon-warning-2color-b"><rect style="fill: black;" x="4.955" y="3.748" width="2.154" height="7.214"/><path d="M 5.999 1.499 C 6.332 1.499 6.639 1.675 6.808 1.963 L 11.871 10.589 C 12.042 10.879 12.042 11.238 11.876 11.529 C 11.709 11.819 11.398 12 11.062 12 L 0.937 12 C 0.601 12 0.29 11.819 0.123 11.529 C -0.043 11.238 -0.041 10.877 0.128 10.589 L 5.191 1.963 C 5.36 1.675 5.667 1.499 5.999 1.499 Z M 5.999 4.499 C 5.688 4.499 5.437 4.75 5.437 5.062 L 5.437 7.687 C 5.437 7.999 5.688 8.249 5.999 8.249 C 6.311 8.249 6.562 7.999 6.562 7.687 L 6.562 5.062 C 6.562 4.75 6.311 4.499 5.999 4.499 Z M 6.75 9.75 C 6.75 9.172 6.124 8.811 5.624 9.1 C 5.392 9.234 5.249 9.482 5.249 9.75 C 5.249 10.327 5.874 10.688 6.375 10.399 C 6.607 10.265 6.75 10.018 6.75 9.75 Z"/></symbol>
		<symbol id="icon-warning"><path d="M 5.999 0 C 6.332 0 6.639 0.176 6.808 0.464 L 11.871 9.09 C 12.042 9.38 12.042 9.739 11.876 10.03 C 11.709 10.32 11.398 10.501 11.062 10.501 L 0.937 10.501 C 0.601 10.501 0.29 10.32 0.123 10.03 C -0.043 9.739 -0.041 9.378 0.128 9.09 L 5.191 0.464 C 5.36 0.176 5.667 0 5.999 0 Z M 5.999 2.795 C 5.466 2.79 5.1 3.251 5.1 3.563 L 5.1 6.188 C 5.1 6.5 5.5 6.869 5.999 6.872 C 6.498 6.875 6.9 6.5 6.9 6.188 L 6.9 3.563 C 6.9 3.251 6.532 2.8 5.999 2.795 Z M 6.9 8.401 C 6.9 7.864 6.5 7.501 6 7.501 C 5.504 7.501 5.1 7.896 5.1 8.401 C 5.1 8.935 5.525 9.301 6 9.301 C 6.469 9.301 6.9 8.845 6.9 8.401 Z"></path></symbol>
		<symbol id="icon-error"><path d="M 3.408 0.473 C 3.711 0.171 4.122 0 4.551 0 L 7.45 0 C 7.878 0 8.289 0.171 8.594 0.473 L 11.276 3.158 C 11.579 3.461 11.75 3.872 11.75 4.301 L 11.75 7.2 C 11.75 7.628 11.579 8.04 11.276 8.344 L 8.594 11.026 C 8.289 11.329 7.878 11.5 7.45 11.5 L 4.551 11.5 C 4.122 11.5 3.711 11.329 3.408 11.026 L 0.723 8.344 C 0.421 8.04 0.25 7.628 0.25 7.2 L 0.25 4.301 C 0.25 3.872 0.421 3.461 0.723 3.158 L 3.408 0.473 Z M 5.137 3.354 L 5.137 5.942 C 5.137 6.517 5.712 6.708 6 6.708 C 6.287 6.708 6.897 6.496 6.862 5.942 L 6.862 3.354 C 6.862 2.779 6.287 2.587 6 2.587 C 5.712 2.587 5.153 2.762 5.137 3.354 Z M 6 7.168 C 5.438 7.15 5.042 7.475 5.046 8.02 C 5.042 8.625 5.42 9.005 6 9.008 C 6.58 9.011 6.958 8.625 6.916 8.046 C 6.911 7.498 6.562 7.187 6 7.168 Z"></path></symbol>
		<symbol id="icon-go-up"><path d="M 0.562 10.825 C 0.251 10.825 0 11.076 0 11.388 C 0 11.699 0.251 11.95 0.562 11.95 L 3 11.95 C 4.139 11.95 5.062 11.027 5.062 9.888 L 5.062 1.871 L 7.101 3.909 C 7.321 4.13 7.677 4.13 7.895 3.909 C 8.113 3.689 8.116 3.333 7.895 3.115 L 4.898 0.115 C 4.678 -0.105 4.321 -0.105 4.104 0.115 L 1.101 3.115 C 0.881 3.335 0.881 3.691 1.101 3.909 C 1.322 4.127 1.678 4.13 1.896 3.909 L 3.935 1.871 L 3.937 9.888 C 3.937 10.406 3.518 10.825 3 10.825 L 0.562 10.825 Z"></path></symbol>
		<symbol id="icon-close"><path d="M 10.78 2.282 C 11.073 1.989 11.073 1.514 10.78 1.223 C 10.486 0.933 10.011 0.93 9.721 1.223 L 6.003 4.941 L 2.282 1.22 C 1.989 0.927 1.514 0.927 1.223 1.22 C 0.933 1.514 0.93 1.989 1.223 2.279 L 4.941 5.997 L 1.22 9.718 C 0.927 10.011 0.927 10.486 1.22 10.777 C 1.514 11.067 1.989 11.07 2.279 10.777 L 5.997 7.059 L 9.718 10.78 C 10.011 11.073 10.486 11.073 10.777 10.78 C 11.067 10.486 11.07 10.011 10.777 9.721 L 7.059 6.003 L 10.78 2.282 Z"></path></symbol>
		<symbol id="icon-wrench"><path d="M 7.554 6.281 C 7.774 6.342 8.006 6.375 8.25 6.375 C 9.668 6.375 10.823 5.25 10.873 3.844 L 10.28 4.437 C 9.998 4.718 9.616 4.875 9.218 4.875 L 8.625 4.875 C 7.798 4.875 7.125 4.202 7.125 3.375 L 7.125 2.78 C 7.125 2.381 7.282 1.999 7.563 1.718 L 8.156 1.127 C 6.75 1.174 5.625 2.332 5.625 3.75 C 5.625 3.991 5.658 4.226 5.719 4.446 C 5.827 4.835 5.714 5.252 5.428 5.538 L 1.338 9.626 C 1.2 9.762 1.125 9.949 1.125 10.144 C 1.125 10.547 1.453 10.875 1.856 10.875 C 2.051 10.875 2.236 10.798 2.374 10.662 L 6.462 6.572 C 6.748 6.286 7.165 6.176 7.554 6.281 Z M 9.952 0.923 L 8.36 2.515 C 8.29 2.585 8.25 2.681 8.25 2.78 L 8.25 3.375 C 8.25 3.581 8.419 3.75 8.625 3.75 L 9.22 3.75 C 9.319 3.75 9.415 3.71 9.485 3.64 L 11.077 2.048 C 11.245 1.88 11.529 1.91 11.632 2.126 C 11.869 2.618 12 3.169 12 3.75 C 12 5.822 10.322 7.5 8.25 7.5 C 7.905 7.5 7.573 7.453 7.259 7.366 L 3.169 11.456 C 2.82 11.805 2.348 12 1.856 12 C 0.832 12 0 11.168 0 10.144 C 0 9.652 0.195 9.18 0.544 8.831 L 4.634 4.741 C 4.547 4.427 4.5 4.095 4.5 3.75 C 4.5 1.678 6.178 0 8.25 0 C 8.831 0 9.382 0.134 9.874 0.368 C 10.09 0.471 10.12 0.755 9.952 0.923 Z M 2.063 9.563 C 2.351 9.563 2.532 9.875 2.387 10.125 C 2.32 10.241 2.196 10.313 2.063 10.313 C 1.774 10.313 1.593 10 1.738 9.75 C 1.805 9.634 1.929 9.563 2.063 9.563 Z"></path></symbol>
		<symbol id="icon-stopwatch"><path d="M 3.001 0.563 C 3.001 0.251 3.251 0 3.563 0 L 6.189 0 C 6.5 0 6.751 0.251 6.751 0.563 C 6.751 0.874 6.5 1.125 6.189 1.125 L 5.439 1.125 L 5.439 2.283 C 6.456 2.4 7.38 2.829 8.106 3.476 L 8.791 2.79 C 9.011 2.569 9.367 2.569 9.585 2.79 C 9.803 3.01 9.806 3.366 9.585 3.584 L 8.859 4.311 C 9.421 5.106 9.752 6.076 9.752 7.124 C 9.752 9.818 7.569 12 4.876 12 C 2.182 12 0 9.82 0 7.126 C 0 4.623 1.885 2.562 4.313 2.283 L 4.313 1.125 L 3.563 1.125 C 3.251 1.125 3.001 0.874 3.001 0.563 Z M 4.876 10.877 C 7.763 10.877 9.568 7.752 8.124 5.251 C 7.454 4.091 6.216 3.376 4.876 3.376 C 1.989 3.376 0.184 6.501 1.628 9.002 C 2.298 10.162 3.536 10.877 4.876 10.877 Z M 5.439 5.063 L 5.439 7.501 C 5.439 7.813 5.188 8.064 4.876 8.064 C 4.564 8.064 4.313 7.813 4.313 7.501 L 4.313 5.063 C 4.313 4.752 4.564 4.501 4.876 4.501 C 5.188 4.501 5.439 4.752 5.439 5.063 Z"></path></symbol>
		</svg>""")
		#<symbol id=""><path d=""></path></symbol>
		print("""<span class="TOOLTIP" id="charttooltip"></span>""")
		print("""<span class="HLINE" id="charthlineup"></span>""")
		print("""<span class="HLINE" id="charthlinedown"></span>""")

		#MENUBAR
		print("""<div id="header">""")
		print("""<table class="HDR" cellpadding="0" cellspacing="0" border="0">""")
		print("""<tr>""")
		logo_src="logo.svg"
		print("""<td class="LOGO"><a href="https://moosefs.com/"><img src="%s" alt="logo" style="border:0;height:40px;width:126px;vertical-align:baseline;" /></a></td>""" % logo_src)
		print("""<td class="MENU"><table class="MENU" cellspacing="0">""")
		print("""<tr>""")
		last="U"
		for k in sectionorder:
			if k==sectionorder[-1]:
				last = "L%s" % last
			if k in sectionset:
				if len(sectionset)<=1:
					print("""<td class="%sS">%s<svg class="thumbtack" style="vertical-align:bottom;"><use xlink:href="#icon-thumbtack"/></svg></td>""" % (last,sectiondef[k])) #
				else:
					print("""<td class="%sS"><a href="%s">%s</a><a href="%s"><svg class="thumbtack" style="vertical-align:bottom;"><use xlink:href="#icon-thumbtack"/></svg></a></td>""" % (last,createhtmllink({"sections":k}),sectiondef[k],createhtmllink({"sections":"|".join(sectionset-set([k]))})))
				last="S"
			else:
				print("""<td class="%sU"><a href="%s">%s</a><a href="%s"><svg class="thumbtack" style="vertical-align:up;"><use xlink:href="#icon-thumbtack-rot"/></svg></a></td>""" % (last,createhtmllink({"sections":k}),sectiondef[k],createhtmllink({"sections":"|".join(sectionset|set([k]))})))
				last="U"
		print("""</tr>""")
		print("""</table></td>""")
		print("""<td class="FILLER" style="white-space:nowrap;">""")
		print("""</td>""")
		print("""</tr>""")
		print("""</table>""")
		print("""</div>""")

		print("""<div id="container">""")
		print("""<div id="container-ajax">""")

# deal with missing leader in the cluster
if leaderfound==0:
	if cgimode:
		out = []
		out.append("""<div class="tab_title ERROR">Oops!</div>""")
		out.append("""<table class="FR MESSAGE" cellspacing="0">""")
		if len(masterlist)==0:
			out.append("""	<tr>""")
			out.append("""		<td align="center">""")
			out.append("""			<span class="ERROR">Can't find masters (resolve given name)!</span><br />""")
			out.append("""			<form method="GET">""")
			out.append("""				Input your DNS master name: <input type="text" name="masterhost" value="%s" size="100">""" % htmlentities(masterhost))
			for i in createinputs(["masterhost"]):
				out.append("""				%s""" % (i))
			out.append("""				<input type="submit" value="Try it!">""")
			out.append("""			</form>""")
			out.append("""		</td>""")
			out.append("""	</tr>""")
		else:
			out.append("""	<tr>""")
			out.append("""		<td align="center">""")
			out.append("""			<span class="ERROR">Can't find working masters!</span><br />""")
			out.append("""			<form method="GET">""")
			out.append("""				Input your DNS master name: <input type="text" name="masterhost" value="%s" size="100"><br />""" % htmlentities(masterhost))
			out.append("""				Input your master-client port number: <input type="text" name="masterport" value="%u" size="5"><br />""" % (masterport))
			out.append("""				Input your master-control port number: <input type="text" name="mastercontrolport" value="%u" size="5"><br />""" % (mastercontrolport))
			for i in createinputs(["masterhost","masterport","mastercontrolport"]):
				out.append("""				%s""" % (i))
			out.append("""				<input type="submit" value="Try it!">""")
			out.append("""			</form>""")
			out.append("""		</td>""")
			out.append("""	</tr>""")
		out.append("""</table>""")
		print("\n".join(out))
	elif jsonmode:
		if len(masterlist)==0:
			jcollect["errors"].append("Can't find masters (resolve '%s')!" % (masterhost))
		else:
			jcollect["errors"].append("Working master servers not found! Maybe you are using wrong port number or wrong dns name")
	else:
		if len(masterlist)==0:
			print("Can't find masters (resolve '%s')!" % (masterhost))
		else:
			print("Working master servers not found! Maybe you are using wrong port number or wrong dns name")
	if not cgimode:
		Table.Needseparator=1

# decide on non-cgi sections
if not cgimode:
	if leaderfound:
		if masterconn.version_less_than(1,7,0):
			allowedsections = ["IN","CS","HD","EX","MS","MO","MC","CC"]
		elif masterconn.version_less_than(2,1,0):
			allowedsections = ["IN","CS","HD","EX","MS","MO","QU","MC","CC"]
		else:
			allowedsections = ["IN","CS","HD","EX","MS","MO","RS","QU","MC","CC"]
	elif electfound:
		allowedsections = ["IN","CS","HD","EX","QU","MC","CC"]
	elif followerfound or usurperfound:
		allowedsections = ["IN","MC"]
	elif len(masterlist)>0:
		allowedsections = ["IN"]
	else:
		sys.exit(1)

	filtered_sectionset = []
	for section in sectionset:
		if section in allowedsections:
			filtered_sectionset.append(section)
		else:
			if jsonmode:
				jcollect["errors"].append("section '%s' not allowed" % section)
			else:
				print("section '%s' not allowed" % section)
	sectionset = filtered_sectionset

# parse cgi parameters and data ordering
if cgimode:
	try:
		ICsclassid = int(fields.getvalue("ICsclassid"))
	except Exception:
		ICsclassid = -1
	try:
		ICmatrix = int(fields.getvalue("ICmatrix"))
	except Exception:
		ICmatrix = 0
	try:
		IMorder = int(fields.getvalue("IMorder"))
	except Exception:
		IMorder = 0
	try:
		IMrev = int(fields.getvalue("IMrev"))
	except Exception:
		IMrev = 0
	try:
		MForder = int(fields.getvalue("MForder"))
	except Exception:
		MForder = 0
	try:
		MFrev = int(fields.getvalue("MFrev"))
	except Exception:
		MFrev = 0
	try:
		MFlimit = int(fields.getvalue("MFlimit"))
	except Exception:
		MFlimit = 100
	try:
		CSorder = int(fields.getvalue("CSorder"))
	except Exception:
		CSorder = 0
	try:
		CSrev = int(fields.getvalue("CSrev"))
	except Exception:
		CSrev = 0
	try:
		CScsid = str(fields.getvalue("CScsid"))
	except Exception:
		CScsid = ""
	try:
		MBorder = int(fields.getvalue("MBorder"))
	except Exception:
		MBorder = 0
	try:
		MBrev = int(fields.getvalue("MBrev"))
	except Exception:
		MBrev = 0
	try:
		if "HDdata" in fields:
			HDdata = str(fields.getvalue("HDdata"))
		else:
			HDdata = ""
	except Exception:
		HDdata = ""
	try:
		HDorder = int(fields.getvalue("HDorder"))
	except Exception:
		HDorder = 0
	try:
		HDrev = int(fields.getvalue("HDrev"))
	except Exception:
		HDrev = 0
	try:
		HDperiod = int(fields.getvalue("HDperiod"))
	except Exception:
		HDperiod = 0
	try:
		HDtime = int(fields.getvalue("HDtime"))
	except Exception:
		HDtime = 0
	try:
		HDaddrname = int(fields.getvalue("HDaddrname"))
	except Exception:
		HDaddrname = 0
	try:
		EXorder = int(fields.getvalue("EXorder"))
	except Exception:
		EXorder = 0
	try:
		EXrev = int(fields.getvalue("EXrev"))
	except Exception:
		EXrev = 0
	try:
		MSorder = int(fields.getvalue("MSorder"))
	except Exception:
		MSorder = 0
	try:
		MSrev = int(fields.getvalue("MSrev"))
	except Exception:
		MSrev = 0
	try:
		MOorder = int(fields.getvalue("MOorder"))
	except Exception:
		MOorder = 0
	try:
		MOrev = int(fields.getvalue("MOrev"))
	except Exception:
		MOrev = 0
	try:
		MOdata = int(fields.getvalue("MOdata"))
	except Exception:
		MOdata = 0
	try:
		SCorder = int(fields.getvalue("SCorder"))
	except Exception:
		SCorder = 0
	try:
		SCrev = int(fields.getvalue("SCrev"))
	except Exception:
		SCrev = 0
	try:
		SCdata = int(fields.getvalue("SCdata"))
	except Exception:
		SCdata = 0
	try:
		PAorder = int(fields.getvalue("PAorder"))
	except Exception:
		PAorder = 0
	try:
		PArev = int(fields.getvalue("PArev"))
	except Exception:
		PArev = 0
	try:
		OForder = int(fields.getvalue("OForder"))
	except Exception:
		OForder = 0
	try:
		OFrev = int(fields.getvalue("OFrev"))
	except Exception:
		OFrev = 0
	try:
		OFsessionid = int(fields.getvalue("OFsessionid"))
	except Exception:
		OFsessionid = 0
	try:
		ALorder = int(fields.getvalue("ALorder"))
	except Exception:
		ALorder = 0
	try:
		ALrev = int(fields.getvalue("ALrev"))
	except Exception:
		ALrev = 0
	try:
		ALinode = int(fields.getvalue("ALinode"))
	except Exception:
		ALinode = 0
	try:
		QUorder = int(fields.getvalue("QUorder"))
	except Exception:
		QUorder = 0
	try:
		QUrev = int(fields.getvalue("QUrev"))
	except Exception:
		QUrev = 0
	try:
		if "MCdata" in fields:
			MCdata = fields.getvalue("MCdata")
			if type(MCdata) is list:
				MCdata = MCdata[0]
		else:
			MCdata = ""
	except Exception:
		MCdata = ""
	try:
		if "CCdata" in fields:
			CCdata = fields.getvalue("CCdata")
			if type(CCdata) is list:
				CCdata = CCdata[0]
		else:
			CCdata = ""
	except Exception:
		CCdata = ""
else:
	# fix order id's
	if CSorder>=2:
		CSorder+=1
	if leaderfound and masterconn.version_less_than(1,7,25) and CSorder>=4:
		CSorder+=1
	if leaderfound and masterconn.version_less_than(1,6,28) and CSorder>=5:
		CSorder+=1
	if CSorder>=6 and CSorder<=9:
		CSorder+=4
	elif CSorder>=10 and CSorder<=13:
		CSorder+=10

	if MBorder>=2:
		MBorder+=1

	if HDorder>=13 and HDorder<=15:
		HDorder+=7

	if leaderfound and masterconn.version_less_than(1,7,0) and EXorder>=10:
		EXorder+=1

	if MSorder>=3:
		MSorder+=1
	if leaderfound and masterconn.version_less_than(1,7,0) and MSorder>=10:
		MSorder+=1
	if MSorder==0:
		MSorder=2

	if MOorder==2:
		MOorder = 3
	elif MOorder>=3 and MOorder<=18:
		MOorder += 97
	elif MOorder==19:
		MOorder = 150
	elif MOorder!=1:
		MOorder = 0

	if QUorder>=2 and QUorder<=6:
		QUorder += 8
	elif QUorder>=7 and QUorder<=10:
		QUorder += 14
	elif QUorder>=11 and QUorder<=22:
		QUorder = (lambda x: x[0]+x[1]*10)(divmod(QUorder-11,3))+31
	elif QUorder<1 or QUorder>22:
		QUorder = 0

# Status section
if "ST" in sectionset and cgimode and (leaderfound or electfound):
	try:
		def seconds_since_beginning_of_hour():
			now = datetime.now()  # Get the current date and time
			start_of_hour = now.replace(minute=0, second=0, microsecond=0)  # Set minutes, seconds, and microseconds to 0
			seconds_since = (now - start_of_hour).total_seconds()  # Calculate the difference in seconds
			return int(seconds_since)  # Return the result as an integer
		
		# Get licence info from the leader
		def get_licence():
			return None

		# Decode masterinfo TODO: move to separate function
		length = len(masterinfo)
		if length==101 or length==121 or length==129 or length==137 or length==149 or length==173 or length==181 or length==193 or length==205:
			offset = 8 if (length>=137 and length!=173) else 0
			if offset==8:
				v1,v2,v3,memusage,syscpu,usercpu,totalspace,availspace,freespace,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks = struct.unpack(">HBBQQQQQQQLQLLLLL",masterinfo[:84+offset])
			else:
				v1,v2,v3,memusage,syscpu,usercpu,totalspace,availspace,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks = struct.unpack(">HBBQQQQQQLQLLLLL",masterinfo[:84])
				freespace = None
		(leader_strver,_) = version_str_and_sort((v1,v2,v3))
		servers_warnings = []
		trsh_wrn = 0.8
		trsh_err = 0.95

		# Prepare master servers info
		mslist = []
		i = 0
		for sortip,strip,sortver,strver,statestr,statecolor,metaversion,memusage,syscpu,usercpu,lastsuccessfulstore,lastsaveseconds,lastsavestatus,exportschecksum,metaid,lastsavemetaversion,lastsavemetachecksum,usectime,chlogtime in masterlistinfo:
			(_,port,_) = masterlistver[i]
			server = {
				"name": masterhost,
				"ip": strip,
				"port": port,
				"strver": strver,
				"statestr": statestr,
				"cpuload": (syscpu+usercpu)/100.0,
				"memory": memusage,
				"live": 0 if statestr=="UNREACHABLE" else 1
			}
			if server['live']!=1 or leader_strver!=strver or (not statestr in ['LEADER', 'FOLLOWER', 'MASTER', '-']) or server['cpuload'] > trsh_wrn:
				servers_warnings.append(strip)
			mslist.append((sortip, port, server))
			i += 1
		mslist.sort(key=lambda entry: (entry[0], entry[1]))

		masterservers = []
		for ms in mslist:
			masterservers.append(ms[2])

		metaloggers = []
		mlslist=dataprovider.get_metaloggers(1)
		for sf,host,sortip,strip,sortver,strver in mlslist:
			server = {
				"name": host,
				"ip": strip,
				"strver": strver,
			}
			if leader_strver!=strver:
				servers_warnings.append(strip)
			metaloggers.append(server)
		
		cslist = []
		(hdds_all, scanhdds)=dataprovider.get_hdds("ALL")
		for cs in dataprovider.get_chunkservers(1):
			(hdds_status, hdds)=dataprovider.cs_hdds_status(cs, hdds_all+scanhdds)
			(load_state, load_state_msg, _, _) = gracetime2txt(cs.gracetime)
			srv = {
				"id": "%s:%s" % (cs.strip,cs.port),
				"name": cs.host,
				"strver": cs.strver,
				"ip": cs.strip,
				"port": cs.port,
				"live": 1 if (cs.flags&1)==0 else 0,     # if (cs.flags&1)!=0: <- dead server
				"maintenance": 1 if (cs.flags&2)!=0 else 0, # 1 - server in maintenance mode
				"load": cs.load,
				"load_state": load_state,
				"load_state_msg": load_state_msg,
				"flags": cs.flags,
				"hdd_reg_total": cs.total,
				"hdd_reg_used": cs.used,
				"hdd_rem_total": cs.tdtotal,
				"hdd_rem_used": cs.tdused,
				"hdds_status": hdds_status,
				"hdds": None #unused: hdds
				}
			cslist.append((cs.sortip, cs.port, srv))

			if srv['live']!=1 or srv['maintenance']!=0 or leader_strver!=cs.strver or srv['load_state'] == 'OVLD' or (srv['hdd_reg_total']!=0 and srv['hdd_reg_used']/srv['hdd_reg_total'] > trsh_wrn) or hdds_status!='ok':
				servers_warnings.append(cs.strip)

		chunkservers = []
		for cs in cslist:
			chunkservers.append(cs[2])
		
		# Prepare general cluster info 
		# Calculate IOPS, #TODO: get better IOPS statistics
		sessions=dataprovider.get_sessions()
		rops_c_total = 0
		rops_l_total = 0
		wops_c_total = 0
		wops_l_total = 0
		ops_c_total = 0
		ops_l_total = 0
		for ses in sessions: #summing both last and current hour operations
			ops_c_total += sum(ses.stats_c)
			ops_l_total += sum(ses.stats_l)
			#TODO: add read/write ops stats
			if (dataprovider.stats_to_show>16):
				rops_c_total=ses.stats_c[16] # 17: read
				wops_c_total=ses.stats_c[17] # 18: write
				rops_l_total=ses.stats_l[16] # 17: read
				wops_l_total=ses.stats_l[17] # 18: write
		if seconds_since_beginning_of_hour()>30.0:
			ops = float(ops_c_total)/float(seconds_since_beginning_of_hour())
			rops = float(rops_c_total)/float(seconds_since_beginning_of_hour())
			wops = float(wops_c_total)/float(seconds_since_beginning_of_hour())
		else:
			ops = float(ops_l_total)/3600.0
			rops = float(rops_l_total)/3600.0
			wops = float(wops_l_total)/3600.0
		
		bread_ext = None
		bwrite_ext = None
		# bread_ext_int = None
		# bwrite_ext_int = None
		if (dataprovider.stats_to_show<=16):
			wops = None
			ops = None

		licence=get_licence()
		licmaxsize = licence[6] if licence != None else None
		currentsize = licence[7] if licence != None else None
		# Calculate sums of missing, undergoal and endangered chunks
		(matrix,progressstatus)=dataprovider.get_matrix(-1)
		chunks_total = 0
		chunks_missing = 0
		chunks_undergoal = 0 
		chunks_endangered = 0
		for goal in range(11):
			for vc in range(11):
				chunks_total+=matrix[0][goal][vc]
				if goal>0:
					if vc==0:
						chunks_missing+=matrix[0][goal][vc]
					elif(vc<goal):
						if vc==1:
							chunks_endangered+=matrix[0][goal][vc]
						else:
							chunks_undergoal+=matrix[0][goal][vc]
		clusterinfo = {
			"leaderfound" : 1 if leaderfound else 0,
			"strver": leader_strver,
			"totalspace": totalspace,
			"availspace": availspace,
			"freespace": freespace,
			"dirs": dirs,
			"files": files,
			"trfiles": trfiles,
			"trspace": trspace,
			"chunks_progress": progressstatus,
			"chunks_total": chunks_total,
			"chunks_missing": chunks_missing,
			"chunks_undergoal": chunks_undergoal,
			"chunks_endangered": chunks_endangered,
			"servers_warnings": servers_warnings,
			"mounts": len(sessions),
			"ops": ops,	  #all (metadata and read and write) operations per second
			"rops": rops, #read operations per second
			"wops": wops, #write operations per second
			# "bread_ext_int": bread_ext_int,   #traffic read by all CS - both from clients (ext) and other servers (int)
			# "bwrite_ext_int": bwrite_ext_int, #traffic write to all CS - both from clients (ext) and other servers (int)
			# "bread_ext": bread_ext,           #traffic read by all CS - only from clients (ext)
			# "bwrite_ext": bwrite_ext,         #traffic write to all CS - only from clients (ext)
			"licmaxsize": licmaxsize, 
			"currentsize": currentsize
		}

		try:
			import mfsgraph as gr
			print("""<div id="mfsgraph-tile">""")
			print(gr.html_cluster_state(clusterinfo))
			print("""<div id="mfsgraph">"""+gr.svg_mfs_graph(masterservers, metaloggers, chunkservers, leader_strver)+"""</div><!-- mfsgraph -->""")
			print("""<div id="mfsgraph-info"></div>""")
			print("""</div>""")
		except Exception:
			print_error("Graph unsupported")
			print_exception()
	except Exception:
		print_exception()

# Info section
if "IN" in sectionset:
	if not cgimode and jsonmode:
		json_in_dict = {}

	if "IG" in sectionsubset and masterconn!=None:
		try:
			length = len(masterinfo)
			if length==68:
				v1,v2,v3,total,avail,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks,allcopies,regularcopies = struct.unpack(">HBBQQQLQLLLLLLL",masterinfo)
				strver,sortver = version_str_and_sort((v1,v2,v3))
				if cgimode:
					out = []
					out.append("""<div class="tab_title">Cluster summary</div>""")
					out.append("""<table class="FR" cellspacing="0">""")
					out.append("""	<tr>""")
					out.append("""		<th>version</th>""")
					out.append("""		<th>total space</th>""")
					out.append("""		<th>avail space</th>""")
					out.append("""		<th>trash space</th>""")
					out.append("""		<th>trash files</th>""")
					out.append("""		<th>sustained space</th>""")
					out.append("""		<th>sustained files</th>""")
					out.append("""		<th>all fs objects</th>""")
					out.append("""		<th>directories</th>""")
					out.append("""		<th>files</th>""")
					out.append("""		<th>chunks</th>""")
					out.append("""		<th><a style="cursor:default" title="chunks from 'regular' hdd space and 'marked for removal' hdd space">all chunk copies</a></th>""")
					out.append("""		<th><a style="cursor:default" title="only chunks from 'regular' hdd space">regular chunk copies</a></th>""")
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					out.append("""		<td align="center">%s</td>""" % strver)
					out.append("""		<td align="right"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(total),humanize_number(total,"&nbsp;")))
					out.append("""		<td align="right"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(avail),humanize_number(avail,"&nbsp;")))
					out.append("""		<td align="right"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(trspace),humanize_number(trspace,"&nbsp;")))
					out.append("""		<td align="right">%s</td>""" % decimal_number_html(trfiles))
					out.append("""		<td align="right"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(respace),humanize_number(respace,"&nbsp;")))
					out.append("""		<td align="right">%s</td>""" % decimal_number_html(refiles))
					out.append("""		<td align="right">%s</td>""" % decimal_number_html(nodes))
					out.append("""		<td align="right">%s</td>""" % decimal_number_html(dirs))
					out.append("""		<td align="right">%s</td>""" % decimal_number_html(files))
					out.append("""		<td align="right">%s</td>""" % decimal_number_html(chunks))
					out.append("""		<td align="right">%s</td>""" % decimal_number_html(allcopies))
					out.append("""		<td align="right">%s</td>""" % decimal_number_html(regularcopies))
					out.append("""	</tr>""")
					out.append("""</table>""")
					print("\n".join(out))
				elif jsonmode:
					json_ig_dict = {}
					json_ig_dict["strver"] = strver
					if strver.endswith(" PRO"):
						json_ig_dict["version"] = strver[:-4]
						json_ig_dict["pro"] = True
					else:
						json_ig_dict["version"] = strver
						json_ig_dict["pro"] = False
					json_ig_dict["memory_usage"] = None
					json_ig_dict["memory_usage_human"] = ""
					json_ig_dict["cpu_usage_percent"] = None
					json_ig_dict["cpu_system_percent"] = None
					json_ig_dict["cpu_user_percent"] = None
					json_ig_dict["total_space"] = total
					json_ig_dict["total_space_human"] = humanize_number(total," ")
					json_ig_dict["avail_space"] = avail
					json_ig_dict["avail_space_human"] = humanize_number(avail," ")
					json_ig_dict["trash_space"] = trspace
					json_ig_dict["trash_space_human"] = humanize_number(trspace," ")
					json_ig_dict["trash_files"] = trfiles
					json_ig_dict["sustained_space"] = respace
					json_ig_dict["sustained_space_human"] = humanize_number(respace," ")
					json_ig_dict["sustained_files"] = refiles
					json_ig_dict["filesystem_objects"] = nodes
					json_ig_dict["directories"] = dirs
					json_ig_dict["files"] = files
					json_ig_dict["chunks"] = chunks
					json_ig_dict["all_copies"] = allcopies
					json_ig_dict["regular_copies"] = regularcopies
					json_ig_dict["chunks_in_ec_percent"] = None
					json_ig_dict["redundancy_ratio"] = None
					json_ig_dict["ec_bytes_saved"] = None
					json_ig_dict["last_metadata_save_time"] = None
					json_ig_dict["last_metadata_save_time_str"] = ""
					json_ig_dict["last_metadata_save_duration"] = None
					json_ig_dict["last_metadata_save_duration_human"] = ""
					json_ig_dict["last_metadata_save_status"] = None
					json_ig_dict["last_metadata_save_status_txt"] = ""

					json_in_dict["general"] = json_ig_dict
				else:
					if ttymode:
						tab = Table("Cluster summary",2)
					else:
						tab = Table("cluster summary",2)
					tab.defattr("l","r")
					tab.append("master version",strver)
					if ttymode:
						tab.append("total space",humanize_number(total," "))
						tab.append("avail space",humanize_number(avail," "))
						tab.append("trash space",humanize_number(trspace," "))
					else:
						tab.append("total space",total)
						tab.append("avail space",avail)
						tab.append("trash space",trspace)
					tab.append("trash files",trfiles)
					if ttymode:
						tab.append("sustained space",humanize_number(respace," "))
					else:
						tab.append("sustained space",respace)
					tab.append("sustained files",refiles)
					tab.append("all fs objects",nodes)
					tab.append("directories",dirs)
					tab.append("files",files)
					tab.append("chunks",chunks)
					tab.append("all chunk copies",allcopies)
					tab.append("regular chunk copies",regularcopies)
					print(myunicode(tab))
			elif length==76:
				v1,v2,v3,memusage,total,avail,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks,allcopies,regularcopies = struct.unpack(">HBBQQQQLQLLLLLLL",masterinfo)
				strver,sortver = version_str_and_sort((v1,v2,v3))
				if cgimode:
					out = []
					out.append("""<div class="tab_title">Cluster summary</div>""")
					out.append("""<table class="FR" cellspacing="0">""")
					out.append("""	<tr>""")
					out.append("""		<th>version</th>""")
					out.append("""		<th>RAM used</th>""")
					out.append("""		<th>total space</th>""")
					out.append("""		<th>avail space</th>""")
					out.append("""		<th>trash space</th>""")
					out.append("""		<th>trash files</th>""")
					out.append("""		<th>sustained space</th>""")
					out.append("""		<th>sustained files</th>""")
					out.append("""		<th>all fs objects</th>""")
					out.append("""		<th>directories</th>""")
					out.append("""		<th>files</th>""")
					out.append("""		<th>chunks</th>""")
					out.append("""		<th><a style="cursor:default" title="chunks from 'regular' hdd space and 'marked for removal' hdd space">all chunk copies</a></th>""")
					out.append("""		<th><a style="cursor:default" title="only chunks from 'regular' hdd space">regular chunk copies</a></th>""")
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					out.append("""		<td align="center">%s</td>""" % strver)
					if memusage>0:
						out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(memusage),humanize_number(memusage,"&nbsp;")))
					else:
						out.append("""		<td align="center"><a style="cursor:default" title="obtaining memory usage is not supported by your OS">not available</td>""")
					out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(total),humanize_number(total,"&nbsp;")))
					out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(avail),humanize_number(avail,"&nbsp;")))
					out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(trspace),humanize_number(trspace,"&nbsp;")))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(trfiles))
					out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(respace),humanize_number(respace,"&nbsp;")))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(refiles))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(nodes))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(dirs))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(files))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(chunks))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(allcopies))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(regularcopies))
					out.append("""	</tr>""")
					out.append("""</table>""")
					print("\n".join(out))
				elif jsonmode:
					json_ig_dict = {}
					json_ig_dict["strver"] = strver
					if strver.endswith(" PRO"):
						json_ig_dict["version"] = strver[:-4]
						json_ig_dict["pro"] = True
					else:
						json_ig_dict["version"] = strver
						json_ig_dict["pro"] = False
					if memusage>0:
						json_ig_dict["memory_usage"] = memusage
						json_ig_dict["memory_usage_human"] = humanize_number(memusage," ")
					else:
						json_ig_dict["memory_usage"] = None
						json_ig_dict["memory_usage_human"] = ""
					json_ig_dict["cpu_usage_percent"] = None
					json_ig_dict["cpu_system_percent"] = None
					json_ig_dict["cpu_user_percent"] = None
					json_ig_dict["total_space"] = total
					json_ig_dict["total_space_human"] = humanize_number(total," ")
					json_ig_dict["avail_space"] = avail
					json_ig_dict["avail_space_human"] = humanize_number(avail," ")
					json_ig_dict["trash_space"] = trspace
					json_ig_dict["trash_space_human"] = humanize_number(trspace," ")
					json_ig_dict["trash_files"] = trfiles
					json_ig_dict["sustained_space"] = respace
					json_ig_dict["sustained_space_human"] = humanize_number(respace," ")
					json_ig_dict["sustained_files"] = refiles
					json_ig_dict["filesystem_objects"] = nodes
					json_ig_dict["directories"] = dirs
					json_ig_dict["files"] = files
					json_ig_dict["chunks"] = chunks
					json_ig_dict["all_copies"] = allcopies
					json_ig_dict["regular_copies"] = regularcopies
					json_ig_dict["chunks_in_ec_percent"] = None
					json_ig_dict["redundancy_ratio"] = None
					json_ig_dict["ec_bytes_saved"] = None
					json_ig_dict["last_metadata_save_time"] = None
					json_ig_dict["last_metadata_save_time_str"] = ""
					json_ig_dict["last_metadata_save_duration"] = None
					json_ig_dict["last_metadata_save_duration_human"] = ""
					json_ig_dict["last_metadata_save_status"] = None
					json_ig_dict["last_metadata_save_status_txt"] = ""

					json_in_dict["general"] = json_ig_dict
				else:
					if ttymode:
						tab = Table("Cluster summary",2)
					else:
						tab = Table("cluster summary",2)
					tab.defattr("l","r")
					tab.append("master version",strver)
					if memusage>0:
						if ttymode:
							tab.append("RAM used",humanize_number(memusage," "))
						else:
							tab.append("RAM used",memusage)
					else:
						tab.append("RAM used","not available")
					if ttymode:
						tab.append("total space",humanize_number(total," "))
						tab.append("avail space",humanize_number(avail," "))
						tab.append("trash space",humanize_number(trspace," "))
					else:
						tab.append("total space",total)
						tab.append("avail space",avail)
						tab.append("trash space",trspace)
					tab.append("trash files",trfiles)
					if ttymode:
						tab.append("sustained space",humanize_number(respace," "))
					else:
						tab.append("sustained space",respace)
					tab.append("sustained files",refiles)
					tab.append("all fs objects",nodes)
					tab.append("directories",dirs)
					tab.append("files",files)
					tab.append("chunks",chunks)
					tab.append("all chunk copies",allcopies)
					tab.append("regular chunk copies",regularcopies)
					print(myunicode(tab))
			elif length==101 or length==121 or length==129 or length==137 or length==149 or length==173 or length==181 or length==193 or length==205:
				offset = 8 if (length>=137 and length!=173) else 0
				if offset==8:
					v1,v2,v3,memusage,syscpu,usercpu,totalspace,availspace,freespace,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks = struct.unpack(">HBBQQQQQQQLQLLLLL",masterinfo[:84+offset])
				else:
					v1,v2,v3,memusage,syscpu,usercpu,totalspace,availspace,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks = struct.unpack(">HBBQQQQQQLQLLLLL",masterinfo[:84])
					freespace = None
				if length==205:
					lastsuccessfulstore,lastsaveseconds,lastsavestatus = struct.unpack(">LLB",masterinfo[offset+96:offset+105])
				else:
					lastsuccessfulstore,lastsaveseconds,lastsavestatus = struct.unpack(">LLB",masterinfo[offset+92:offset+101])
				if length>=173:
					if length==205:
						copychunks,ec8chunks,ec4chunks = struct.unpack(">LLL",masterinfo[offset+84:offset+96])
					else:
						copychunks,ec8chunks = struct.unpack(">LL",masterinfo[offset+84:offset+92])
						ec4chunks = 0
					if length==205:
						chunkcopies,chunkec8parts,chunkec4parts,chunkhypcopies = struct.unpack(">QQQQ",masterinfo[offset+153:offset+185])
					else:
						chunkcopies,chunkec8parts,chunkhypcopies = struct.unpack(">QQQ",masterinfo[offset+149:offset+173])
						chunkec4parts = 0
					if (copychunks + ec8chunks + ec4chunks) > 0:
						ecchunkspercent = 100.0 * (ec8chunks + ec4chunks) / (copychunks + ec8chunks + ec4chunks)
						dataredundancyratio = (chunkcopies + 0.125 * chunkec8parts + 0.25 * chunkec4parts) / (copychunks + ec8chunks + ec4chunks)
					else:
						ecchunkspercent = None
						dataredundancyratio = None
					if (chunkcopies + 0.125 * chunkec8parts + 0.25 * chunkec4parts) > 0.0:
						noecratio = (chunkhypcopies / (chunkcopies + 0.125 * chunkec8parts + 0.25 * chunkec4parts))
						if noecratio>1.0:
							extranoecspace = noecratio - 1.0
						else:
							extranoecspace = 0.0
						if freespace!=None:
							savedbyec = int((totalspace-freespace) * extranoecspace)
						else:
							savedbyec = int((totalspace-availspace) * extranoecspace)
					else:
						savedbyec = None
					metainfomode = 1
				else:
					allcopies,regularcopies = struct.unpack(">LL",masterinfo[offset+84:offset+92])
					metainfomode = 0
				
				strver,sortver = version_str_and_sort((v1,v2,v3))
				syscpu/=10000000.0
				usercpu/=10000000.0
				if masterconn.version_at_least(2,0,14):
					lastsaveseconds = lastsaveseconds / 1000.0
				if cgimode:
					out = []
					if length==101:
						out.append("""<div class="tab_title">Cluster summary</div>""")
						out.append("""<table class="FR" cellspacing="0">""")
						out.append("""	<tr>""")
						out.append("""		<th>version</th>""")
						out.append("""		<th>RAM used</th>""")
						out.append("""		<th>CPU used</th>""")
						out.append("""		<th>last successful metadata save</th>""")
						out.append("""		<th>last metadata save duration</th>""")
						out.append("""		<th>last metadata save status</th>""")
						out.append("""	</tr>""")
						out.append("""	<tr>""")
						out.append("""		<td align="center">%s</td>""" % strver)
						if memusage>0:
							out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(memusage),humanize_number(memusage,"&nbsp;")))
						else:
							out.append("""		<td align="center"><a style="cursor:default" title="obtaining memory usage is not supported by your OS">not available</td>""")
						if syscpu>0 or usercpu>0:
							out.append("""		<td align="center"><a style="cursor:default" title="all:%.7f%% sys:%.7f%% user:%.7f">all:%.2f%%&nbsp;sys:%.2f%%&nbsp;user:%.2f%%</a></td>""" % (syscpu+usercpu,syscpu,usercpu,syscpu+usercpu,syscpu,usercpu))
						else:
							out.append("""		<td align="center"><a style="cursor:default" title="obtaining cpu usage is not supported by your OS">not available</td>""")
						if lastsuccessfulstore>0:
							out.append("""		<td align="center">%s</td>""" % time.asctime(time.localtime(lastsuccessfulstore)))
							out.append("""		<td align="center"><a style="cursor:default" title="%s">%s</a></td>""" % (timeduration_to_fullstr(lastsaveseconds),timeduration_to_shortstr(lastsaveseconds)))
						else:
							out.append("""		<td align="center">-</td><td align="center">-</td>""")
						if lastsuccessfulstore>0 or lastsavestatus>0:
							out.append("""		<td align="center"><span class="%s">%s</span></td>""" % ("SUCCESS" if lastsavestatus==0 else "ERROR","OK" if lastsavestatus==0 else "ERROR (%u)" % lastsavestatus))
						else:
							out.append("""		<td align="center">-</td>""")
						out.append("""	</tr>""")
						out.append("""</table>""")

					out.append("""<div class="tab_title">Cluster summary</div>""")
					out.append("""<table class="FR no-hover" cellspacing="0">""")

					out.append("""	<tr>""")
					out.append("""		<th colspan="3">Cluster space</th>""")
					out.append("""		<th colspan="2">Trash</th>""")
					out.append("""		<th colspan="2">Sustained</th>""")
					out.append("""		<th colspan="3">File system objects</th>""")
					out.append("""		<th rowspan="2">Chunks<br/>total</th>""")
					if metainfomode:
						out.append("""		<th rowspan="2"><a style="cursor:default" title="chunk is in EC format when redundancy level in EC format is higher than number of full copies">EC chunks</a></th>""")
						out.append("""		<th rowspan="2"><a style="cursor:default" title="storage needed to maintain defined redundancy level divided by raw data size">Disks overhead<br/>due to redundancy</a></th>""")
						out.append("""		<th rowspan="2"><a style="cursor:default" title="assumes that full and EC chunks have the same average length and that we save extra copies using redundancy level from EC definition">Space saved<br/>by EC</a></th>""")
					else:
						out.append("""		<th colspan="2">Chunk copies</th>""")
					out.append("""	</tr>""")

					out.append("""	<tr>""")
					out.append("""		<th style="width:70px;">total</th>""")
					out.append("""		<th style="width:70px;">available</th>""")

					out.append("""		<th style="width:140px;">% used</th>""")
					out.append("""		<th style="min-width:50px;">space</th>""")
					out.append("""		<th style="min-width:50px;">files</th>""")
					out.append("""		<th style="min-width:50px;">space</th>""")
					out.append("""		<th style="min-width:50px;">files</th>""")
					out.append("""		<th style="min-width:50px;">all</th>""")
					out.append("""		<th style="min-width:50px;">directories</th>""")
					out.append("""		<th style="min-width:50px;">files</th>""")
					if not metainfomode:
						out.append("""		<th><a style="cursor:default" title="chunks from 'regular' hdd space and 'marked for removal' hdd space">all</a></th>""")
						out.append("""		<th><a style="cursor:default" title="only chunks from 'regular' hdd space">regular</a></th>""")
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(totalspace),humanize_number(totalspace,"&nbsp;")))
					out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(availspace),humanize_number(availspace,"&nbsp;")))
					usedpercent=100*(totalspace-availspace)/totalspace if totalspace!=0 else 0
					out.append("""		<td align="center"><div class="PROGBOX"><div class="PROGCOVER" style="width:%.2f%%;"></div><div class="PROGVALUE"><span>%.1f</span></div></div></td>""" % (100.0-usedpercent, usedpercent))
					out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(trspace),humanize_number(trspace,"&nbsp;")))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(trfiles)) 
					out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(respace),humanize_number(respace,"&nbsp;")))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(refiles))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(nodes))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(dirs))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(files))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(chunks))
					if metainfomode:
						if ecchunkspercent!=None:
							out.append("""		<td align="center"><a style="cursor:default" title="full chunks: %u ; EC8 chunks: %u ; EC4 chunks: %u">%.1f&#8239;%%</a></td>""" % (copychunks,ec8chunks,ec4chunks,ecchunkspercent))
						else:
							out.append("""		<td align="center"><a style="cursor:default" title="full chunks: %u ; EC8 chunks: %u ; EC4 chunks: %u">-</a></td>""" % (copychunks,ec8chunks,ec4chunks))
						if dataredundancyratio!=None:
							out.append("""		<td align="center"><a style="cursor:default" title="full copies: %u ; EC8 parts: %u ; EC4 parts: %u ; chunks: %u">+ %.1f&#8239;%%</a></td>""" % (chunkcopies,chunkec8parts,chunkec4parts,(copychunks+ec8chunks+ec4chunks),(dataredundancyratio-1)*100))
						else:
							out.append("""		<td align="center"><a style="cursor:default" title="full copies: %u ; EC8 parts: %u ; EC4 parts: %u ; chunks: %u">-</a></td>""" % (chunkcopies,chunkec8parts,chunkec4parts,(copychunks+ec8chunks+ec4chunks)))
						if savedbyec!=None:
							out.append("""		<td align="center">%s</td>""" % (("about "+humanize_number(savedbyec,"&nbsp;")) if savedbyec>0 else "not yet"))
						else:
							out.append("""		<td align="center">-</td>""")
					else:
						out.append("""		<td align="center">%u</td>""" % allcopies)
						out.append("""		<td align="center">%u</td>""" % regularcopies)
					out.append("""	</tr>""")
					out.append("""</table>""")
					print("\n".join(out))
				elif jsonmode:
					json_ig_dict = {}
					json_ig_dict["strver"] = strver
					if strver.endswith(" PRO"):
						json_ig_dict["version"] = strver[:-4]
						json_ig_dict["pro"] = True
					else:
						json_ig_dict["version"] = strver
						json_ig_dict["pro"] = False
					if memusage>0:
						json_ig_dict["memory_usage"] = memusage
						json_ig_dict["memory_usage_human"] = humanize_number(memusage," ")
					else:
						json_ig_dict["memory_usage"] = None
						json_ig_dict["memory_usage_human"] = ""
					if syscpu>0 or usercpu>0:
						json_ig_dict["cpu_usage_percent"] = syscpu+usercpu
						json_ig_dict["cpu_system_percent"] = syscpu
						json_ig_dict["cpu_user_percent"] = usercpu
					else:
						json_ig_dict["cpu_usage_percent"] = None
						json_ig_dict["cpu_system_percent"] = None
						json_ig_dict["cpu_user_percent"] = None
					json_ig_dict["total_space"] = totalspace
					json_ig_dict["total_space_human"] = humanize_number(totalspace," ")
					json_ig_dict["avail_space"] = availspace
					json_ig_dict["avail_space_human"] = humanize_number(availspace," ")
					json_ig_dict["free_space"] = freespace
					json_ig_dict["free_space_human"] = humanize_number(freespace," ")
					json_ig_dict["trash_space"] = trspace
					json_ig_dict["trash_space_human"] = humanize_number(trspace," ")
					json_ig_dict["trash_files"] = trfiles
					json_ig_dict["sustained_space"] = respace
					json_ig_dict["sustained_space_human"] = humanize_number(respace," ")
					json_ig_dict["sustained_files"] = refiles
					json_ig_dict["filesystem_objects"] = nodes
					json_ig_dict["directories"] = dirs
					json_ig_dict["files"] = files
					json_ig_dict["chunks"] = chunks
					if metainfomode:
						json_ig_dict["copy_chunks"] = copychunks
						json_ig_dict["ec8_chunks"] = ec8chunks
						json_ig_dict["ec4_chunks"] = ec4chunks
						json_ig_dict["full_chunk_copies"] = chunkcopies
						json_ig_dict["ec8_chunk_parts"] = chunkec8parts
						json_ig_dict["ec4_chunk_parts"] = chunkec4parts
						json_ig_dict["hypothetical_chunk_copies"] = chunkhypcopies
						json_ig_dict["ec_bytes_saved"] = savedbyec
						if savedbyec!=None:
							json_ig_dict["ec_bytes_saved_human"] = humanize_number(savedbyec," ")
						else:
							json_ig_dict["ec_bytes_saved_human"] = ""
						json_ig_dict["redundancy_ratio"] = dataredundancyratio
						json_ig_dict["chunks_in_ec_percent"] = ecchunkspercent
					else:
						json_ig_dict["all_copies"] = allcopies
						json_ig_dict["regular_copies"] = regularcopies
					if lastsuccessfulstore>0:
						json_ig_dict["last_metadata_save_time"] = lastsuccessfulstore
						json_ig_dict["last_metadata_save_time_str"] = time.asctime(time.localtime(lastsuccessfulstore))
						json_ig_dict["last_metadata_save_duration"] = lastsaveseconds
						json_ig_dict["last_metadata_save_duration_human"] = timeduration_to_shortstr(lastsaveseconds)
					else:
						json_ig_dict["last_metadata_save_time"] = None
						json_ig_dict["last_metadata_save_time_str"] = ""
						json_ig_dict["last_metadata_save_duration"] = None
						json_ig_dict["last_metadata_save_duration_human"] = ""
					if lastsuccessfulstore>0 or lastsavestatus>0:
						json_ig_dict["last_metadata_save_status"] = lastsavestatus
						json_ig_dict["last_metadata_save_status_txt"] = "Saved in background" if lastsavestatus==0 else "Downloaded from another master" if lastsavestatus==1 else "Saved in foreground" if lastsavestatus==2 else "CRC saved in background" if lastsavestatus==3 else ("Unknown status: %u" % lastsavestatus)
					else:
						json_ig_dict["last_metadata_save_status"] = None
						json_ig_dict["last_metadata_save_status_txt"] = ""

					json_in_dict["general"] = json_ig_dict
				else:
					if ttymode:
						tab = Table("Cluster summary",2)
					else:
						tab = Table("cluster summary",2)
					tab.defattr("l","r")
					tab.append("master version",strver)
					if memusage>0:
						if ttymode:
							tab.append("RAM used",humanize_number(memusage," "))
						else:
							tab.append("RAM used",memusage)
					else:
						tab.append("RAM used","not available")
					if syscpu>0 or usercpu>0:
						if ttymode:
							tab.append("CPU used","%.2f%%" % (syscpu+usercpu))
							tab.append("CPU used (system)","%.2f%%" % (syscpu))
							tab.append("CPU used (user)","%.2f%%" % (usercpu))
						else:
							tab.append("CPU used (system)","%.9f" % (syscpu/100.0))
							tab.append("CPU used (user)","%.9f" % (usercpu/100.0))
					else:
						tab.append("CPU used","not available")
					if ttymode:
						tab.append("total space",humanize_number(totalspace," "))
						tab.append("avail space",humanize_number(availspace," "))
						if freespace!=None:
							tab.append("free space",humanize_number(freespace," "))
						tab.append("trash space",humanize_number(trspace," "))
					else:
						tab.append("total space",totalspace)
						tab.append("avail space",availspace)
						if freespace!=None:
							tab.append("free space",freespace)
						tab.append("trash space",trspace)
					tab.append("trash files",trfiles)
					if ttymode:
						tab.append("sustained space",humanize_number(respace," "))
					else:
						tab.append("sustained space",respace)
					tab.append("sustained files",refiles)
					tab.append("all fs objects",nodes)
					tab.append("directories",dirs)
					tab.append("files",files)
					tab.append("chunks",chunks)
					if metainfomode:
						if ttymode:
							if ecchunkspercent!=None:
								tab.append("percent of chunks in EC format","%.1f%%" % ecchunkspercent)
							if dataredundancyratio!=None:
								tab.append("real data redundancy ratio","%.3f" % dataredundancyratio)
							if savedbyec!=None:
								tab.append("storage bytes saved by EC",humanize_number(savedbyec," "))
						else:
							if copychunks!=None:
								tab.append("chunks in copy format",copychunks)
							if ec8chunks!=None:
								tab.append("chunks in EC8 format",ec8chunks)
							if ec4chunks!=None:
								tab.append("chunks in EC4 format",ec4chunks)
							if chunkcopies!=None:
								tab.append("all full chunk copies",chunkcopies)
							if chunkec8parts!=None:
								tab.append("all chunk EC8 parts",chunkec8parts)
							if chunkec4parts!=None:
								tab.append("all chunk EC4 parts",chunkec4parts)
							if chunkhypcopies!=None:
								tab.append("hypothetical full chunk copies",chunkhypcopies)
							if savedbyec!=None:
								tab.append("storage bytes saved by EC",savedbyec)
					else:
						if allcopies!=None:
							tab.append("all chunk copies",allcopies)
						if regularcopies!=None:
							tab.append("regular chunk copies",regularcopies)
					if lastsuccessfulstore>0:
						if ttymode:
							tab.append("last successful store",time.asctime(time.localtime(lastsuccessfulstore)))
							tab.append("last save duration",timeduration_to_shortstr(lastsaveseconds))
						else:
							tab.append("last successful store",lastsuccessfulstore)
							tab.append("last save duration","%.3f" % lastsaveseconds)
					else:
						tab.append("last successful store","-")
						tab.append("last save duration","-")
					if lastsuccessfulstore>0 or lastsavestatus>0:
						tab.append("last save status",("Saved in background","4") if lastsavestatus==0 else ("Downloaded from another master","4") if lastsavestatus==1 else ("Saved in foreground","2") if lastsavestatus==2 else ("CRC saved in background","5") if lastsavestatus==3 else ("Unknown status: %u" % lastsavestatus,"1"))
					else:
						tab.append("last save status","-")
					print(myunicode(tab))
			else:
				if cgimode:
					out = []
					out.append("""<div class="tab_title ERROR">Oops!</div>""")
					out.append("""<table class="FR MESSAGE" cellspacing="0">""")
					out.append("""	<tr><td align="left">Unrecognized answer from MFSmaster</td></tr>""")
					out.append("""</table>""")
					print("\n".join(out))
				else:
					print("unrecognized answer from MFSmaster")
		except Exception:
			print_exception()

	if "IM" in sectionsubset and len(masterlistinfo)>0:
		try:
			if cgimode:
				out = []
				out.append("""<div class="tab_title">Metadata servers (masters)</div>""")
				out.append("""<table class="acid_tab  acid_tab_storageid_mfsmasters" cellspacing="0">""") #acid_tab_zebra_C1_C2
				out.append("""	<tr>""")
				out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
				out.append("""		<th rowspan="2">IP</th>""")
				out.append("""		<th rowspan="2">Version</th>""")
				out.append("""		<th rowspan="2">State</th>""")
				out.append("""		<th rowspan="2">Local time</th>""")
				out.append("""		<th colspan="3">Current metadata</th>""")
				out.append("""		<th colspan="2">Resources</th>""")
				out.append("""		<th colspan="5">Last metadata save</th>""")
				out.append("""		<th rowspan="2">Exports checksum</th>""")
				out.append("""	</tr>""")
				out.append("""	<tr>""")
				out.append("""		<th>id</th>""")
				out.append("""		<th>version</th>""")
				out.append("""		<th>delay</th>""")
				out.append("""		<th>RAM</th>""")
				out.append("""		<th>CPU (sys&#8239;+&#8239;usr)</th>""")
				out.append("""		<th>time</th>""")
				out.append("""		<th>duration</th>""")
				out.append("""		<th>status</th>""")
				out.append("""		<th>version</th>""")
				out.append("""		<th>checksum</th>""")
				out.append("""	</tr>""")
			elif jsonmode:
				json_im_array = []
			elif ttymode:
				tab = Table("Metadata Servers (masters)",15,"r")
				tab.header("ip","version","state","local time","metadata version","metadata id","metadata delay","RAM used","CPU used","last meta save","last save duration","last save status","last save version","last save checksum","exports checksum")
			else:
				tab = Table("metadata servers (masters)",15)
			masterstab = []
			highest_saved_metaversion = 0
			highest_metaversion_checksum = 0
			for sortip,strip,sortver,strver,statestr,statecolor,metaversion,memusage,syscpu,usercpu,lastsuccessfulstore,lastsaveseconds,lastsavestatus,exportschecksum,metaid,lastsavemetaversion,lastsavemetachecksum,usectime,chlogtime in masterlistinfo:
				if lastsavemetaversion!=None and lastsavemetachecksum!=None:
					if lastsavemetaversion>highest_saved_metaversion:
						highest_saved_metaversion = lastsavemetaversion
						highest_metaversion_checksum = lastsavemetachecksum
					elif lastsavemetaversion==highest_saved_metaversion:
						highest_metaversion_checksum |= lastsavemetachecksum
				if IMorder==1:
					sf = sortip
				elif IMorder==2:
					sf = sortver
				elif IMorder==3:
					sf = statecolor
				elif IMorder==4:
					sf = usectime if usectime!=None else 0
				elif IMorder==5:
					sf = metaversion
				elif IMorder==6:
					sf = metaid
				elif IMorder==7:
					sf = chlogtime if chlogtime!=None else 0
				elif IMorder==8:
					sf = memusage
				elif IMorder==9:
					sf = syscpu+usercpu
				elif IMorder==10:
					sf = lastsuccessfulstore
				elif IMorder==11:
					sf = lastsaveseconds
				elif IMorder==12:
					sf = lastsavestatus
				elif IMorder==13:
					sf = lastsavemetaversion
				elif IMorder==14:
					sf = lastsavemetachecksum
				elif IMorder==15:
					sf = exportschecksum
				else:
					sf = 0
				masterstab.append((sf,sortip,strip,sortver,strver,statestr,statecolor,metaversion,memusage,syscpu,usercpu,lastsuccessfulstore,lastsaveseconds,lastsavestatus,exportschecksum,metaid,lastsavemetaversion,lastsavemetachecksum,usectime,chlogtime))

			masterstab.sort()
			if IMrev:
				masterstab.reverse()

			for sf,sortip,strip,sortver,strver,statestr,statecolor,metaversion,memusage,syscpu,usercpu,lastsuccessfulstore,lastsaveseconds,lastsavestatus,exportschecksum,metaid,lastsavemetaversion,lastsavemetachecksum,usectime,chlogtime in masterstab:
				if usectime==None or usectime==0:
					secdelta = None
				else:
					secdelta = 0.0
					if leader_usectime==None or leader_usectime==0:
						if master_maxusectime!=None and master_minusectime!=None:
							secdelta = (master_maxusectime - master_minusectime) / 1000000.0
					else:
						if leader_usectime > usectime:
							secdelta = (leader_usectime - usectime) / 1000000.0
						else:
							secdelta = (usectime - leader_usectime) / 1000000.0
				if chlogtime==None or chlogtime==0 or leader_usectime==None or leader_usectime==0:
					metadelay = None
				else:
					metadelay = leader_usectime/1000000.0 - chlogtime
					if metadelay>1.0:
						metadelay-=1.0
					else:
						metadelay=0.0
				if cgimode:
					if masterconn!=None and masterconn.is_pro() and not strver.endswith(" PRO"):
						verclass = "BADVERSION"
					elif masterconn!=None and masterconn.sort_ver() > sortver:
						verclass = "LOWERVERSION"
					elif masterconn!=None and masterconn.sort_ver() < sortver:
						verclass = "HIGHERVERSION"
					else:
						verclass = "OKVERSION"
					out.append("""	<tr>""")
					out.append("""		<td align="right"></td><td align="center"><span class="sortkey">%s </span>%s</td><td align="center"><span class="sortkey">%s </span><span class="%s">%s</span></td>""" % (sortip,strip,sortver,verclass,strver.replace(" PRO", "<small> PRO</small>")))
					out.append("""		<td align="center"><span class="STATECOLOR%u">%s</span></td>""" % (statecolor,statestr))
					if secdelta==None:
						out.append("""		<td align="center">-</td>""")
					else:
						out.append("""		<td align="center"><span class="%s">%s</span></td>""" % (("ERROR" if secdelta>2.0 else "WARNING" if secdelta>1.0 else "DEFAULT" if secdelta>0.0 else "DEFINED"),time_to_str(usectime//1000000))) # SUCCESS
					if metaid!=None:
						out.append("""		<td align="center"><span class="%s">%016X</span></td>""" % (("ERROR" if metaid != master_metaid else "DEFAULT"),metaid)) # SUCCESS
					else:
						out.append("""		<td align="center">-</td>""")
					metaversion_str=decimal_number_html(metaversion) if metaversion>0 else '-'
					out.append("""		<td align="right">%s</td>""" % (metaversion_str))
					if metadelay==None:
						out.append("""		<td align="right">-</td>""")
					else:
						out.append("""		<td align="right"><span class="%s">%.0f&#8239;s</span></td>""" % (("SUCCESS" if metadelay<1.0 else "WARNING" if metadelay<6.0 else "ERROR"),metadelay))
					not_available = 'n/a' if statestr!='UNREACHABLE' else '-'
					if memusage>0:
						out.append("""		<td align="right"><a style="cursor:default" title="%s&#8239;B">%s</a></td>""" % (decimal_number(memusage),humanize_number(memusage,"&nbsp;")))
					else:
						out.append("""		<td align="center"><a style="cursor:default" title="obtaining memory usage is not supported by your OS or can't be obtained from server">%s</td>""" % not_available)
					if syscpu>0 or usercpu>0:
						out.append("""		<td align="center"><a style="cursor:default" title="%.7f%% (sys:%.7f%%, usr:%.7f%%)">%.1f%%&nbsp;(%.1f&#8239;+&#8239;%.1f)</a></td>""" % (syscpu+usercpu,syscpu,usercpu,syscpu+usercpu,syscpu,usercpu))
					else:
						out.append("""		<td align="center"><a style="cursor:default" title="obtaining cpu usage is not supported by your OS or can't be obtained from server">%s</td>""" % not_available)
					if lastsuccessfulstore>0:
						out.append("""		<td align="center">%s</td>""" % time_to_str(lastsuccessfulstore))
						out.append("""		<td align="center"><a style="cursor:default" title="%s">%s</a></td>""" % (timeduration_to_fullstr(lastsaveseconds),timeduration_to_shortstr(lastsaveseconds,"&#8239;")))
					else:
						out.append("""		<td align="center">-</td><td align="center">-</td>""")
					if lastsuccessfulstore>0 or lastsavestatus>0:
						cls="DEFAULT" if lastsavestatus==0 or lastsavestatus==1 else "PARTIAL" if lastsavestatus==3 else "WARNING" if lastsavestatus==2 else "ERROR"
						txt='<a style="cursor:default" title="This server dumped a consistent metadata file in the background">Background save</a>' if lastsavestatus==0 else "Got from other" if lastsavestatus==1 else '<a style="cursor:default" title="Warning: This server dumped a consistent metadata file in the foreground (instead of background). Check your vm.overcommit_memory parameter in the /etc/sysctl.conf file. For more details refer to documentation.">Foreground save</a>' if lastsavestatus==2 else "CRC bckgrnd save" if lastsavestatus==3 else "Unknown: %u" % lastsavestatus
						out.append("""		<td align="center"><span class="%s">%s</span></td>""" % (cls,txt))
					else:
						out.append("""		<td align="center">-</td>""")
					if lastsuccessfulstore>0 and (lastsavestatus==0 or lastsavestatus>=2) and lastsavemetaversion!=None:
						out.append("""		<td align="right"><span class="%s">%s</span></td>""" % ("DEFAULT" if lastsavemetaversion==highest_saved_metaversion else "UNDEFINED", decimal_number_html(lastsavemetaversion)))
					else:
						out.append("""		<td align="center">-</td>""")
					if lastsuccessfulstore>0 and (lastsavestatus==0 or lastsavestatus>=2) and lastsavemetaversion!=None and lastsavemetachecksum!=None:
						out.append("""		<td align="center"><span class="%s">%08X</span></td>""" % ("DEFAULT" if lastsavemetaversion==highest_saved_metaversion and lastsavemetachecksum==highest_metaversion_checksum else "ERROR" if lastsavemetaversion==highest_saved_metaversion and lastsavemetachecksum!=highest_metaversion_checksum else "UNDEFINED",lastsavemetachecksum))
					else:
						out.append("""		<td align="center">-</td>""")
					if exportschecksum!=None:
						out.append("""		<td align="center"><span class="%s">%016X</span></td>""" % (("ERROR" if exportschecksum != master_exportschecksum else "DEFAULT"),exportschecksum))
					else:
						out.append("""		<td align="center">-</td>""")
					out.append("""	</tr>""")
				elif jsonmode:
					json_im_dict = {}
					json_im_dict["ip"] = strip
					json_im_dict["strver"] = strver
					if strver.endswith(" PRO"):
						json_im_dict["version"] = strver[:-4]
						json_im_dict["pro"] = True
					else:
						json_im_dict["version"] = strver
						json_im_dict["pro"] = False
					json_im_dict["state"] = statestr
					if secdelta==None:
						json_im_dict["localtime"] = None
						json_im_dict["localtime_str"] = ""
					else:
						json_im_dict["localtime"] = usectime/1000000.0
						json_im_dict["localtime_str"] = time.asctime(time.localtime(usectime//1000000))
					json_im_dict["metadata_version"] = metaversion
					json_im_dict["metadata_id"] = metaid
					json_im_dict["metadata_delay"] = metadelay
					if memusage>0:
						json_im_dict["memory_usage"] = memusage
						json_im_dict["memory_usage_human"] = humanize_number(memusage," ")
					else:
						json_im_dict["memory_usage"] = None
						json_im_dict["memory_usage_human"] = ""
					if syscpu>0 or usercpu>0:
						json_im_dict["cpu_usage_percent"] = syscpu+usercpu
						json_im_dict["cpu_system_percent"] = syscpu
						json_im_dict["cpu_user_percent"] = usercpu
					else:
						json_im_dict["cpu_usage_percent"] = None
						json_im_dict["cpu_system_percent"] = None
						json_im_dict["cpu_user_percent"] = None
					if lastsuccessfulstore>0:
						json_im_dict["last_metadata_save_time"] = lastsuccessfulstore
						json_im_dict["last_metadata_save_time_str"] = time.asctime(time.localtime(lastsuccessfulstore))
						json_im_dict["last_metadata_save_duration"] = lastsaveseconds
						json_im_dict["last_metadata_save_duration_human"] = timeduration_to_shortstr(lastsaveseconds)
					else:
						json_im_dict["last_metadata_save_time"] = None
						json_im_dict["last_metadata_save_time_str"] = ""
						json_im_dict["last_metadata_save_duration"] = None
						json_im_dict["last_metadata_save_duration_human"] = ""
					if lastsuccessfulstore>0 or lastsavestatus>0:
						json_im_dict["last_metadata_save_status"] = lastsavestatus
						json_im_dict["last_metadata_save_status_txt"] = "Saved in background" if lastsavestatus==0 else "Downloaded from other master" if lastsavestatus==1 else "Saved in foreground" if lastsavestatus==2 else "CRC saved in background" if lastsavestatus==3 else ("Unknown status: %u" % lastsavestatus)
					else:
						json_im_dict["last_metadata_save_status"] = None
						json_im_dict["last_metadata_save_status_txt"] = ""
					if lastsuccessfulstore>0 and (lastsavestatus==0 or lastsavestatus>=2) and lastsavemetaversion!=None:
						json_im_dict["last_metadata_save_version"] = lastsavemetaversion
					else:
						json_im_dict["last_metadata_save_version"] = None
					if lastsuccessfulstore>0 and (lastsavestatus==0 or lastsavestatus>=2) and lastsavemetaversion!=None and lastsavemetachecksum!=None:
						json_im_dict["last_metadata_save_checksum"] = "%08X" % lastsavemetachecksum
					else:
						json_im_dict["last_metadata_save_checksum"] = None
					if exportschecksum!=None:
						json_im_dict["exports_checksum"] = "%016X" % exportschecksum
					else:
						json_im_dict["exports_checksum"] = None
					json_im_array.append(json_im_dict)
				else:
					clist = [strip,strver,(statestr,"c%u" % statecolor)]
					if secdelta==None:
						clist.append("not available")
					else:
						if ttymode:
							clist.append((time.asctime(time.localtime(usectime//1000000)),("1" if secdelta>2.0 else "3" if secdelta>1.0 else "4" if secdelta>0.0 else "0")))
						else:
							clist.append("%.6lf" % (usectime/1000000.0))
					clist.append(decimal_number(metaversion))
					if metaid!=None:
						clist.append((("%016X" % metaid),("1" if metaid != master_metaid else "4")))
					else:
						clist.append("-")
					if metadelay==None:
						clist.append("not available")
					else:
						if ttymode:
							clist.append((("%.0f s" % metadelay),("4" if metadelay<1.0 else "3" if metadelay<6.0 else "1")))
						else:
							clist.append(int(metadelay))
					if memusage>0:
						if ttymode:
							clist.append(humanize_number(memusage," "))
						else:
							clist.append(memusage)
					else:
						clist.append("not available")
					if syscpu>0 or usercpu>0:
						if ttymode:
							clist.append("all:%.2f%% sys:%.2f%% user:%.2f%%" % (syscpu+usercpu,syscpu,usercpu))
						else:
							clist.append("all:%.7f%% sys:%.7f%% user:%.7f%%" % (syscpu+usercpu,syscpu,usercpu))
					else:
						clist.append("not available")
					if lastsuccessfulstore>0:
						if ttymode:
							clist.append(time.asctime(time.localtime(lastsuccessfulstore)))
							clist.append(timeduration_to_shortstr(lastsaveseconds))
						else:
							clist.append(lastsuccessfulstore)
							clist.append("%.3f" % lastsaveseconds)
					else:
						clist.append("-")
						clist.append("-")
					if lastsuccessfulstore>0 or lastsavestatus>0:
						clist.append(("Saved in background","4") if lastsavestatus==0 else ("Downloaded from other master","4") if lastsavestatus==1 else ("Saved in foreground","2") if lastsavestatus==2 else ("CRC saved in background","5") if lastsavestatus==3 else ("Unknown status: %u" % lastsavestatus,"1"))
					else:
						clist.append("-")
					if lastsuccessfulstore>0 and (lastsavestatus==0 or lastsavestatus>=2) and lastsavemetaversion!=None:
						clist.append((decimal_number(lastsavemetaversion),("4" if lastsavemetaversion==highest_saved_metaversion else "8")))
					else:
						clist.append("-")
					if lastsuccessfulstore>0 and (lastsavestatus==0 or lastsavestatus>=2) and lastsavemetaversion!=None and lastsavemetachecksum!=None:
						clist.append((("%08X" % lastsavemetachecksum),("4" if lastsavemetaversion==highest_saved_metaversion and lastsavemetachecksum==highest_metaversion_checksum else "1" if lastsavemetaversion==highest_saved_metaversion and lastsavemetachecksum!=highest_metaversion_checksum else "8")))
					else:
						clist.append("-")
					if exportschecksum!=None:
						clist.append((("%016X" % exportschecksum),("1" if exportschecksum != master_exportschecksum else "4")))
					else:
						clist.append("-")
					tab.append(*clist)

			if len(masterstab)==0:
				if cgimode:
					out.append("""	<tr><td colspan="10">Servers not found! Check your DNS</td></tr>""")
				else:
					tab.append(("""Servers not found! Check your DNS""","c",9))

			if cgimode:
				out.append("""</table>""")
				print("\n".join(out))
			elif jsonmode:
				json_in_dict["masters"] = json_im_array
			else:
				print(myunicode(tab))
		except Exception:
			print_exception()


	if "IC" in sectionsubset and leaderfound:
		try:
			(matrix,progressstatus)=dataprovider.get_matrix(ICsclassid)
			sclass_dict = []
			sclass_name = "Error"
			sclass_desc = "Error"
			if masterconn.has_feature(FEATURE_SCLASS_IN_MATRIX):
				sclass_dict = dataprovider.get_storage_classes()
				if ICsclassid<0:
					sclass_name = ""
					sclass_desc = "all storage classes"
				elif ICsclassid in sclass_dict:
					sclass_name = sclass_dict[ICsclassid].name
					sclass_desc = "storage class '%s'" % sclass_name
				else:
					sclass_name = "[%u]" % ICsclassid
					sclass_desc = "non existent storage class id:%u" % ICsclassid
			progressstr = "disconnections" if (progressstatus==1) else "connections" if (progressstatus==2) else "connections and disconnections"
			if len(matrix)==2 or len(matrix)==6 or len(matrix)==8:
				if cgimode:
					out = []
					out.append("""<form action="#">""")
					out.append("""<div class="tab_title">Chunk matrix table</div>""")
					out.append("""<table class="acid_tab acid_tab_storageid_mfsmatrix" cellspacing="0" id="mfsmatrix">""")
					out.append("""	<tr><th colspan="14">""") 
					out.append("""<div style="display: flex; align-items: center;">""")
					options=[(-45, "Regular chunks only", "'marked for removal' excluded","acid_tab.switchdisplay('mfsmatrix','matrixar_vis',1);"),
							(45, "All chunks","'marked for removal' included","acid_tab.switchdisplay('mfsmatrix','matrixar_vis',0);")]
					out.append(html_knob_selector("matrixar_vis",12,(380,42),(190,20),options))
					if len(matrix)==6:
						options=[(-45, "Copies and EC chunks", None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',0);"),
								(45, "Copies only",None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',1);"),
								(135, "EC chunks only",None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',2);")]
						out.append(html_knob_selector("matrixec_vis",12,(340,42),(170,20),options))
					elif len(matrix)==8:
						options=[(-45,"Copies and EC chunks", None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',0);"),
								(45,"EC8 chunks only",None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',2);"),
								(135,"EC4 chunks only",None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',3);"),
								(-135,"Copies only",None,"acid_tab.switchdisplay('mfsmatrix','matrixec_vis',1);")]
						out.append(html_knob_selector("matrixec_vis",12,(350,42),(175,20),options))

					if len(sclass_dict)>1 and selectable:
						sclass_used=[]
						name_too_long=False
						for sclass in sclass_dict:
							if ICsclassid!=sclass:
								if sclass_dict[sclass].has_chunks:
									sclass_used.append((sclass,sclass_dict[sclass].name))
									if len(sclass_dict[sclass].name)>17:
										name_too_long=True
							else:
								sclass_used.append((sclass,sclass_dict[sclass].name))
								if len(sclass_dict[sclass].name)>17:
									name_too_long=True					
						sclass_count=len(sclass_used)
						if sclass_count<=5 and not name_too_long: #just a few (max 5+1=6) storage classes, use a knob 
							angles=[[-45],[-45,45],[-45,45,135],[-135,-45,45,135],[-135,-45,45,90,135],[-135,-90,-45,45,90,135]]
							options=[(angles[sclass_count][0],"All storage classes", None,"document.location.href='%s&ICsclassid=-1'" % createrawlink({"ICsclassid":""}))]
							i=1
							for (sclass, name) in sclass_used:
								options.append((angles[sclass_count][i],name, None,"document.location.href='%s&ICsclassid=%u'" % (createrawlink({"ICsclassid":""}),sclass)))
								i+=1
							out.append(html_knob_selector("sclass_knob",12,(340,43),(170,21),options))
						else: #too many classes to display as a knob, use a drop-down instead 
							out.append("""	<label for="storage_class_select">Storage class </label><div class="select-fl"><select id="storage_class_select" name="storage_class" onchange="document.location.href='%s&ICsclassid='+this.options[this.selectedIndex].value">""" % createrawlink({"ICsclassid":""}))
							if ICsclassid>=0:
								out.append("""		<option value="-1">all storage classes</option>""")
							else:
								out.append("""		<option value="-1" selected="selected">all storage classes</option>""")
							for sclass in sclass_dict:
								if ICsclassid!=sclass:
									if sclass_dict[sclass].has_chunks:
										out.append("""		<option value="%u">%s</option>""" % (sclass,sclass_dict[sclass].name))
								else:
									out.append("""		<option value="%u" selected="selected">%s</option>""" % (sclass,sclass_dict[sclass].name))
							if ICsclassid>=0 and ICsclassid not in sclass_dict:
								out.append("""		<option value="%u" selected="selected">nonexistent class number %u</option>""" % (ICsclassid,ICsclassid))
							out.append("""	</select><span class="arrow"></span></div>""")
					out.append("""</div>""")
					out.append("""	</th></tr>""")
					if progressstatus>0:
						out.append("""<tr><th colspan="14"><span class="WARNING">Warning: counters may not be valid - %s in progress</span></th></tr>""" % progressstr)
					out.append("""	<tr>""")
					out.append("""		<th rowspan="2" colspan="2" class="acid_tab_skip">""")
					out.append("""		</th>""")
					out.append("""		<th colspan="12" class="acid_tab_skip">""")
					line = []
					line.append("""<span class="matrixec_vis0">Actual Redundancy Level</span>""")
					line.append("""<span class="matrixec_vis1">Actual number of full copies</span>""")
					line.append("""<span class="matrixec_vis2">Actual number of EC8 parts</span>""")
					if len(matrix)>=8:
						line.append("""<span class="matrixec_vis3">Actual number of EC4 parts</span>""")
					line.append("""<span class="matrixar_vis0">, 'marked for removal' included</span>""")
					line.append("""<span class="matrixar_vis1">, 'marked for removal' excluded</span>""")	
					line.append("""<span class="matrixec_vis0">&nbsp;<small>('RL = n' means EC: 8&#x202F;+n or 4&#x202F;+n&#x202F;parts, GOAL: 1 +n&#x202F;copies)</small></span>""")
					out.append("".join(line))
					out.append("""		</th>""")
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					for rl in xrange(11):
						out.append("""		<th class="acid_tab_skip" style="min-width:50px;">""")
						out.append("""			<span class="matrixec_vis0">%s</span>""" % ("missing" if (rl==0) else "RL&#x202F;>=&#x202F;9" if (rl==10) else ("RL&#x202F;=&#x202F;%u" % (rl-1))))
						out.append("""			<span class="matrixec_vis1">%s</span>""" % ("missing" if (rl==0) else ("1&#x202F;+%u&#x202F;copies" % (rl-1))))
						out.append("""			<span class="matrixec_vis2">%s</span>""" % ("missing" if (rl==0) else ("8&#x202F;+%u&#x202F;parts" % (rl-1))))
						if len(matrix)>=8:
							out.append("""			<span class="matrixec_vis3">%s</span>""" % ("missing" if (rl==0) else ("4&#x202F;+%u&#x202F;parts" % (rl-1))))
						out.append("""		</th>""")
					out.append("""		<th class="acid_tab_skip" style="min-width:50px;">all</th>""")
					out.append("""	</tr>""")
				elif jsonmode:
					json_ic_dict = {}
					json_ic_sum_dict = {}
					matrixkeys = ['allchunks','regularchunks','allchunks_copies','regularchunks_copies','allchunks_ec8','regularchunks_ec8','allchunks_ec4', 'regularchunks_ec4']
					sumkeys = ['missing','endangered','undergoal','stable','overgoal','pending_deletion','ready_to_remove']
					#sumlist has to be clean for JSON mode
					sumlist = []
				elif ttymode:
					tabtypesc = "" if (ICsclassid<0) else (" (data only for %s)" % sclass_desc)
					tabtypear = "Regular" if (ICmatrix&1) else "All"
					tabtypeec = "Both copies and EC chunks" if ((ICmatrix>>1)==0) else "Only chunks stored in full copies" if ((ICmatrix>>1)==1) else "Only chunks stored using Raid-like Erasure Codes in 8+N format" if ((ICmatrix>>1)==2) else "Only chunks stored using Raid-like Erasure Codes in 4+N format"
					tab = Table("%s chunks state matrix%s - %s mode" % (tabtypear,tabtypesc,tabtypeec),13,"r")
					if progressstatus>0:
						tab.header(("Warning: counters may not be valid - %s in progress" % progressstr,"1c",13))
						tab.header(("---","",13))
					if (ICmatrix>>1)==0:
						tab.header("target",("current redundancy level (RL = N means EC: (8+N) or (4+N) parts, GOAL: (1+N) copies)","",12))
					elif (ICmatrix>>1)==1:
						tab.header("target",("current number of valid copies","",12))
					elif (ICmatrix>>1)==2:
						tab.header("target",("current number of EC8 parts","",12))
					else:
						tab.header("target",("current number of EC4 parts","",12))
					tab.header("redundancy",("---","",12))
					if (ICmatrix>>1)==0:
						tab.header("level"," missing ","  RL = 0  ","  RL = 1  ","  RL = 2  ","  RL = 3  ","  RL = 4  ","  RL = 5  ","  RL = 6  ","  RL = 7  ","  RL = 8  ","  RL >= 9  ","   all   ")
					elif (ICmatrix>>1)==1:
						tab.header("level"," missing ","1+0 copies","1+1 copies","1+2 copies","1+3 copies","1+4 copies","1+5 copies","1+6 copies","1+7 copies","1+8 copies",">1+9 copies","   all   ")
					elif (ICmatrix>>1)==2:
						tab.header("level"," missing ","8+0 parts ","8+1 parts ","8+2 parts ","8+3 parts ","8+4 parts ","8+5 parts ","8+6 parts ","8+7 parts ","8+8 parts ",">8+9 parts ","   all   ")
					else:
						tab.header("level"," missing ","4+0 parts ","4+1 parts ","4+2 parts ","4+3 parts ","4+4 parts ","4+5 parts ","4+6 parts ","4+7 parts ","4+8 parts ",">4+9 parts ","   all   ")
				else:
					out = []
					if ICmatrix==0:
						mtypeprefix=("all chunks matrix:%s" % plaintextseparator)
					elif ICmatrix==1:
						mtypeprefix=("regular chunks matrix:%s" % plaintextseparator)
					elif ICmatrix==2:
						mtypeprefix=("all chunks matrix - copies only:%s" % plaintextseparator)
					elif ICmatrix==3:
						mtypeprefix=("regular chunks matrix - copies only:%s" % plaintextseparator)
					elif ICmatrix==4:
						mtypeprefix=("all chunks matrix - EC8 only:%s" % plaintextseparator)
					elif ICmatrix==5:
						mtypeprefix=("regular chunks matrix - EC8 only:%s" % plaintextseparator)
					elif ICmatrix==6:
						mtypeprefix=("all chunks matrix - EC4 only:%s" % plaintextseparator)
					elif ICmatrix==7:
						mtypeprefix=("regular chunks matrix - EC4 only:%s" % plaintextseparator)
					else:
						mtypeprefix=("UNKNOWN chunks matrix:%s" % plaintextseparator)
					if ICsclassid>=0:
						mtypeprefix=("%s%s%s" % (mtypeprefix,sclass_desc,plaintextseparator))
				classsum = []
				sumlist = []
				for i in xrange(len(matrix)):
					classsum.append(7*[0])
					sumlist.append(11*[0])
				left_col_once=1
				for goal in range(11):
					if cgimode:
						out.append("""	<tr>""")
						if left_col_once:
							vertical_title = """<svg viewbox="0 0 16 220" width="16" height="220" xmlns="http://www.w3.org/2000/svg"><text transform="rotate(-90)" style="font-family: arial, verdana, sans-serif; font-size: 13px; text-anchor: middle;" x="-110" y="12">PLACEHOLDER</text></svg>"""
							out.append("""<th rowspan="12" style="min-width: 16px;" class="acid_tab_skip">""")
							out.append("""			<div class="matrixec_vis0">%s</div>""" % vertical_title.replace("PLACEHOLDER", "Expected Redundancy Level (RL)"))
							out.append("""			<div class="matrixec_vis1">%s</div>""" % vertical_title.replace("PLACEHOLDER", "Expected number of copies"))
							out.append("""			<div class="matrixec_vis2">%s</div>""" % vertical_title.replace("PLACEHOLDER", "Expected number of parts"))
							if len(matrix)>=8:
								out.append("""			<div class="matrixec_vis3">%s</div>""" % vertical_title.replace("PLACEHOLDER", "Expected number of parts"))
							out.append("""</th>""")
							left_col_once=0

						if goal==0:
							out.append("""		<th align="center" class="acid_tab_skip">deleted</th>""")
						else:
							out.append("""		<th align="center" class="acid_tab_skip" style="min-width:50px;">""")
							out.append("""			<span class="matrixec_vis0">RL&#x202F;=&#x202F;%u</span>""" % (goal-1))
							out.append("""			<span class="matrixec_vis1">%s</span>""" % ("-" if (goal>9) else ("1&#x202F;+%u&#x202F;copies" % (goal-1))))
							out.append("""			<span class="matrixec_vis2">%s</span>""" % ("-" if (goal==1) else ("8&#x202F;+%u&#x202F;parts" % (goal-1))))
							if len(matrix)>=8:
								out.append("""			<span class="matrixec_vis3">%s</span>""" % ("-" if (goal==1) else ("4&#x202F;+%u&#x202F;parts" % (goal-1))))
							out.append("""		</th>""")
					else:
						if goal==0:
							clist = ["deleted"]
						else:
							if (ICmatrix>>1)==0:
								clist = ["RL = %u" % (goal-1)]
							elif (ICmatrix>>1)==1:
								if goal>9:
									clist = ["-"]
								else:
									clist = ["1+%u copies" % (goal-1)]
							elif (ICmatrix>>1)==2:
								if goal==1:
									clist = ["-"]
								else:
									clist = ["8+%u parts" % (goal-1)]
							else:
								if goal==1:
									clist = ["-"]
								else:
									clist = ["4+%u parts" % (goal-1)]
					for vc in range(11):
						if goal==0:
							if vc==0:
								cl = "DELETEREADY"
								clidx = 6
							else:
								cl = "DELETEPENDING"
								clidx = 5
						elif vc==0:
							cl = "MISSING"
							clidx = 0
						elif vc>goal:
							cl = "OVERGOAL"
							clidx = 4
						elif vc<goal:
							if vc==1:
								cl = "ENDANGERED"
								clidx = 1
							else:
								cl = "UNDERGOAL"
								clidx = 2
						else:
							cl = "NORMAL"
							clidx = 3
						for i in xrange(len(matrix)):
							classsum[i][clidx]+=matrix[i][goal][vc]
						if cgimode:
							out.append("""		<td align="right" class="acid_tab_skip">""")
							if len(matrix)==8:
								out.append("""			<span class="matrixar_vis0">""")
								if matrix[0][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis0">%s</span>""" % (cl,decimal_number_html(matrix[0][goal][vc])))
								if matrix[2][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis1">%s</span>""" % (cl,decimal_number_html(matrix[2][goal][vc])))
								if matrix[4][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis2">%s</span>""" % (cl,decimal_number_html(matrix[4][goal][vc])))
								if matrix[6][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis3">%s</span>""" % (cl,decimal_number_html(matrix[6][goal][vc])))
								out.append("""			</span>""")
								out.append("""			<span class="matrixar_vis1">""")
								if matrix[1][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis0">%s</span>""" % (cl,decimal_number_html(matrix[1][goal][vc])))
								if matrix[3][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis1">%s</span>""" % (cl,decimal_number_html(matrix[3][goal][vc])))
								if matrix[5][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis2">%s</span>""" % (cl,decimal_number_html(matrix[5][goal][vc])))
								if matrix[7][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis3">%s</span>""" % (cl,decimal_number_html(matrix[7][goal][vc])))
								out.append("""			</span>""")
							elif len(matrix)==6:
								out.append("""			<span class="matrixar_vis0">""")
								if matrix[0][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis0">%s</span>""" % (cl,decimal_number_html(matrix[0][goal][vc])))
								if matrix[2][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis1">%s</span>""" % (cl,decimal_number_html(matrix[2][goal][vc])))
								if matrix[4][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis2">%s</span>""" % (cl,decimal_number_html(matrix[4][goal][vc])))
								out.append("""			</span>""")
								out.append("""			<span class="matrixar_vis1">""")
								if matrix[1][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis0">%s</span>""" % (cl,decimal_number_html(matrix[1][goal][vc])))
								if matrix[3][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis1">%s</span>""" % (cl,decimal_number_html(matrix[3][goal][vc])))
								if matrix[5][goal][vc]>0:
									out.append("""			<span class="%s matrixec_vis2">%s</span>""" % (cl,decimal_number_html(matrix[5][goal][vc])))
								out.append("""			</span>""")
							else:
								if matrix[0][goal][vc]>0:
									out.append("""			<span class="%s matrixar_vis0">%s</span>""" % (cl,decimal_number_html(matrix[0][goal][vc])))
								if matrix[1][goal][vc]>0:
									out.append("""			<span class="%s matrixar_vis1">%s</span>""" % (cl,decimal_number_html(matrix[1][goal][vc])))
							out.append("""		</td>""")
						elif jsonmode:
							pass
						elif ttymode:
							if matrix[ICmatrix][goal][vc]>0:
								clist.append((matrix[ICmatrix][goal][vc],"1234678"[clidx]))
							else:
								clist.append("-")
						else:
							if matrix[ICmatrix][goal][vc]>0:
								if goal==0:
									goalstr = "missing"
								else:
									if ICmatrix>>1==0:
										goalstr = "%u" % (goal-1)
									elif ICmatrix>>1==1:
										goalstr = "1+%u" % (goal-1)
									elif ICmatrix>>1==2:
										goalstr = "8+%u" % (goal-1)
									else:
										goalstr = "4+%u" % (goal-1)
								if vc==0:
									vcstr = "missing"
								else:
									if ICmatrix>>1==0:
										vcstr = "%u" % (vc-1)
									elif ICmatrix>>1==1:
										vcstr = "1+%u" % (vc-1)
									elif ICmatrix>>1==2:
										vcstr = "8+%u" % (vc-1)
									else:
										vcstr = "4+%u" % (vc-1)
								if ICmatrix>>1==0:
									descstr = "redundancy level"
								elif ICmatrix>>1==1:
									descstr = "copies"
								elif ICmatrix>>1==2:
									descstr = "EC8 parts"
								else:
									descstr = "EC4 parts"
								out.append("""%s%s target/current/chunks:%s%s%s%s%s%u""" % (mtypeprefix,descstr,plaintextseparator,goalstr,plaintextseparator,vcstr,plaintextseparator,matrix[ICmatrix][goal][vc]))
					if cgimode:
						if goal==0:
							cl="IGNORE"
						else:
							cl=""
						out.append("""		<td align="right" class="acid_tab_skip">""")
						if len(matrix)==8:
							out.append("""			<span class="matrixar_vis0">""")
							out.append("""				<span class="%s matrixec_vis0">%s</span>""" % (cl,decimal_number_html(sum(matrix[0][goal]))))
							out.append("""				<span class="%s matrixec_vis1">%s</span>""" % (cl,decimal_number_html(sum(matrix[2][goal]))))
							out.append("""				<span class="%s matrixec_vis2">%s</span>""" % (cl,decimal_number_html(sum(matrix[4][goal]))))
							out.append("""				<span class="%s matrixec_vis3">%s</span>""" % (cl,decimal_number_html(sum(matrix[6][goal]))))
							out.append("""			</span>""")
							out.append("""			<span class="matrixar_vis1">""")
							out.append("""				<span class="%s matrixec_vis0">%s</span>""" % (cl,decimal_number_html(sum(matrix[1][goal]))))
							out.append("""				<span class="%s matrixec_vis1">%s</span>""" % (cl,decimal_number_html(sum(matrix[3][goal]))))
							out.append("""				<span class="%s matrixec_vis2">%s</span>""" % (cl,decimal_number_html(sum(matrix[5][goal]))))
							out.append("""				<span class="%s matrixec_vis3">%s</span>""" % (cl,decimal_number_html(sum(matrix[7][goal]))))
							out.append("""			</span>""")
						elif len(matrix)==6:
							out.append("""			<span class="matrixar_vis0">""")
							out.append("""				<span class="%s matrixec_vis0">%s</span>""" % (cl,decimal_number_html(sum(matrix[0][goal]))))
							out.append("""				<span class="%s matrixec_vis1">%s</span>""" % (cl,decimal_number_html(sum(matrix[2][goal]))))
							out.append("""				<span class="%s matrixec_vis2">%s</span>""" % (cl,decimal_number_html(sum(matrix[4][goal]))))
							out.append("""			</span>""")
							out.append("""			<span class="matrixar_vis1">""")
							out.append("""				<span class="%s matrixec_vis0">%s</span>""" % (cl,decimal_number_html(sum(matrix[1][goal]))))
							out.append("""				<span class="%s matrixec_vis1">%s</span>""" % (cl,decimal_number_html(sum(matrix[3][goal]))))
							out.append("""				<span class="%s matrixec_vis2">%s</span>""" % (cl,decimal_number_html(sum(matrix[5][goal]))))
							out.append("""			</span>""")
						else:
							out.append("""			<span class="%s matrixar_vis0">%s</span>""" % (cl,decimal_number_html(sum(matrix[0][goal]))))
							out.append("""			<span class="%s matrixar_vis1">%s</span>""" % (cl,decimal_number_html(sum(matrix[1][goal]))))
						out.append("""		</td>""")
						out.append("""	</tr>""")
					elif ttymode:
						clist.append(sum(matrix[ICmatrix][goal]))
						tab.append(*clist)
					if goal>0:
						for i in xrange(len(matrix)):
							sumlist[i] = [ a + b for (a,b) in zip(sumlist[i],matrix[i][goal])]
				if cgimode:
					out.append("""	<tr>""")
					out.append("""		<th align="center" class="acid_tab_skip">all 1+</th>""")
					for vc in range(11):
						out.append("""		<td align="right" class="acid_tab_skip">""")
						if len(matrix)==8:
							out.append("""			<span class="matrixar_vis0">""")
							out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sumlist[0][vc]))
							out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sumlist[2][vc]))
							out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sumlist[4][vc]))
							out.append("""				<span class="matrixec_vis3">%s</span>""" % decimal_number_html(sumlist[6][vc]))
							out.append("""			</span>""")
							out.append("""			<span class="matrixar_vis1">""")
							out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sumlist[1][vc]))
							out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sumlist[3][vc]))
							out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sumlist[5][vc]))
							out.append("""				<span class="matrixec_vis3">%s</span>""" % decimal_number_html(sumlist[7][vc]))
							out.append("""			</span>""")
						elif len(matrix)==6:
							out.append("""			<span class="matrixar_vis0">""")
							out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sumlist[0][vc]))
							out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sumlist[2][vc]))
							out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sumlist[4][vc]))
							out.append("""			</span>""")
							out.append("""			<span class="matrixar_vis1">""")
							out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sumlist[1][vc]))
							out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sumlist[3][vc]))
							out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sumlist[5][vc]))
							out.append("""			</span>""")
						else:
							out.append("""			<span class="matrixar_vis0">%s</span>""" % decimal_number_html(sumlist[0][vc]))
							out.append("""			<span class="matrixar_vis1">%s</span>""" % decimal_number_html(sumlist[1][vc]))
						out.append("""		</td>""")
					out.append("""		<td align="right" class="acid_tab_skip">""")
					if len(matrix)==8:
						out.append("""			<span class="matrixar_vis0">""")
						out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sum(sumlist[0])))
						out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sum(sumlist[2])))
						out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sum(sumlist[4])))
						out.append("""				<span class="matrixec_vis3">%s</span>""" % decimal_number_html(sum(sumlist[6])))
						out.append("""			</span>""")
						out.append("""			<span class="matrixar_vis1">""")
						out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sum(sumlist[1])))
						out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sum(sumlist[3])))
						out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sum(sumlist[5])))
						out.append("""				<span class="matrixec_vis3">%s</span>""" % decimal_number_html(sum(sumlist[7])))
						out.append("""			</span>""")
					elif len(matrix)==6:
						out.append("""			<span class="matrixar_vis0">""")
						out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sum(sumlist[0])))
						out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sum(sumlist[2])))
						out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sum(sumlist[4])))
						out.append("""			</span>""")
						out.append("""			<span class="matrixar_vis1">""")
						out.append("""				<span class="matrixec_vis0">%s</span>""" % decimal_number_html(sum(sumlist[1])))
						out.append("""				<span class="matrixec_vis1">%s</span>""" % decimal_number_html(sum(sumlist[3])))
						out.append("""				<span class="matrixec_vis2">%s</span>""" % decimal_number_html(sum(sumlist[5])))
						out.append("""			</span>""")
					else:
						out.append("""			<span class="matrixar_vis0">%s</span>""" % decimal_number_html(sum(sumlist[0])))
						out.append("""			<span class="matrixar_vis1">%s</span>""" % decimal_number_html(sum(sumlist[1])))
					out.append("""		</td>""")
					out.append("""	</tr>""")
					out.append("""	<tr><th align="center" class="acid_tab_skip"></th><td colspan="13" class="acid_tab_skip" style="padding-left:80px;padding-right:80px;">""")
					if len(matrix)==8:
						out.append("""		<span class="matrixar_vis0">""")
						out.append("""			<span class="matrixec_vis0">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[0][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""			<span class="matrixec_vis1">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[2][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""			<span class="matrixec_vis2">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[4][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""			<span class="matrixec_vis3">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[6][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""		</span>""")
						out.append("""		<span class="matrixar_vis1">""")
						out.append("""			<span class="matrixec_vis0">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[1][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""			<span class="matrixec_vis1">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[3][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""			<span class="matrixec_vis2">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[5][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""			<span class="matrixec_vis3">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[7][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""		</span>""")
					elif len(matrix)==6:
						out.append("""		<span class="matrixar_vis0">""")
						out.append("""			<span class="matrixec_vis0">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[0][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""			<span class="matrixec_vis1">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[2][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""			<span class="matrixec_vis2">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[4][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""		</span>""")
						out.append("""		<span class="matrixar_vis1">""")
						out.append("""			<span class="matrixec_vis0">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[1][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""			<span class="matrixec_vis1">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[3][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""			<span class="matrixec_vis2">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[5][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""		</span>""")
					else:
						out.append("""		<span class="matrixar_vis0">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[0][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
						out.append("""		<span class="matrixar_vis1">"""+("&nbsp;&nbsp;&nbsp;&nbsp;".join(["""<span class="%sBOX"></span>&nbsp;%s <span class="%s">%s</span>""" % (cl,desc,cl,decimal_number_html(classsum[1][clidx])) for clidx,cl,desc in [(0,"MISSING","missing"),(1,"ENDANGERED","endangered"),(2,"UNDERGOAL","undergoal"),(3,"NORMAL","stable"),(4,"OVERGOAL","overgoal"),(5,"DELETEPENDING","pending&nbsp;deletion"),(6,"DELETEREADY","ready&nbsp;to&nbsp;be&nbsp;removed")]]))+"</span>")
					out.append("""	</td></tr>""")
					out.append("""</table></form>""")
					print("\n".join(out))
				elif jsonmode:
					#Go through all available matrices:
					for mx in xrange(len(matrix)):
						classsum = []
						for i in xrange(len(matrix)):
							classsum.append(7*[0])
							sumlist.append(11*[0])
						cssumdict = {}
						mxarray = []
						if mx>7:
							mtypeprefix = "unknown"
						else:
							mtypeprefix = matrixkeys[mx]
						for goal in xrange(11):
							for vc in xrange(11):
								tccdict = {}
								#Rules for summ
								if goal==0:
									if vc==0:
										clidx = 6
									else:
										clidx = 5
								elif vc==0:
									clidx = 0
								elif vc>goal:
									clidx = 4
								elif vc<goal:
									if vc==1:
										clidx = 1
									else:
										clidx = 2
								else:
									clidx = 3
								for i in xrange(len(matrix)):
									classsum[i][clidx]+=matrix[i][goal][vc]
								#Goal String
								if matrix[mx][goal][vc]>0:
									if goal==0:
										goalstr = "0"
									else:
										if mx>>1==0:
											goalstr = "%u" % (goal)
										elif mx>>1==1:
											goalstr = "1+%u" % (goal)
										elif mx>>1==2:
											goalstr = "8+%u" % (goal)
										else:
											goalstr = "4+%u" % (goal)
									if vc==0:
										vcstr = "0"
									else:
										if mx>>1==0:
											vcstr = "%u" % (vc)
										elif mx>>1==1:
											vcstr = "1+%u" % (vc)
										elif mx>>1==2:
											vcstr = "8+%u" % (vc)
										else:
											vcstr = "4+%u" % (vc)
									if mx>>1==0:
										descstr = "redundancy level"
									elif mx>>1==1:
										descstr = "copies"
									elif mx>>1==2:
										descstr = "EC 8 parts"
									else:
										descstr = "EC 4 parts"
									tccdict['target'] = goalstr
									tccdict['current'] = vcstr
									tccdict['chunks'] = matrix[mx][goal][vc]
									mxarray.append(tccdict)
						
						for index,key in enumerate(sumkeys):
							cssumdict[key] = classsum[mx][index]
						json_ic_sum_dict[matrixkeys[mx]] = cssumdict
						json_ic_dict[mtypeprefix]=mxarray

					json_ic_dict['summary'] = json_ic_sum_dict
					json_ic_dict['progress_status'] = progressstatus
					if progressstatus>0:
						json_ic_dict['progress_str'] = "counters may not be valid - %s in progress" % progressstr
					else:
						json_ic_dict['progress_str'] = "counters are valid"
					json_ic_dict['storage_class_id'] = ICsclassid
					json_ic_dict['storage_class_name'] = sclass_name
					json_ic_dict['storage_class_desc'] = sclass_desc
					#Add created dictionaries to main dictionary
					json_in_dict['chunks'] = json_ic_dict
				elif ttymode:
					clist = ["all 1+"]
					for vc in range(11):
						clist.append(sumlist[ICmatrix][vc])
					clist.append(sum(sumlist[ICmatrix]))
					tab.append(*clist)
					tab.append(("---","",13))
					#tab.append(("missing: %s%u%s / endangered: %s%u%s / undergoal: %s%u%s / stable: %s%u%s / overgoal: %s%u%s / pending deletion: %s%u%s / to be removed: %s%u%s" % (colorcode[0],classsum[0],ttyreset,colorcode[1],classsum[1],ttyreset,colorcode[2],classsum[2],ttyreset,colorcode[3],classsum[3],ttyreset,colorcode[5],classsum[4],ttyreset,colorcode[6],classsum[5],ttyreset,colorcode[7],classsum[6],ttyreset),"c",13))
					tab.append(("missing: %u / endangered: %u / undergoal: %u / stable: %u / overgoal: %u / pending deletion: %u / to be removed: %u" % (classsum[ICmatrix][0],classsum[ICmatrix][1],classsum[ICmatrix][2],classsum[ICmatrix][3],classsum[ICmatrix][4],classsum[ICmatrix][5],classsum[ICmatrix][6]),"c",13))
#							out.append("chunkclass missing: %s%u%s" % (colorcode[0],classsum[0],ttyreset))
#							out.append("chunkclass endangered: %s%u%s" % (colorcode[1],classsum[1],ttyreset))
#							out.append("chunkclass undergoal: %s%u%s" % (colorcode[2],classsum[2],ttyreset))
#							out.append("chunkclass stable: %s%u%s" % (colorcode[3],classsum[3],ttyreset))
#							out.append("chunkclass overgoal: %s%u%s" % (colorcode[4],classsum[4],ttyreset))
#							out.append("chunkclass pending deletion: %s%u%s" % (colorcode[5],classsum[5],ttyreset))
#							out.append("chunkclass to be removed: %s%u%s" % (colorcode[6],classsum[6],ttyreset))
					print(myunicode(tab))
				else:
					out.append("%schunkclass missing:%s%u" % (mtypeprefix,plaintextseparator,classsum[ICmatrix][0]))
					out.append("%schunkclass endangered:%s%u" % (mtypeprefix,plaintextseparator,classsum[ICmatrix][1]))
					out.append("%schunkclass undergoal:%s%u" % (mtypeprefix,plaintextseparator,classsum[ICmatrix][2]))
					out.append("%schunkclass stable:%s%u" % (mtypeprefix,plaintextseparator,classsum[ICmatrix][3]))
					out.append("%schunkclass overgoal:%s%u" % (mtypeprefix,plaintextseparator,classsum[ICmatrix][4]))
					out.append("%schunkclass pending deletion:%s%u" % (mtypeprefix,plaintextseparator,classsum[ICmatrix][5]))
					out.append("%schunkclass to be removed:%s%u" % (mtypeprefix,plaintextseparator,classsum[ICmatrix][6]))
					print(str(Table("",0))+"\n".join(out))
		except Exception:
			print_exception()

	if "IL" in sectionsubset and leaderfound:
		try: # Filesystem self-check loop
			if (masterconn.version_at_least(2,0,66) and masterconn.version_less_than(3,0,0)) or masterconn.version_at_least(3,0,19):
				data,length = masterconn.command(CLTOMA_FSTEST_INFO,MATOCL_FSTEST_INFO,struct.pack(">B",0))
				pver = 1
			else:
				data,length = masterconn.command(CLTOMA_FSTEST_INFO,MATOCL_FSTEST_INFO)
				pver = 0
			if length>=(36 + pver*8):
				if pver==1:
					loopstart,loopend,files,ugfiles,mfiles,mtfiles,msfiles,chunks,ugchunks,mchunks,msgbuffleng = struct.unpack(">LLLLLLLLLLL",data[:44])
					datastr = data[44:].decode('utf-8','replace')
				else:
					mtfiles = None
					msfiles = None
					loopstart,loopend,files,ugfiles,mfiles,chunks,ugchunks,mchunks,msgbuffleng = struct.unpack(">LLLLLLLLL",data[:36])
					datastr = data[36:].decode('utf-8','replace')
				if cgimode:
					out = []
					out.append("""<div class="tab_title">Filesystem self-check</div>""")
					out.append("""<table class="FR no-hover" cellspacing="0">""")
					out.append("""	<tr>""")
					out.append("""		<th colspan="2">Self-check loop</th>""")
					colspan = 5 if pver==1 else 3
					out.append("""		<th colspan="%d">Files (health)</th>""" % colspan)
					out.append("""		<th colspan="3">Chunks (health)</th>""")
					out.append("""	</tr>""")

					out.append("""	<tr>""")
					out.append("""		<th style="min-width: 100px;">start time</th>""")
					out.append("""		<th style="min-width: 100px;">end time</th>""")
					out.append("""		<th style="min-width: 100px;">checked</th>""")
					out.append("""		<th style="min-width: 100px;">missing</th>""")
					out.append("""		<th style="min-width: 100px;">undergoal</th>""")
					if pver==1:
						out.append("""		<th style="min-width: 100px;">missing in trash</th>""")
						out.append("""		<th style="min-width: 100px;">missing sustained</th>""")
					out.append("""		<th style="min-width: 100px;">checked</th>""")
					out.append("""		<th style="min-width: 100px;">missing</th>""")
					out.append("""		<th style="min-width: 100px;">undergoal</th>""")
					out.append("""	</tr>""")
					if loopstart>0:
						out.append("""	<tr>""")
						out.append("""		<td align="center">%s</td>""" % (time_to_str(loopstart),))
						out.append("""		<td align="center">%s</td>""" % (time_to_str(loopend),))
						out.append("""		<td align="center">%s</td>""" % decimal_number_html(files))
						out.append("""		<td align="center"><span class="%s">%s</span></td>""" % ("" if mfiles==0 else "MISSING", decimal_number_html(mfiles)))
						out.append("""		<td align="center"><span class="%s">%s</span></td>""" % ("" if ugfiles==0 else "UNDERGOAL", decimal_number_html(ugfiles)))
						if pver==1:
							out.append("""		<td align="center">%s</td>""" % decimal_number_html(mtfiles))
							out.append("""		<td align="center">%s</td>""" % decimal_number_html(msfiles))
						out.append("""		<td align="center">%s</td>""" % decimal_number_html(chunks))
						out.append("""		<td align="center"><span class="%s">%s</span></td>""" % ("" if mchunks==0 else "MISSING", decimal_number_html(mchunks)))
						out.append("""		<td align="center"><span class="%s">%s</span></td>""" % ("" if ugchunks==0 else "UNDERGOAL", decimal_number_html(ugchunks)))
						out.append("""	</tr>""")
						if msgbuffleng>0:
							if msgbuffleng==100000:
								out.append("""	<tr><th colspan="8">Important messages (first 100k):</th></tr>""")
							else:
								out.append("""	<tr><th colspan="8">Important messages:</th></tr>""")
							out.append("""	<tr>""")
							out.append("""		<td colspan="8" align="left"><pre>%s</pre></td>""" % (datastr.replace("&","&amp;").replace(">","&gt;").replace("<","&lt;")))
							out.append("""	</tr>""")
					else:
						out.append("""	<tr>""")
						out.append("""		<td colspan="%u" align="center">No data, self-check loop not finished yet</td>""" % (8 if pver==0 else 10))
						out.append("""	</tr>""")
					out.append("""</table>""")
					print("\n".join(out))
				elif jsonmode:
					if loopstart>0:
						json_il_dict = {}
						json_il_dict["start_time"] = loopstart
						json_il_dict["end_time"] = loopend
						json_il_dict["start_time_str"] = time.asctime(time.localtime(loopstart))
						json_il_dict["end_time_str"] = time.asctime(time.localtime(loopend))
						json_il_dict["files"] = files
						json_il_dict["undergoal_files"] = ugfiles
						json_il_dict["missing_files"] = mfiles
						json_il_dict["missing_trash_files"] = mtfiles
						json_il_dict["missing_sustained_files"] = msfiles
						json_il_dict["chunks"] = chunks
						if pver==1:
							json_il_dict["undergoal_chunks"] = ugchunks
							json_il_dict["missing_chunks"] = mchunks
						json_in_dict['fs_loop'] = json_il_dict
					else:
						json_in_dict['fs_loop'] = None
				elif ttymode:
					if pver==1:
						tab = Table("Filesystem self-check",10,"r")
						tabwidth = 10
						tab.header("check loop start time","check loop end time","files","under-goal files","missing files","missing trash files","missing sustained files","chunks","under-goal chunks","missing chunks")
					else:
						tab = Table("Filesystem self-check",8,"r")
						tabwidth = 8
						tab.header("check loop start time","check loop end time","files","under-goal files","missing files","chunks","under-goal chunks","missing chunks")
					if loopstart>0:
						if pver==1:
							tab.append((time.asctime(time.localtime(loopstart)),"c"),(time.asctime(time.localtime(loopend)),"c"),files,ugfiles,mfiles,mtfiles,msfiles,chunks,ugchunks,mchunks)
						else:
							tab.append((time.asctime(time.localtime(loopstart)),"c"),(time.asctime(time.localtime(loopend)),"c"),files,ugfiles,mfiles,chunks,ugchunks,mchunks)
						if msgbuffleng>0:
							tab.append(("---","",tabwidth))
							if msgbuffleng==100000:
								tab.append(("Important messages (first 100k):","c",tabwidth))
							else:
								tab.append(("Important messages:","c",tabwidth))
							tab.append(("---","",tabwidth))
							for line in datastr.strip().split("\n"):
								tab.append((line.strip(),"l",tabwidth))
					else:
						tab.append(("no data","c",tabwidth))
					print(myunicode(tab))
				else:
					out = []
					if loopstart>0:
						out.append("""check loop%sstart:%s%u""" % (plaintextseparator,plaintextseparator,loopstart))
						out.append("""check loop%send:%s%u""" % (plaintextseparator,plaintextseparator,loopend))
						out.append("""check loop%sfiles:%s%u""" % (plaintextseparator,plaintextseparator,files))
						out.append("""check loop%sunder-goal files:%s%u""" % (plaintextseparator,plaintextseparator,ugfiles))
						out.append("""check loop%smissing files:%s%u""" % (plaintextseparator,plaintextseparator,mfiles))
						if pver==1:
							out.append("""check loop%smissing trash files:%s%u""" % (plaintextseparator,plaintextseparator,mtfiles))
							out.append("""check loop%smissing sustained files:%s%u""" % (plaintextseparator,plaintextseparator,msfiles))
						out.append("""check loop%schunks:%s%u""" % (plaintextseparator,plaintextseparator,chunks))
						out.append("""check loop%sunder-goal chunks:%s%u""" % (plaintextseparator,plaintextseparator,ugchunks))
						out.append("""check loop%smissing chunks:%s%u""" % (plaintextseparator,plaintextseparator,mchunks))
						if msgbuffleng>0:
							for line in datastr.strip().split("\n"):
								out.append("check loop%simportant messages:%s%s" % (plaintextseparator,plaintextseparator,line.strip()))
					else:
						out.append("""check loop: no data""")
					print(str(Table("",0))+"\n".join(out))
		except Exception:
			print_exception()

		try: # Chunk operations loop
			data,length = masterconn.command(CLTOMA_CHUNKSTEST_INFO,MATOCL_CHUNKSTEST_INFO)
			if length==52:
				loopstart,loopend,del_invalid,ndel_invalid,del_unused,ndel_unused,del_dclean,ndel_dclean,del_ogoal,ndel_ogoal,rep_ugoal,nrep_ugoal,rebalance = struct.unpack(">LLLLLLLLLLLLL",data)
				if cgimode:
					out = []
					out.append("""<div class="tab_title">Chunk operations info</div>""")
					out.append("""<table class="FR no-hover" cellspacing="0">""")
					out.append("""	<tr>""")
					out.append("""		<th colspan="2">Loop time</th>""")
					out.append("""		<th colspan="4">Deletions</th>""")
					out.append("""		<th colspan="2">Replications</th>""")
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					out.append("""		<th>start</th>""")
					out.append("""		<th>end</th>""")
					out.append("""		<th>invalid</th>""")
					out.append("""		<th>unused</th>""")
					out.append("""		<th>disk clean</th>""")
					out.append("""		<th>over goal</th>""")
					out.append("""		<th>under goal</th>""")
					out.append("""		<th>rebalance</th>""")
					out.append("""	</tr>""")
					if loopstart>0:
						out.append("""	<tr>""")
						out.append("""		<td align="center">%s</td>""" % (time.asctime(time.localtime(loopstart)),))
						out.append("""		<td align="center">%s</td>""" % (time.asctime(time.localtime(loopend)),))
						out.append("""		<td align="right">%u/%u</td>""" % (del_invalid,del_invalid+ndel_invalid))
						out.append("""		<td align="right">%u/%u</td>""" % (del_unused,del_unused+ndel_unused))
						out.append("""		<td align="right">%u/%u</td>""" % (del_dclean,del_dclean+ndel_dclean))
						out.append("""		<td align="right">%u/%u</td>""" % (del_ogoal,del_ogoal+ndel_ogoal))
						out.append("""		<td align="right">%u/%u</td>""" % (rep_ugoal,rep_ugoal+nrep_ugoal))
						out.append("""		<td align="right">%u</td>""" % rebalance)
						out.append("""	</tr>""")
					else:
						out.append("""	<tr>""")
						out.append("""		<td colspan="8" align="center">no data</td>""")
						out.append("""	</tr>""")
					out.append("""</table>""")
					print("\n".join(out))
				elif jsonmode:
					if loopstart>0:
						json_il_dict = {}
						json_il_dict["start_time"] = loopstart
						json_il_dict["end_time"] = loopend
						json_il_dict["start_time_str"] = time.asctime(time.localtime(loopstart))
						json_il_dict["end_time_str"] = time.asctime(time.localtime(loopend))
						json_il_dict["del_invalid"] = del_invalid
						json_il_dict["ndel_invalid"] = ndel_invalid
						json_il_dict["del_unused"] = del_unused
						json_il_dict["ndel_unused"] = ndel_unused
						json_il_dict["del_dclean"] = del_dclean
						json_il_dict["ndel_dclean"] = ndel_dclean
						json_il_dict["del_ogoal"] = del_ogoal
						json_il_dict["ndel_ogoal"] = ndel_ogoal
						json_il_dict["rep_ugoal"] = rep_ugoal
						json_il_dict["nrep_ugoal"] = nrep_ugoal
						json_il_dict["rebalance"] = rebalance
						json_in_dict["chunk_loop"] = json_il_dict
					else:
						json_in_dict["chunk_loop"] = None
				elif ttymode:
					tab = Table("Chunk operations info",8,"r")
					tab.header(("loop time","",2),("deletions","",4),("replications","",2))
					tab.header(("---","",8))
					tab.header("start","end","invalid","unused","disk clean","over goal","under goal","rebalance")
					if loopstart>0:
						tab.append((time.asctime(time.localtime(loopstart)),"c"),(time.asctime(time.localtime(loopend)),"c"),"%u/%u" % (del_invalid,del_invalid+ndel_invalid),"%u/%u" % (del_unused,del_unused+ndel_unused),"%u/%u" % (del_dclean,del_dclean+ndel_dclean),"%u/%u" % (del_ogoal,del_ogoal+ndel_ogoal),"%u/%u" % (rep_ugoal,rep_ugoal+nrep_ugoal),rebalance)
					else:
						tab.append(("no data","c",8))
					print(myunicode(tab))
				else:
					out = []
					if loopstart>0:
						out.append("""chunk loop%sstart:%s%u""" % (plaintextseparator,plaintextseparator,loopstart))
						out.append("""chunk loop%send:%s%u""" % (plaintextseparator,plaintextseparator,loopend))
						out.append("""chunk loop%sdeletions%sinvalid:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_invalid,del_invalid+ndel_invalid))
						out.append("""chunk loop%sdeletions%sunused:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_unused,del_unused+ndel_unused))
						out.append("""chunk loop%sdeletions%sdisk clean:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_dclean,del_dclean+ndel_dclean))
						out.append("""chunk loop%sdeletions%sover goal:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_ogoal,del_ogoal+ndel_ogoal))
						out.append("""chunk loop%sreplications%sunder goal:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,rep_ugoal,rep_ugoal+nrep_ugoal))
						out.append("""chunk loop%sreplications%srebalance:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,rebalance))
					else:
						out.append("""chunk loop%sno data""" % plaintextseparator)
					print(str(Table("",0))+"\n".join(out))
			elif length==60:
				loopstart,loopend,del_invalid,ndel_invalid,del_unused,ndel_unused,del_dclean,ndel_dclean,del_ogoal,ndel_ogoal,rep_ugoal,nrep_ugoal,rebalance,locked_unused,locked_used = struct.unpack(">LLLLLLLLLLLLLLL",data)
				if cgimode:
					out = []
					out.append("""<div class="tab_title">Chunk operations info</div>""")
					out.append("""<table class="FR no-hover" cellspacing="0">""")
					out.append("""	<tr>""")
					out.append("""		<th colspan="2">Loop time</th>""")
					out.append("""		<th colspan="4">Deletions</th>""")
					out.append("""		<th colspan="2">Replications</th>""")
					out.append("""		<th colspan="2">Locked</th>""")
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					out.append("""		<th>start</th>""")
					out.append("""		<th>end</th>""")
					out.append("""		<th>invalid</th>""")
					out.append("""		<th>unused</th>""")
					out.append("""		<th>disk clean</th>""")
					out.append("""		<th>over goal</th>""")
					out.append("""		<th>under goal</th>""")
					out.append("""		<th>rebalance</th>""")
					out.append("""		<th>unused</th>""")
					out.append("""		<th>used</th>""")
					out.append("""	</tr>""")
					if loopstart>0:
						out.append("""	<tr>""")
						out.append("""		<td align="center">%s</td>""" % (time.asctime(time.localtime(loopstart)),))
						out.append("""		<td align="center">%s</td>""" % (time.asctime(time.localtime(loopend)),))
						out.append("""		<td align="right">%u/%u</td>""" % (del_invalid,del_invalid+ndel_invalid))
						out.append("""		<td align="right">%u/%u</td>""" % (del_unused,del_unused+ndel_unused))
						out.append("""		<td align="right">%u/%u</td>""" % (del_dclean,del_dclean+ndel_dclean))
						out.append("""		<td align="right">%u/%u</td>""" % (del_ogoal,del_ogoal+ndel_ogoal))
						out.append("""		<td align="right">%u/%u</td>""" % (rep_ugoal,rep_ugoal+nrep_ugoal))
						out.append("""		<td align="right">%u</td>""" % rebalance)
						out.append("""		<td align="right">%u</td>""" % locked_unused)
						out.append("""		<td align="right">%u</td>""" % locked_used)
						out.append("""	</tr>""")
					else:
						out.append("""	<tr>""")
						out.append("""		<td colspan="10" align="center">no data</td>""")
						out.append("""	</tr>""")
					out.append("""</table>""")
					print("\n".join(out))
				elif jsonmode:
					if loopstart>0:
						json_il_dict = {}
						json_il_dict["start_time"] = loopstart
						json_il_dict["end_time"] = loopend
						json_il_dict["start_time_str"] = time.asctime(time.localtime(loopstart))
						json_il_dict["end_time_str"] = time.asctime(time.localtime(loopend))
						json_il_dict["del_invalid"] = del_invalid
						json_il_dict["ndel_invalid"] = ndel_invalid
						json_il_dict["del_unused"] = del_unused
						json_il_dict["ndel_unused"] = ndel_unused
						json_il_dict["del_dclean"] = del_dclean
						json_il_dict["ndel_dclean"] = ndel_dclean
						json_il_dict["del_ogoal"] = del_ogoal
						json_il_dict["ndel_ogoal"] = ndel_ogoal
						json_il_dict["rep_ugoal"] = rep_ugoal
						json_il_dict["nrep_ugoal"] = nrep_ugoal
						json_il_dict["rebalance"] = rebalance
						json_il_dict["locked_unused"] = locked_unused
						json_il_dict["locked_used"] = locked_used
						json_in_dict["chunk_loop"] = json_il_dict
					else:
						json_in_dict["chunk_loop"] = None
				elif ttymode:
					tab = Table("Chunk operations info",10,"r")
					tab.header(("loop time","",2),("deletions","",4),("replications","",2),("locked","",2))
					tab.header(("---","",10))
					tab.header("start","end","invalid","unused","disk clean","over goal","under goal","rebalance","unused","used")
					if loopstart>0:
						tab.append((time.asctime(time.localtime(loopstart)),"c"),(time.asctime(time.localtime(loopend)),"c"),"%u/%u" % (del_invalid,del_invalid+ndel_invalid),"%u/%u" % (del_unused,del_unused+ndel_unused),"%u/%u" % (del_dclean,del_dclean+ndel_dclean),"%u/%u" % (del_ogoal,del_ogoal+ndel_ogoal),"%u/%u" % (rep_ugoal,rep_ugoal+nrep_ugoal),rebalance,locked_unused,locked_used)
					else:
						tab.append(("no data","c",10))
					print(myunicode(tab))
				else:
					out = []
					if loopstart>0:
						out.append("""chunk loop%sstart:%s%u""" % (plaintextseparator,plaintextseparator,loopstart))
						out.append("""chunk loop%send:%s%u""" % (plaintextseparator,plaintextseparator,loopend))
						out.append("""chunk loop%sdeletions%sinvalid:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_invalid,del_invalid+ndel_invalid))
						out.append("""chunk loop%sdeletions%sunused:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_unused,del_unused+ndel_unused))
						out.append("""chunk loop%sdeletions%sdisk clean:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_dclean,del_dclean+ndel_dclean))
						out.append("""chunk loop%sdeletions%sover goal:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_ogoal,del_ogoal+ndel_ogoal))
						out.append("""chunk loop%sreplications%sunder goal:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,rep_ugoal,rep_ugoal+nrep_ugoal))
						out.append("""chunk loop%sreplications%srebalance:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,rebalance))
						out.append("""chunk loop%slocked%sunused:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,locked_unused))
						out.append("""chunk loop%slocked%sused:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,locked_used))
					else:
						out.append("""chunk loop%sno data""" % plaintextseparator)
					print(str(Table("",0))+"\n".join(out))
			elif length==72:
				loopstart,loopend,del_invalid,ndel_invalid,del_unused,ndel_unused,del_dclean,ndel_dclean,del_ogoal,ndel_ogoal,rep_ugoal,nrep_ugoal,rep_wlab,nrep_wlab,rebalance,labels_dont_match,locked_unused,locked_used = struct.unpack(">LLLLLLLLLLLLLLLLLL",data)
				if cgimode:
					out = []
					out.append("""<div class="tab_title">Chunk operations info</div>""")
					out.append("""<table class="FR no-hover" cellspacing="0">""")
					out.append("""	<tr>""")
					out.append("""		<th colspan="2">Loop time</th>""")
					out.append("""		<th colspan="4">Deletions</th>""")
					out.append("""		<th colspan="3">Replications</th>""")
					out.append("""		<th colspan="2">Locked</th>""")
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					out.append("""		<th>start</th>""")
					out.append("""		<th>end</th>""")
					out.append("""		<th>invalid</th>""")
					out.append("""		<th>unused</th>""")
					out.append("""		<th>disk clean</th>""")
					out.append("""		<th>over goal</th>""")
					out.append("""		<th>under goal</th>""")
					out.append("""		<th>wrong labels</th>""")
					out.append("""		<th>rebalance</th>""")
					out.append("""		<th>unused</th>""")
					out.append("""		<th>used</th>""")
					out.append("""	</tr>""")
					if loopstart>0:
						out.append("""	<tr>""")
						out.append("""		<td align="center">%s</td>""" % (time.asctime(time.localtime(loopstart)),))
						out.append("""		<td align="center">%s</td>""" % (time.asctime(time.localtime(loopend)),))
						out.append("""		<td align="right">%u/%u</td>""" % (del_invalid,del_invalid+ndel_invalid))
						out.append("""		<td align="right">%u/%u</td>""" % (del_unused,del_unused+ndel_unused))
						out.append("""		<td align="right">%u/%u</td>""" % (del_dclean,del_dclean+ndel_dclean))
						out.append("""		<td align="right">%u/%u</td>""" % (del_ogoal,del_ogoal+ndel_ogoal))
						out.append("""		<td align="right">%u/%u</td>""" % (rep_ugoal,rep_ugoal+nrep_ugoal))
						out.append("""		<td align="right">%u/%u/%u</td>""" % (rep_wlab,labels_dont_match,rep_wlab+nrep_wlab+labels_dont_match))
						out.append("""		<td align="right">%u</td>""" % rebalance)
						out.append("""		<td align="right">%u</td>""" % locked_unused)
						out.append("""		<td align="right">%u</td>""" % locked_used)
						out.append("""	</tr>""")
					else:
						out.append("""	<tr>""")
						out.append("""		<td colspan="11" align="center">no data</td>""")
						out.append("""	</tr>""")
					out.append("""</table>""")
					print("\n".join(out))
				elif jsonmode:
					if loopstart>0:
						json_il_dict = {}
						json_il_dict["start_time"] = loopstart
						json_il_dict["end_time"] = loopend
						json_il_dict["start_time_str"] = time.asctime(time.localtime(loopstart))
						json_il_dict["end_time_str"] = time.asctime(time.localtime(loopend))
						json_il_dict["del_invalid"] = del_invalid
						json_il_dict["ndel_invalid"] = ndel_invalid
						json_il_dict["del_unused"] = del_unused
						json_il_dict["ndel_unused"] = ndel_unused
						json_il_dict["del_dclean"] = del_dclean
						json_il_dict["ndel_dclean"] = ndel_dclean
						json_il_dict["del_ogoal"] = del_ogoal
						json_il_dict["ndel_ogoal"] = ndel_ogoal
						json_il_dict["rep_ugoal"] = rep_ugoal
						json_il_dict["nrep_ugoal"] = nrep_ugoal
						json_il_dict["rep_wlab"] = rep_wlab
						json_il_dict["nrep_wlab"] = nrep_wlab
						json_il_dict["rebalance"] = rebalance
						json_il_dict["locked_unused"] = locked_unused
						json_il_dict["locked_used"] = locked_used
						json_in_dict["chunk_loop"] = json_il_dict
					else:
						json_in_dict["chunk_loop"] = None
				elif ttymode:
					tab = Table("Chunk operations info",11,"r")
					tab.header(("loop time","",2),("deletions","",4),("replications","",3),("locked","",2))
					tab.header(("---","",11))
					tab.header("start","end","invalid","unused","disk clean","over goal","under goal","wrong labels","rebalance","unused","used")
					if loopstart>0:
						tab.append((time.asctime(time.localtime(loopstart)),"c"),(time.asctime(time.localtime(loopend)),"c"),"%u/%u" % (del_invalid,del_invalid+ndel_invalid),"%u/%u" % (del_unused,del_unused+ndel_unused),"%u/%u" % (del_dclean,del_dclean+ndel_dclean),"%u/%u" % (del_ogoal,del_ogoal+ndel_ogoal),"%u/%u" % (rep_ugoal,rep_ugoal+nrep_ugoal),"%u/%u/%u" % (rep_wlab,labels_dont_match,rep_wlab+nrep_wlab+labels_dont_match),rebalance,locked_unused,locked_used)
					else:
						tab.append(("no data","c",11))
					print(myunicode(tab))
				else:
					out = []
					if loopstart>0:
						out.append("""chunk loop%sstart:%s%u""" % (plaintextseparator,plaintextseparator,loopstart))
						out.append("""chunk loop%send:%s%u""" % (plaintextseparator,plaintextseparator,loopend))
						out.append("""chunk loop%sdeletions%sinvalid:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_invalid,del_invalid+ndel_invalid))
						out.append("""chunk loop%sdeletions%sunused:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_unused,del_unused+ndel_unused))
						out.append("""chunk loop%sdeletions%sdisk clean:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_dclean,del_dclean+ndel_dclean))
						out.append("""chunk loop%sdeletions%sover goal:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,del_ogoal,del_ogoal+ndel_ogoal))
						out.append("""chunk loop%sreplications%sunder goal:%s%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,rep_ugoal,rep_ugoal+nrep_ugoal))
						out.append("""chunk loop%sreplications%swrong labels:%s%u/%u/%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,rep_wlab,labels_dont_match,rep_wlab+nrep_wlab+labels_dont_match))
						out.append("""chunk loop%sreplications%srebalance:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,rebalance))
						out.append("""chunk loop%slocked%sunused:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,locked_unused))
						out.append("""chunk loop%slocked%sused:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,locked_used))
					else:
						out.append("""chunk loop%sno data""" % plaintextseparator)
					print(str(Table("",0))+"\n".join(out))
			elif length==96:
				loopstart,loopend,fixed,forcekeep,delete_invalid,delete_no_longer_needed,delete_wrong_version,delete_duplicated_ecpart,delete_excess_ecpart,delete_excess_copy,delete_diskclean_ecpart,delete_diskclean_copy,replicate_dupserver_ecpart,replicate_needed_ecpart,replicate_needed_copy,replicate_wronglabels_ecpart,replicate_wronglabels_copy,split_copy_into_ecparts,join_ecparts_into_copy,recover_ecpart,calculate_ecchksum,locked_unused,locked_used,replicate_rebalance = struct.unpack(">"+24*"L",data)
				if cgimode:
					out = []
					if loopstart>0:
						out.append("""<div class="tab_title">Chunks housekeeping operations</div>""")
						out.append("""<table class="FR panel no-hover">""")
						out.append(""" <tr><td>""")
						tdata=[["Loop start", time_to_str(loopstart)], 
								["Loop end", time_to_str(loopend)], 
								["Locked unused chunks",decimal_number_html(locked_unused)],
								["Locked chunks", decimal_number_html(locked_used)],
								["Fixed chunks", decimal_number_html(fixed)],
								["Forced keep mode", decimal_number_html(forcekeep)]]
						out.append(html_table_vertical("Housekeeping loop statictics", tdata))
						out.append(""" </td><td>""")
						tdata=[["Invalid", decimal_number_html(delete_invalid)], 
								["Removed", decimal_number_html(delete_no_longer_needed)], 
								["Wrong version", decimal_number_html(delete_wrong_version)],
								["Excess", decimal_number_html(delete_excess_copy)],
								["Marked for removal", decimal_number_html(delete_diskclean_copy)]]
						out.append(html_table_vertical("Chunk copies - deletions", tdata))

						tdata=[["Needed", decimal_number_html(replicate_needed_copy)], 
								["Wrong labels", decimal_number_html(replicate_wronglabels_copy)]]
						out.append(html_table_vertical("Chunks copies - replications", tdata))	
						out.append(""" </td><td>""")
						tdata=[["Duplicated", decimal_number_html(delete_duplicated_ecpart)], 
								["Excess", decimal_number_html(delete_excess_ecpart)], 
								["Marked for removal", decimal_number_html(delete_diskclean_ecpart)]]
						out.append(html_table_vertical("EC parts - deletions", tdata))

						tdata=[["Duplicated server", decimal_number_html(replicate_dupserver_ecpart)], 
								["Needed", decimal_number_html(replicate_needed_ecpart)], 
								["Wrong labels", decimal_number_html(replicate_wronglabels_ecpart)],
								["Recovered", decimal_number_html(recover_ecpart)],
								["Calculated checksums", decimal_number_html(calculate_ecchksum)]]
						out.append(html_table_vertical("EC parts - replications", tdata))
						out.append(""" </td><td>""")
						tdata=[["Split: copies &rarr; EC parts", decimal_number_html(split_copy_into_ecparts)], 
								["Join: EC parts &rarr; copies", decimal_number_html(join_ecparts_into_copy)]]
						out.append(html_table_vertical("Copies &harr; EC parts", tdata))	

						tdata=[["Rebalance", decimal_number_html(replicate_rebalance)]]
						out.append(html_table_vertical("Replications", tdata))	
	
						out.append(""" </td></tr>""")
						out.append("""</table>""")						
					else:
						out.append("""<div class="tab_title">Chunks housekeeping operations</div>""")
						out.append("""<table class="FR" cellspacing="0">""")
						out.append("""	<tr>""")
						out.append("""		<td align="center">No data, self-check loop not finished yet</td>""")
						out.append("""	</tr>""")
						out.append("""</table>""")
					print("\n".join(out))

				elif jsonmode:
					if loopstart>0:
						json_il_dict = {}
						json_il_dict["start_time"] = loopstart
						json_il_dict["end_time"] = loopend
						json_il_dict["start_time_str"] = time.asctime(time.localtime(loopstart))
						json_il_dict["end_time_str"] = time.asctime(time.localtime(loopend))
						json_il_dict["fixed_chunks"] = fixed
						json_il_dict["forced_keep"] = forcekeep
						json_il_dict["locked_unused"] = locked_unused
						json_il_dict["locked_used"] = locked_used
						json_il_dict["del_invalid_copies"] = delete_invalid
						json_il_dict["del_no_longer_needed"] = delete_no_longer_needed
						json_il_dict["del_wrong_version"] = delete_wrong_version
						json_il_dict["del_duplicate_ecpart"] = delete_duplicated_ecpart
						json_il_dict["del_excess_ecpart"] = delete_excess_ecpart
						json_il_dict["del_excess_copy"] = delete_excess_copy
						json_il_dict["del_mfr_ecpart"] = delete_diskclean_ecpart
						json_il_dict["del_mfr_copy"] = delete_diskclean_copy
						json_il_dict["rep_dupserver_ecpart"] = replicate_dupserver_ecpart
						json_il_dict["rep_needed_ecpart"] = replicate_needed_ecpart
						json_il_dict["rep_needed_copy"] = replicate_needed_copy
						json_il_dict["rep_wronglabels_ecpart"] = replicate_wronglabels_ecpart
						json_il_dict["rep_wronglabels_copy"] = replicate_wronglabels_copy
						json_il_dict["rep_split_copy_into_ecparts"] = split_copy_into_ecparts
						json_il_dict["rep_join_ecparts_into_copy"] = join_ecparts_into_copy
						json_il_dict["rep_recover_ecpart"] = recover_ecpart
						json_il_dict["rep_calculate_ecchksum"] = calculate_ecchksum
						json_il_dict["rep_replicate_rebalance"] = replicate_rebalance
						json_in_dict["chunk_loop"] = json_il_dict
					else:
						json_in_dict["chunk_loop"] = None
				elif ttymode:
					if loopstart>0:
						tab = Table("Chunks housekeeping operations",2)
						tab.defattr("l","r")
						tab.append("Loop start",time.asctime(time.localtime(loopstart)))
						tab.append("Loop end",time.asctime(time.localtime(loopend)))
						tab.append("Locked unused chunks",locked_unused)
						tab.append("Locked chunks",locked_used)
						tab.append("Fixed chunks",fixed)
						tab.append("Forced keep mode",forcekeep)
						tab.append(("---","",2))
						tab.append(("Chunk copies - deletions","c",2))
						tab.append(("---","",2))
						tab.append("Invalid",delete_invalid)
						tab.append("Removed",delete_no_longer_needed)
						tab.append("Wrong version",delete_wrong_version)
						tab.append("Excess",delete_excess_copy)
						tab.append("Marked for removal",delete_diskclean_copy)
						tab.append(("---","",2))
						tab.append(("Chunk copies - replications","c",2))
						tab.append(("---","",2))
						tab.append("Needed",replicate_needed_copy)
						tab.append("Wrong labels",replicate_wronglabels_copy)
						tab.append(("---","",2))
						tab.append(("EC parts - deletions","c",2))
						tab.append(("---","",2))
						tab.append("Duplicated",delete_duplicated_ecpart)
						tab.append("Excess",delete_excess_ecpart)
						tab.append("Marked for removal",delete_diskclean_ecpart)
						tab.append(("---","",2))
						tab.append(("EC parts - replications","c",2))
						tab.append(("---","",2))
						tab.append("Duplicated server",replicate_dupserver_ecpart)
						tab.append("Needed",replicate_needed_ecpart)
						tab.append("Wrong labels",replicate_wronglabels_ecpart)
						tab.append("Recovered",recover_ecpart)
						tab.append("Calculated checksums",calculate_ecchksum)
						tab.append(("---","",2))
						tab.append(("Copies <-> EC parts","c",2))
						tab.append(("---","",2))
						tab.append("Split: copies -> EC parts",split_copy_into_ecparts)
						tab.append("Join: EC parts -> copies",join_ecparts_into_copy)
						tab.append(("---","",2))
						tab.append(("Replications","c",2))
						tab.append(("---","",2))
						tab.append("rebalance",replicate_rebalance)

#						tab = Table("Chunk operations: general info",6,"r")
#						tab.header("loop start","loop end","locked unused chunks","locked chunks","fixed chunks","forced keep mode")
#						tab.append((time.asctime(time.localtime(loopstart)),"c"),(time.asctime(time.localtime(loopend)),"c"),locked_unused,locked_used,fixed,forcekeep)
#						print(myunicode(tab))
#						tab = Table("Chunk operations: deletions",8,"r")
#						tab.header("invalid copies","removed chunk copies","wrong version copies","duplicated EC parts","excess EC parts","excess copies","MFR EC parts","MFR copies")
#						tab.append(delete_invalid,delete_no_longer_needed,delete_wrong_version,delete_duplicated_ecpart,delete_excess_ecpart,delete_excess_copy,delete_diskclean_ecpart,delete_diskclean_copy)
#						print(myunicode(tab))
#						tab = Table("Chunk operations: replications",10,"r")
#						tab.header("dup server EC parts","needed EC parts","needed copies","wrong labels EC parts","wrong labels copies","recovered EC parts","calculated EC checksums","split copies into EC parts","joined EC parts into copies","rebalance")
#						tab.append(replicate_dupserver_ecpart,replicate_needed_ecpart,replicate_needed_copy,replicate_wronglabels_ecpart,replicate_wronglabels_copy,recover_ecpart,calculate_ecchksum,split_copy_into_ecparts,join_ecparts_into_copy,replicate_rebalance)
						print(myunicode(tab))
					else:
						tab = Table("Chunks housekeeping operations",1,"c")
						tab.append("no data")
						print(myunicode(tab))
				else:
					out = []
					if loopstart>0:
						out.append("""chunk loop%sgeneral info%sloop start:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,loopstart))
						out.append("""chunk loop%sgeneral info%sloop end:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,loopend))
						out.append("""chunk loop%sgeneral info%slocked unused chunks:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,locked_unused))
						out.append("""chunk loop%sgeneral info%slocked chunks:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,locked_used))
						out.append("""chunk loop%sgeneral info%sfixed chunks:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,fixed))
						out.append("""chunk loop%sgeneral info%sforced keep mode:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,forcekeep))
						out.append("""chunk loop%sdeletions%sinvalid copies:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,delete_invalid))
						out.append("""chunk loop%sdeletions%sremoved chunk copies:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,delete_no_longer_needed))
						out.append("""chunk loop%sdeletions%swrong version copies:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,delete_wrong_version))
						out.append("""chunk loop%sdeletions%sduplicated EC parts:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,delete_duplicated_ecpart))
						out.append("""chunk loop%sdeletions%sexcess EC parts:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,delete_excess_ecpart))
						out.append("""chunk loop%sdeletions%sexcess copies:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,delete_excess_copy))
						out.append("""chunk loop%sdeletions%sMFR EC parts:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,delete_diskclean_ecpart))
						out.append("""chunk loop%sdeletions%sMFR copies:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,delete_diskclean_copy))
						out.append("""chunk loop%sreplications%sdup server EC parts:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,replicate_dupserver_ecpart))
						out.append("""chunk loop%sreplications%sneeded EC parts:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,replicate_needed_ecpart))
						out.append("""chunk loop%sreplications%sneeded copies:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,replicate_needed_copy))
						out.append("""chunk loop%sreplications%swrong labels EC parts:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,replicate_wronglabels_ecpart))
						out.append("""chunk loop%sreplications%swrong labels copies:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,replicate_wronglabels_copy))
						out.append("""chunk loop%sreplications%srecovered EC parts:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,recover_ecpart))
						out.append("""chunk loop%sreplications%scalculated EC checksums:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,calculate_ecchksum))
						out.append("""chunk loop%sreplications%ssplit copies into EC parts:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,split_copy_into_ecparts))
						out.append("""chunk loop%sreplications%sjoined EC parts into copies:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,join_ecparts_into_copy))
						out.append("""chunk loop%sreplications%srebalance:%s%u""" % (plaintextseparator,plaintextseparator,plaintextseparator,replicate_rebalance))
					else:
						out.append("""chunk loop%sno data""" % plaintextseparator)
					print(str(Table("",0))+"\n".join(out))
		except Exception:
			print_exception()
		
	if "MF" in sectionsubset and leaderfound and ((masterconn.version_at_least(2,0,66) and masterconn.version_less_than(3,0,0)) or masterconn.version_at_least(3,0,19)):
		try:
			inodes = set()
			missingchunks = []
			if ((masterconn.version_at_least(2,0,71) and masterconn.version_less_than(3,0,0)) or masterconn.version_at_least(3,0,25)):
				data,length = masterconn.command(CLTOMA_MISSING_CHUNKS,MATOCL_MISSING_CHUNKS,struct.pack(">B",1))
				if length%17==0:
					n = length//17
					for x in xrange(n):
						chunkid,inode,indx,mtype = struct.unpack(">QLLB",data[x*17:x*17+17])
						inodes.add(inode)
						missingchunks.append((chunkid,inode,indx,mtype))
				mode = 1
			else:
				data,length = masterconn.command(CLTOMA_MISSING_CHUNKS,MATOCL_MISSING_CHUNKS)
				if length%16==0:
					n = length//16
					for x in xrange(n):
						chunkid,inode,indx = struct.unpack(">QLL",data[x*16:x*16+16])
						inodes.add(inode)
						missingchunks.append((chunkid,inode,indx,None))
				mode = 0
			inodepaths = resolve_inodes_paths(masterconn,inodes)
			mcdata = []
			mccnt = 0
			for chunkid,inode,indx,mtype in missingchunks:
				if inode in inodepaths:
					paths = inodepaths[inode]
					mccnt += len(paths)
				else:
					paths = []
					mccnt += 1
				sf = paths
				if MForder==1:
					sf = paths
				elif MForder==2:
					sf = inode
				elif MForder==3:
					sf = indx
				elif MForder==4:
					sf = chunkid
				elif MForder==5:
					sf = mtype
				mcdata.append((sf,paths,inode,indx,chunkid,mtype))
			mcdata.sort()
			if MFrev:
				mcdata.reverse()
			if cgimode:
				out = []
				if mccnt>0:
					out.append("""<div class="tab_title">Missing files</div>""")
					out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_missingfiles" cellspacing="0">""")
					if MFlimit>0 and mccnt>MFlimit:
						out.append("""	<tr><th colspan="%u">Missing files (gathered by the previous filesystem self-check loop) - %u/%u entries - <a href="%s" class="VISIBLELINK">show more</a> - <a href="%s" class="VISIBLELINK">show all</a></th></tr>""" % ((6 if mode==1 else 5),MFlimit,mccnt,createhtmllink({"MFlimit":"%u" % (MFlimit + 100)}),createhtmllink({"MFlimit":"0"})))
					else:
						out.append("""	<tr><th colspan="%u">Missing files (gathered by the previous filesystem self-check loop)</th></tr>""" % (6 if mode==1 else 5))
					out.append("""	<tr>""")
					out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
					out.append("""		<th rowspan="2">Paths</th>""")
					out.append("""		<th rowspan="2">Inode</th>""")
					out.append("""		<th rowspan="2">Index</th>""")
					out.append("""		<th rowspan="2">Chunk&nbsp;id</th>""")
					if mode==1:
						out.append("""		<th rowspan="2">Type&nbsp;of&nbsp;missing&nbsp;chunk</th>""")
					out.append("""	</tr>""")
			elif jsonmode:
				json_mf_array = []
			elif ttymode:
				if mode==1:
					tab = Table("Missing Files/Chunks (gathered by the previous filesystem self-check loop)",5)
					tab.header("path","inode","index","chunk id","type of missing chunk")
					tab.defattr("l","r","r","r","r")
				else:
					tab = Table("Missing Files/Chunks (gathered by the previous filesystem self-check loop)",4)
					tab.header("path","inode","index","chunk id")
					tab.defattr("l","r","r","r")
			else:
				tab = Table("missing files",(5 if mode==1 else 4))
			missingcount = 0
			for sf,paths,inode,indx,chunkid,mtype in mcdata:
				if mtype==0:
					mtypestr = "NO COPY"
				elif mtype==1:
					mtypestr = "INVALID COPIES"
				elif mtype==2:
					mtypestr = "WRONG VERSIONS"
				elif mtype==3:
					mtypestr = "PARTIAL EC"
				else:
					mtypestr = "OTHER"
				if cgimode:
					if mccnt>0:
						if len(paths)==0:
							if missingcount<MFlimit or MFlimit==0:
								out.append("""	<tr>""")
								out.append("""		<td align="right"></td>""")
								out.append("""		<td align="left"> * unknown path * (deleted file)</td>""")
								out.append("""		<td align="center">%u</td>""" % inode)
								out.append("""		<td align="center">%u</td>""" % indx)
								out.append("""		<td align="center" class="monospace">%016X</td>""" % chunkid)
								if mode==1:
									out.append("""		<td align="center">%s</td>""" % mtypestr)
								out.append("""	</tr>""")
							missingcount += 1
						else:
							for path in paths:
								if missingcount<MFlimit or MFlimit==0:
									out.append("""	<tr>""")
									out.append("""		<td align="right"></td>""")
									out.append("""		<td align="left">%s</td>""" % path)
									out.append("""		<td align="center">%u</td>""" % inode)
									out.append("""		<td align="center">%u</td>""" % indx)
									out.append("""		<td align="center" class="monospace">%016X</td>""" % chunkid)
									if mode==1:
										out.append("""		<td align="center">%s</td>""" % mtypestr)
									out.append("""	</tr>""")
								missingcount += 1
				elif jsonmode:
					json_mf_dict = {}
					if len(paths)==0:
						json_mf_dict["paths"] = " * unknown path * (deleted file)"
					else:
						json_mf_dict["paths"] = paths
					json_mf_dict["inode"] = inode
					json_mf_dict["index"] = indx
					json_mf_dict["chunkid"] = ("%016X" % chunkid)
					if mode==1:
						json_mf_dict["type"] = mtype
						json_mf_dict["type_txt"] = mtypestr
					else:
						json_mf_dict["type"] = None
						json_mf_dict["type_txt"] = ""
					json_mf_array.append(json_mf_dict)
				else:
					if len(paths)==0:
						dline = [" * unknown path * (deleted file)",inode,indx,"%016X" % chunkid]
						if mode==1:
							dline.append(mtypestr)
						tab.append(*dline)
					else:
						for path in paths:
							dline = [path,inode,indx,"%016X" % chunkid]
							if mode==1:
								dline.append(mtypestr)
							tab.append(*dline)
			if cgimode:
				if mccnt>0:
					out.append("""</table>""")
				print("\n".join(out))
			elif jsonmode:
				json_in_dict["missing"] = json_mf_array
			else:
				print(myunicode(tab))
		except Exception:
			print_exception()

	if "MU" in sectionsubset and masterconn!=None and masterconn.version_at_least(1,7,16):
		try:
			data,length = masterconn.command(CLTOMA_MEMORY_INFO,MATOCL_MEMORY_INFO)
			if length>=176 and length%16==0:
				memusage = struct.unpack(">QQQQQQQQQQQQQQQQQQQQQQ",data[:176])
				memlabels = ["Chunk hash","Chunks","CS lists","Edge hash","Edges","Node hash","Nodes","Deleted nodes","Chunk tabs","Symlinks","Quota"]
				abrlabels = ["c.h.","c.","c.l.","e.h.","e.","n.h.","n.","d.n.","c.t.","s.","q."]
				totalused = 0
				totalallocated = 0
				for i in xrange(len(memusage)>>1):
					totalused += memusage[1+i*2]
					totalallocated += memusage[i*2]
				if cgimode:
					out = []
					out.append("""<div class="tab_title">Memory usage details</div>""")
					out.append("""<table class="FR" cellspacing="0">""")
					out.append("""	<tr><th></th>""")
					for label in memlabels:
						out.append("""		<th style="min-width: 70px;">%s</th>""" % label)
					out.append("""	<th style="min-width: 70px;">total</th></tr>""")
					out.append("""	<tr><th align="center">Used</th>""")
					for i in xrange(len(memlabels)):
						out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(memusage[1+i*2]),humanize_number(memusage[1+i*2],"&nbsp;")))
					out.append("""	<td align="center"><a style="cursor:default" title="%s B">%s</a></td></tr>""" % (decimal_number(totalused),humanize_number(totalused,"&nbsp;")))
					out.append("""	<tr><th align="center">Allocated</th>""")
					for i in xrange(len(memlabels)):
						out.append("""		<td align="center"><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number(memusage[i*2]),humanize_number(memusage[i*2],"&nbsp;")))
					out.append("""	<td align="center"><a style="cursor:default" title="%s B">%s</a></td></tr>""" % (decimal_number(totalallocated),humanize_number(totalallocated,"&nbsp;")))
					out.append("""	<tr><th align="center">Utilization</th>""")
					for i in xrange(len(memlabels)):
						if memusage[i*2]:
							percent = "%.2f %%" % (100.0 * memusage[1+i*2] / memusage[i*2])
						else:
							percent = "-"
						out.append("""		<td align="center">%s</td>""" % percent)
					if totalallocated:
						percent = "%.2f %%" % (100.0 * totalused / totalallocated)
					else:
						percent = "-"
					out.append("""	<td align="center">%s</td></tr>""" % percent)
					if totalallocated>0:
						out.append("""	<tr><th rowspan="2" align="center">Distribution</th>""")
						for i in xrange(len(memlabels)):
							tpercent = "%.2f %%" % (100.0 * memusage[i*2] / totalallocated)
							out.append("""		<td align="center">%s</td>""" % tpercent)
						out.append("""	<td>-</td></tr>""")
						out.append("""  <tr>""")
						out.append("""		<td colspan="%d" class="NOPADDING">""" % (len(memlabels)+1))
						out.append("""			<table width="100%" cellspacing="0" style="border:0px;" id="bar"><tr>""")
						memdistribution = []
						other = 0.0
						for i,(label,abr) in enumerate(zip(memlabels,abrlabels)):
							tpercent = (100.0 * memusage[i*2] / totalallocated)
							if tpercent>5.0:
								memdistribution.append((tpercent,label,abr))
							else:
								other+=tpercent
						memdistribution.sort()
						memdistribution.reverse()
						if other>0:
							memdistribution.append((other,None,None))
						cl = "FIRST"
						labels = []
						tooltips = []
						for i,(percent,label,abr) in enumerate(memdistribution):
							if label:
								if percent>7.0:
									out.append("""				<td style="width:%.2f%%;" class="MEMDIST%d MEMDIST%s" align="center"><a style="cursor:default;" title="%s (%.2f %%)">%s</a></td>""" % (percent,i,cl,label,percent,label))
								elif percent>3.0:
									out.append("""				<td style="width:%.2f%%;" class="MEMDIST%d MEMDIST%s" align="center"><a style="cursor:default;" title="%s (%.2f %%)">%s</a></td>""" % (percent,i,cl,label,percent,abr)) 
								else:
									out.append("""				<td style="width:%.2f%%;" class="MEMDIST%d MEMDIST%s" align="center"><a style="cursor:default;" title="%s (%.2f %%)">%s</a></td>""" % (percent,i,cl,label,percent,"#")) 
								labels.append(label)
								tooltips.append("%s (%.2f %%)" % (label,percent))
							else:
								out.append("""				<td style="width:%.2f%%;" class="MEMDISTOTHER MEMDIST%s">others</td>""" % (percent,cl))
								labels.append("others")
								tooltips.append("other memory segments (%.2f %%)" % (percent))
							cl = "MID"
						out.append("""			</tr></table>""")
						out.append("""<script type="text/javascript">""")
						out.append("""<!--//--><![CDATA[//><!--""")
						out.append("""	var bar_labels = [%s];""" % ",".join(map(repr,labels)))
						out.append("""	var bar_tooltips = [%s];""" % ",".join(map(repr,tooltips)))
						out.append("""//--><!]]>""")
						out.append("""</script>""")
						out.append("""<script type="text/javascript">
<!--//--><![CDATA[//><!--
	function bar_refresh() {
		var b = document.getElementById("bar");
		var i,j,x;
		if (b) {
			var x = b.getElementsByTagName("td");
			for (i=0 ; i<x.length ; i++) {
				x[i].innerHTML = "";
			}
			for (i=0 ; i<x.length ; i++) {
				var width = x[i].clientWidth;
				var label = bar_labels[i];
				var tooltip = bar_tooltips[i];
				x[i].innerHTML = "<a title='" + tooltip + "'>" + label + "</a>";
				if (width<x[i].clientWidth) {
					x[i].innerHTML = "<a title='" + tooltip + "'>&#8230;</a>";
					if (width<x[i].clientWidth) {
						x[i].innerHTML = "<a title='" + tooltip + "'>&#8226;</a>";
						if (width<x[i].clientWidth) {
							x[i].innerHTML = "<a title='" + tooltip + "'>.</a>";
							if (width<x[i].clientWidth) {
								x[i].innerHTML = "";
							}
						}
					} else {
						for (j=1 ; j<bar_labels[i].length-1 ; j++) {
							x[i].innerHTML = "<a title='" + tooltip + "'>"+label.substring(0,j) + "&#8230;</a>";
							if (width<x[i].clientWidth) {
								break;
							}
						}
						x[i].innerHTML = "<a title='" + tooltip + "'>" + label.substring(0,j-1) + "&#8230;</a>";
					}
				}
			}
		}
	}

	function bar_add_event(obj,type,fn) {
		if (obj.addEventListener) {
			obj.addEventListener(type, fn, false);
		} else if (obj.attachEvent) {
			obj.attachEvent('on'+type, fn);
		}
	}

	//bar_add_event(window,"load",bar_refresh); - comment due to flickering on load (for short bars <10%)
	bar_add_event(window,"resize",bar_refresh);
//--><!]]>
</script>""")
						out.append("""		</td>""")
						out.append("""	</tr>""")
					out.append("""</table>""")
					print("\n".join(out))
				elif jsonmode:
					json_mu_dict = {}
					for i,label in enumerate(memlabels):
						json_label = label.replace(" ","_")
						json_mu_dict[json_label] = {"allocated": memusage[i*2], "used": memusage[1+i*2], "allocated_human": humanize_number(memusage[i*2]," "), "used_human": humanize_number(memusage[1+i*2]," ")}
					json_mu_dict["total"] = {"allocated": totalallocated, "used": totalused, "allocated_human": humanize_number(totalallocated," "), "used_human": humanize_number(totalused," ")}
					json_in_dict["memory"] = json_mu_dict
				else:
					if ttymode:
						tab = Table("Memory Usage Detailed Info",5)
						tab.defattr("l","r","r","r","r")
						tab.header("object name","memory used","memory allocated","utilization percent","percent of total allocated memory")
					else:
						tab = Table("memory usage detailed info",3)
						tab.defattr("l","r","r")
						tab.header("object name","memory used","memory allocated")
					for i,label in enumerate(memlabels):
						if ttymode:
							if memusage[i*2]>0:
								upercent = "%.2f %%" % (100.0 * memusage[1+i*2] / memusage[i*2])
							else:
								upercent = "-"
							if totalallocated:
								tpercent = "%.2f %%" % (100.0 * memusage[i*2] / totalallocated)
							else:
								tpercent = "-"
							tab.append(label,humanize_number(memusage[1+i*2]," "),humanize_number(memusage[i*2]," "),upercent,tpercent)
						else:
							tab.append(label,memusage[1+i*2],memusage[i*2])
					if ttymode:
						tab.append(("---","",5))
						if totalallocated:
							totalpercent = "%.2f %%" % (100.0 * totalused / totalallocated)
						else:
							totalpercent = "-"
						tab.append("total",humanize_number(totalused," "),humanize_number(totalallocated," "),totalpercent,"-")
					print(myunicode(tab))
		except Exception:
			print_exception()

	if not cgimode and jsonmode:
		jcollect["dataset"]["info"] = json_in_dict
	
# Chunk servers section
if "CS" in sectionset and masterconn!=None:
	if "CS" in sectionsubset:
		try:
			servers = []
			dservers = []
			usedsum = 0
			totalsum = 0
			for cs in dataprovider.get_chunkservers():
				if cs.total>0:
					usedsum+=cs.used
					totalsum+=cs.total
				if CSorder==1:
					sf = cs.host
				elif CSorder==2 or CSorder==0:
					sf = cs.sortip
				elif CSorder==3:
					sf = cs.port
				elif CSorder==4:
					sf = cs.csid
				elif CSorder==5:
					sf = cs.sortver
				elif CSorder==6:
					sf = (cs.gracetime,cs.load)
				elif CSorder==7:
					sf = (cs.load)
				elif CSorder==10:
					sf = cs.chunks
				elif CSorder==11:
					sf = cs.used
				elif CSorder==12:
					sf = cs.total
				elif CSorder==13:
					if cs.total>0:
						sf = (1.0*cs.used)/cs.total
					else:
						sf = 0
				elif CSorder==20:
					sf = cs.tdchunks
				elif CSorder==21:
					sf = cs.tdused
				elif CSorder==22:
					sf = cs.tdtotal
				elif CSorder==23:
					if cs.tdtotal>0:
						sf = (1.0*cs.tdused)/cs.tdtotal
					else:
						sf = 0
				else:
					sf = 0
				if (cgimode and CScsid!="None" and CScsid!="" and CScsid!="%s:%s" % (cs.strip,cs.port)):
					continue #do not show cs not matching 'CScsid=host:port' URL param
				if (cs.flags&1)==0:
					servers.append((sf,cs.hostkey,cs.host,cs.sortip,cs.stroip,cs.strip,cs.port,cs.csid,cs.sortver,cs.strver,cs.flags,cs.used,cs.total,cs.chunks,cs.tdused,cs.tdtotal,cs.tdchunks,cs.errcnt,cs.load,cs.gracetime,cs.labels,cs.mfrstatus,cs.maintenanceto))
				else:
					dservers.append((sf,cs.hostkey,cs.host,cs.sortip,cs.stroip,cs.strip,cs.port,cs.csid,cs.flags,cs.maintenanceto))
			servers.sort()
			dservers.sort()
			if CSrev:
				servers.reverse()
				dservers.reverse()
			if totalsum>0:
				avgpercent = (usedsum*100.0)/totalsum
			else:
				avgpercent = 0

			out = []
			if len(servers)>0:
				if cgimode:
					out.append("""<div class="tab_title">Chunk servers</div>""")
					out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfscs" cellspacing="0">""")
					out.append("""	<tr>""")
					out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
					out.append("""		<th rowspan="2">Host</th>""")
					out.append("""		<th rowspan="2">IP</th>""")
					out.append("""		<th rowspan="2">Port</th>""")
					if masterconn.version_at_least(1,7,25):
						out.append("""		<th rowspan="2">Id</th>""")
					if masterconn.version_at_least(2,1,0):
						out.append("""		<th rowspan="2">Labels</th>""")
					out.append("""		<th rowspan="2">Version</th>""")
					if masterconn.version_at_least(1,6,28):
						out.append("""		<th rowspan="2">Queue</th>""")
					if masterconn.version_at_least(3,0,38):
						out.append("""		<th rowspan="2">Queue state</th>""")
					if masterconn.version_at_least(2,0,11):
						out.append("""		<th rowspan="2">Maintenance</th>""")
					out.append("""		<th colspan="4">'Regular' hdd space</th>""")
					if masterconn.version_at_least(3,0,38):
						out.append("""		<th colspan="5">'Marked for removal' hdd space</th>""")
					else:
						out.append("""		<th colspan="4">'Marked for removal' hdd space</th>""")
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					out.append("""		<th>chunks</th>""")
					out.append("""		<th>used</th>""")
					out.append("""		<th>total</th>""")
					out.append("""		<th class="PROGBAR">% used</th>""")
					if masterconn.version_at_least(3,0,38):
						out.append("""		<th>status</th>""")
					out.append("""		<th>chunks</th>""")
					out.append("""		<th>used</th>""")
					out.append("""		<th>total</th>""")
					out.append("""		<th class="PROGBAR">% used</th>""")
					out.append("""	</tr>""")
				elif jsonmode:
					json_cs_array = []
				elif ttymode:
					if masterconn.version_at_least(3,0,38):
						tab = Table("Chunk Servers",17,"r")
						tab.header("","","","","","","","",("'regular' hdd space","",4),("'marked for removal' hdd space","",5))
						tab.header("ip/host","port","id","labels","version","queue","queue state","maintenance",("---","",9))
						tab.header("","","","","","","","","chunks","used","total","% used","status","chunks","used","total","% used")
					elif masterconn.version_at_least(2,1,0):
						tab = Table("Chunk Servers",15,"r")
						tab.header("","","","","","","",("'regular' hdd space","",4),("'marked for removal' hdd space","",4))
						tab.header("ip/host","port","id","labels","version","load","maintenance",("---","",8))
						tab.header("","","","","","","","chunks","used","total","% used","chunks","used","total","% used")
					elif masterconn.version_at_least(2,0,11):
						tab = Table("Chunk Servers",14,"r")
						tab.header("","","","","","",("'regular' hdd space","",4),("'marked for removal' hdd space","",4))
						tab.header("ip/host","port","id","version","load","maintenance",("---","",8))
						tab.header("","","","","","","chunks","used","total","% used","chunks","used","total","% used")
					elif masterconn.version_at_least(1,7,25):
						tab = Table("Chunk Servers",13,"r")
						tab.header("","","","","",("'regular' hdd space","",4),("'marked for removal' hdd space","",4))
						tab.header("ip/host","port","id","version","load",("---","",8))
						tab.header("","","","","","chunks","used","total","% used","chunks","used","total","% used")
					elif masterconn.version_at_least(1,6,28):
						tab = Table("Chunk Servers",12,"r")
						tab.header("","","","",("'regular' hdd space","",4),("'marked for removal' hdd space","",4))
						tab.header("ip/host","port","version","load",("---","",8))
						tab.header("","","","","chunks","used","total","% used","chunks","used","total","% used")
					else:
						tab = Table("Chunk Servers",11,"r")
						tab.header("","","",("'regular' hdd space","",4),("'marked for removal' hdd space","",4))
						tab.header("ip/host","port","version",("---","",8))
						tab.header("","","","chunks","used","total","% used","chunks","used","total","% used")
				else:
					if masterconn.version_at_least(3,0,38):
						tab = Table("chunk servers",15)
					elif masterconn.version_at_least(2,1,0):
						tab = Table("chunk servers",13)
					elif masterconn.version_at_least(2,0,11):
						tab = Table("chunk servers",12)
					elif masterconn.version_at_least(1,7,25):
						tab = Table("chunk servers",11)
					elif masterconn.version_at_least(1,6,28):
						tab = Table("chunk servers",10)
					else:
						tab = Table("chunk servers",9)
				
				for sf,hostkey,host,sortip,stroip,strip,port,csid,sortver,strver,flags,used,total,chunks,tdused,tdtotal,tdchunks,errcnt,load,gracetime,labels,mfrstatus,maintenanceto in servers:
					if (flags&2)==0:
						mmto = "not applicable"
						mmtooltip = 0
					elif maintenanceto==None:
						mmto = "unknown"
						mmtooltip = 0
					elif maintenanceto==0xFFFFFFFF:
						mmto = "permanent"
						mmtooltip = 1
					else:
						mmto = "%u seconds left" % maintenanceto
						mmtooltip = 1
					(load_state, _, load_state_info, load_state_has_link) = gracetime2txt(gracetime)
					if strip!=stroip:
						if cgimode:
							strip = "%s &rightarrow; %s" % (stroip,strip)
						else:
							strip = "%s -> %s" % (stroip,strip)
					if cgimode:
						if masterconn.is_pro() and not strver.endswith(" PRO"):
							verclass = "BADVERSION"
						elif masterconn.sort_ver() > sortver:
							verclass = "LOWERVERSION"
						elif masterconn.sort_ver() < sortver:
							verclass = "HIGHERVERSION"
						else:
							verclass = "OKVERSION"
						if masterconn.version_at_least(2,0,11) and leaderfound:
							if (flags&2)==0:
								mmchecked = ""
								mmstr = "OFF"
								mmurl = createhtmllink({"CSmaintenanceon":("%s:%u" % (stroip,port))})
								mmicon = ''
								cl = None
							elif (flags&4)==0:
								mmchecked = "checked"
								mmstr = "ON"
								mmurl = createhtmllink({"CSmaintenanceoff":("%s:%u" % (stroip,port))})
								mmicon = html_icon('icon-wrench')
								cl = "MAINTAINREADY"
							else:
								mmchecked = "checked"
								mmstr = "TMP"
								mmurl = createhtmllink({"CSmaintenanceoff":("%s:%u" % (stroip,port))})
								mmicon = html_icon('icon-wrench')
								cl = "MAINTAINREADY"
						else:
							cl = None
						out.append("""	<tr>""")
						out.append("""		<td align="right"></td>""")
						if cl:
							out.append("""		<td align="left"><span class="%s text-icon">%s%s</span></td>""" % (cl, host, mmicon))
							out.append("""		<td align="center"><span class="sortkey">%s </span><span class="%s">%s</span></td>""" % (sortip,cl,strip))
							out.append("""		<td align="center"><span class="%s">%u</span></td>""" % (cl,port))
							if masterconn.version_at_least(1,7,25):
								out.append("""		<td align="center"><span class="%s">%u</span></td>""" % (cl,csid))
						else:
							out.append("""		<td align="left">%s</td>""" % (host))
							out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (sortip,strip))
							out.append("""		<td align="center">%u</td>""" % (port))
							if masterconn.version_at_least(1,7,25):
								out.append("""		<td align="center">%u</td>""" % (csid))
						if masterconn.version_at_least(2,1,0):
							if labels==0:
								labelstr = "-"
							else:
								labelstab = []
								for bit,char in enumerate(map(chr,range(ord('A'),ord('Z')+1))):
									if labels & (1<<bit):
										labelstab.append(char)
								labelstr = ",".join(labelstab)
							out.append("""		<td align="left">%s</td>""" % labelstr)
						out.append("""		<td align="center"><span class="sortkey">%s </span><span class="%s">%s</span></td>""" % (sortver,verclass,strver.replace("PRO","<small>PRO</small>")))
						if masterconn.version_at_least(1,6,28):
							if masterconn.version_at_least(3,0,38):
								out.append("""		<td align="right">%u</td>""" % (load))
								if load_state_has_link:
									out.append("""		<td align="center"><a style="cursor:default" title="%s" href="%s"><span class="GRACETIME">%s</span></a></td>""" % (load_state_info,createhtmllink({"CSbacktowork":("%s:%u" % (strip,port))}),load_state))
								else:
									out.append("""		<td align="center"><a style="cursor:default" title="%s"><span class="GRACETIME">%s</span></a></td>""" % (load_state_info,load_state))
							else:
								if gracetime>=0xC0000000:
									out.append("""		<td align="right"><a style="cursor:default" title="server queue heavy loaded"><span class="GRACETIME">&lt;%u&gt;</span></a></td>""" % (load))
								elif gracetime>=0x80000000:
									out.append("""		<td align="right"><a style="cursor:default" title="internal rebalance in progress"><span class="GRACETIME">(%u)</span></a></td>""" % (load))
								elif gracetime>=0x40000000:
									out.append("""		<td align="right"><a style="cursor:default" title="high speed rebalance in progress"><span class="GRACETIME">{%u}</span></a></td>""" % (load))
								elif gracetime>0:
									out.append("""		<td align="right"><a style="cursor:default" title="back after %u seconds" href="%s"><span class="GRACETIME">[%u]</span></a></td>""" % (gracetime,createhtmllink({"CSbacktowork":("%s:%u" % (strip,port))}),load))
								else:
									out.append("""		<td align="right">%u</td>""" % (load))
						if masterconn.version_at_least(2,0,11):
							if leaderfound:
								if readonly:
									out.append("""		<td class="text-center">%s</td>""" % mmstr)
								else:
									# out.append("""		<td class="text-center"><a class="VISIBLELINK" style="cursor:default" title="%s">%s</a>%s</td>""" % (mmto,mmstr,mmlink))
									out.append("""		<td class="text-center">""")
									out.append("""			<span><label class="switch width34" for="maintenance-checkbox-%s"><input type="checkbox" id="maintenance-checkbox-%s" onchange="window.location.href='%s'" %s/><span class="slider round checked_blue"><span class="slider-text">%s</span></span></label></span>""" % (hostkey, hostkey, mmurl, mmchecked,mmstr))
									out.append("""		</td>""")
									#TODO: add tooltip
							else:
								out.append("""		<td class="text-center">n/a</td>""")
						out.append("""		<td align="right">%s</td><td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td><td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number_html(chunks),used,decimal_number(used),humanize_number(used,"&nbsp;"),total,decimal_number(total),humanize_number(total,"&nbsp;")))
						if (total>0):
							usedpercent = (used*100.0)/total
							if usedpercent<avgpercent:
								diffstr = "&#8722;%.4f" % (avgpercent-usedpercent)
							else:
								diffstr = "+%.4f" % (usedpercent-avgpercent)
							out.append("""		<td align="center"><span class="sortkey">%.10f </span><div class="PROGBOX"><div class="PROGCOVER" style="width:%.2f%%;"></div><div class="PROGAVG" style="width:%.2f%%"></div><div class="PROGVALUE"><span><a style="cursor:default" title="%.2f%% = (avg%s%%)">%.1f</a></span></div></div></td>""" % (usedpercent,100.0-usedpercent,avgpercent,usedpercent,diffstr,usedpercent))
						else:
							out.append("""		<td align="center"><span class="sortkey">-1 </span><div class="PROGBOX"><div class="PROGCOVER" style="width:100%;"></div><div class="PROGVALUE"><span></span></div></div></td>""")
						if masterconn.version_at_least(3,0,38):
							if tdchunks==0 or leaderfound==0:
								out.append("""		<td align="center">-</td>""")
							elif mfrstatus==MFRSTATUS_INPROGRESS:
								out.append("""		<td align="center"><a style="cursor:default" title="disks can not be safely removed - please wait"><span class="MFRNOTREADY">not ready for removal (in progress)</span></a></td>""")
							elif mfrstatus==MFRSTATUS_READY:
								out.append("""		<td align="center"><a style="cursor:default" title="all disks marked for removal can be safely removed"><span class="MFRREADY">ready for removal</span></a></td>""")
							else: #MFRSTATUS_VALIDATING
								out.append("""		<td align="center"><a style="cursor:default" title="wait for chunk loop finish to stabilize state"><span class="MFRNOTREADY">not ready for removal (validating)</span></a></td>""")
						out.append("""		<td align="right">%s</td><td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td><td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td>""" % (decimal_number_html(tdchunks),tdused,decimal_number(tdused),humanize_number(tdused,"&nbsp;"),tdtotal,decimal_number(tdtotal),humanize_number(tdtotal,"&nbsp;")))
						if (tdtotal>0):
							usedpercent = (tdused*100.0)/tdtotal
							out.append("""		<td align="center"><span class="sortkey">%.10f </span><div class="PROGBOX"><div class="PROGCOVER" style="width:%.2f%%;"></div><div class="PROGVALUE"><span>%.1f</span></div></div></td>""" % (usedpercent,100.0-usedpercent,usedpercent))
						else:
							out.append("""		<td align="center"><span class="sortkey">-1 </span><div class="PROGBOX"><div class="PROGCOVER" style="width:100%;"></div><div class="PROGVALUE"><span></span></div></div></td>""")
						out.append("""	</tr>""")
					elif jsonmode:
						#Connected chunk servers
						json_cs_dict = {}
						json_cs_dict["connected"] = True
						json_cs_dict["strver"] = strver
						if strver.endswith(" PRO"):
							json_cs_dict["version"] = strver[:-4]
							json_cs_dict["pro"] = True
						else:
							json_cs_dict["version"] = strver
							json_cs_dict["pro"] = False
						json_cs_dict["flags"] = flags
						if masterconn.version_at_least(2,0,11):
							json_cs_dict["maintenance_mode_timeout"] = mmto
							if leaderfound==0:
								json_cs_dict["maintenance_mode"] = "not available"
							elif (flags&2)==0:
								json_cs_dict["maintenance_mode"] = "off"
							elif (flags&4)==0:
								json_cs_dict["maintenance_mode"] = "on"
							else:
								json_cs_dict["maintenance_mode"] = "on (temp)"
						else:
							json_cs_dict["maintenance_mode_timeout"] = None
							json_cs_dict["maintenance_mode"] = None
						json_cs_dict["hostname"] = host
						json_cs_dict["ip"] = strip
						json_cs_dict["port"] = port
						if masterconn.version_at_least(1,7,25):
							json_cs_dict["csid"] = csid
						else:
							json_cs_dict["csid"] = None
						json_cs_dict["errors"] = errcnt
						if masterconn.version_at_least(1,6,28):
							json_cs_dict["load"] = load
							if gracetime>=0xC0000000:
								json_cs_dict["load_state"] = "heavy load"
								json_cs_dict["load_cgi"] = "<%u>" % load
							elif gracetime>=0x80000000:
								json_cs_dict["load_state"] = "internal rebalance"
								json_cs_dict["load_cgi"] = "(%u)" % load
							elif gracetime>=0x40000000:
								json_cs_dict["load_state"] = "high speed rebalance"
								json_cs_dict["load_cgi"] = "{%u}" % load
							elif gracetime>0:
								json_cs_dict["load_state"] = "back to work after %u seconds" % gracetime
								json_cs_dict["load_cgi"] = "[%u]" % load
							else:
								json_cs_dict["load_state"] = "not overloaded"
								json_cs_dict["load_cgi"] = "%u" % load
						else:
							json_cs_dict["load"] = None
							json_cs_dict["load_state"] = "not available"
							json_cs_dict["load_cgi"] = "not available"
						if masterconn.version_at_least(2,1,0):
							if labels==0xFFFFFFFF or labels==0:
								labelstab = []
							else:
								labelstab = []
								for bit,char in enumerate(map(chr,range(ord('A'),ord('Z')+1))):
									if labels & (1<<bit):
										labelstab.append(char)
							json_cs_dict["labels"] = labelstab
							json_cs_dict["labels_str"] = ",".join(labelstab)
						else:
							json_cs_dict["labels"] = []
							json_cs_dict["labels_str"] = "not available"
						json_cs_dict["hdd_regular_used"] = used
						json_cs_dict["hdd_regular_used_human"] = humanize_number(used," ")
						json_cs_dict["hdd_regular_total"] = total
						json_cs_dict["hdd_regular_total_human"] = humanize_number(total," ")
						json_cs_dict["hdd_regular_free"] = total-used
						json_cs_dict["hdd_regular_free_human"] = humanize_number(total-used," ")
						if total>0:
							json_cs_dict["hdd_regular_used_percent"] = (used*100.0)/total
						else:
							json_cs_dict["hdd_regular_used_percent"] = 0.0
						json_cs_dict["hdd_regular_chunks"] = chunks
						json_cs_dict["hdd_removal_used"] = tdused
						json_cs_dict["hdd_removal_used_human"] = humanize_number(tdused," ")
						json_cs_dict["hdd_removal_total"] = tdtotal
						json_cs_dict["hdd_removal_total_human"] = humanize_number(tdtotal," ")
						json_cs_dict["hdd_removal_free"] = tdtotal-tdused
						json_cs_dict["hdd_removal_free_human"] = humanize_number(tdtotal-tdused," ")
						if tdtotal>0:
							json_cs_dict["hdd_removal_used_percent"] = (tdused*100.0)/tdtotal
						else:
							json_cs_dict["hdd_removal_used_percent"] = 0.0
						json_cs_dict["hdd_removal_chunks"] = tdchunks
						if masterconn.version_at_least(3,0,38):
							if tdchunks==0 or leaderfound==0:
								json_cs_dict["hdd_removal_stat"] = "-"
							elif mfrstatus==MFRSTATUS_INPROGRESS:
								json_cs_dict["hdd_removal_stat"] = "NOT READY (IN PROGRESS)"
							elif mfrstatus==MFRSTATUS_READY:
								json_cs_dict["hdd_removal_stat"] = "READY"
							else: #MFRSTATUS_VALIDATING
								json_cs_dict["hdd_removal_stat"] = "NOT READY (VALIDATING)"
						else:
							json_cs_dict["hdd_removal_stat"] = None
						json_cs_array.append(json_cs_dict)
					elif ttymode:
						if total>0:
							regperc = "%.2f%%" % ((used*100.0)/total)
						else:
							regperc = "-"
						if tdtotal>0:
							tdperc = "%.2f%%" % ((tdused*100.0)/tdtotal)
						else:
							tdperc = "-"
						data = [host,port]
						if masterconn.version_at_least(1,7,25):
							data.append(csid)
						if masterconn.version_at_least(2,1,0):
							if labels==0xFFFFFFFF or labels==0:
								labelstr = "-"
							else:
								labelstab = []
								for bit,char in enumerate(map(chr,range(ord('A'),ord('Z')+1))):
									if labels & (1<<bit):
										labelstab.append(char)
								labelstr = ",".join(labelstab)
							data.append(labelstr)
						data.append(strver)
						if masterconn.version_at_least(1,6,28):
							if masterconn.version_at_least(3,0,38):
								data.append(load)
								data.append(load_state)
							else:
								if gracetime>=0xC0000000:
									data.append("<%u>" % load)
								elif gracetime>=0x80000000:
									data.append("(%u)" % load)
								elif gracetime>=0x40000000:
									data.append("{%u}" % load)
								elif gracetime>0:
									data.append("[%u]" % load)
								else:
									data.append(load)
						if masterconn.version_at_least(2,0,11):
							if leaderfound==0:
								data.append("not available")
							elif (flags&2)==0:
								data.append("off")
							elif (flags&4)==0:
								data.append("on (%s)" % mmto)
							else:
								data.append("on (temp ; %s)" % mmto)
						data.extend([chunks,humanize_number(used," "),humanize_number(total," "),regperc])
						if masterconn.version_at_least(3,0,38):
							if tdchunks==0 or leaderfound==0:
								data.append("-")
							elif mfrstatus==MFRSTATUS_INPROGRESS:
								data.append(("NOT READY",'3'))
							elif mfrstatus==MFRSTATUS_READY:
								data.append(("READY",'4'))
							else:
								data.append("NOT READY")
						data.extend([tdchunks,humanize_number(tdused," "),humanize_number(tdtotal," "),tdperc])
						tab.append(*data)
					else:
						data = [host,port]
						if masterconn.version_at_least(1,7,25):
							data.append(csid)
						if masterconn.version_at_least(2,1,0):
							if labels==0xFFFFFFFF or labels==0:
								labelstr = "-"
								labelstab = []
							else:
								labelstab = []
								for bit,char in enumerate(map(chr,range(ord('A'),ord('Z')+1))):
									if labels & (1<<bit):
										labelstab.append(char)
								labelstr = ",".join(labelstab)
							data.append(labelstr)
						data.append(strver)
						if masterconn.version_at_least(1,6,28):
							if masterconn.version_at_least(3,0,38):
								data.append(load)
								data.append(load_state)
							else:
								if gracetime>=0xC0000000:
									data.append("<%u>" % load)
								elif gracetime>=0x80000000:
									data.append("(%u)" % load)
								elif gracetime>=0x40000000:
									data.append("{%u}" % load)
								elif gracetime>0:
									data.append("[%u]" % load)
								else:
									data.append(load)
						if masterconn.version_at_least(2,0,11):
							if leaderfound==0:
								data.append("-")
							elif (flags&2)==0:
								data.append("maintenance_off")
							elif (flags&4)==0:
								data.append("maintenance_on")
							else:
								data.append("maintenance_tmp_on")
						data.extend([chunks,used,total])
						if masterconn.version_at_least(3,0,38):
							if tdchunks==0 or leaderfound==0:
								data.append("-")
							elif mfrstatus==MFRSTATUS_INPROGRESS:
								data.append("NOT READY")
							elif mfrstatus==MFRSTATUS_READY:
								data.append("READY")
							else: #MFRSTATUS_VALIDATING
								data.append("NOT READY")
						data.extend([tdchunks,tdused,tdtotal])
						tab.append(*data)
				if cgimode:
					out.append("""</table>""")

			if len(dservers)>0:
				if cgimode:
					out.append("""<div class="tab_title">Disconnected chunk servers</div>""")
					out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsdiscs" cellspacing="0">""")
					out.append("""	<tr>""")
					out.append("""		<th class="acid_tab_enumerate">#</th>""")
					out.append("""		<th>Host</th>""")
					out.append("""		<th>IP</th>""")
					out.append("""		<th>Port</th>""")
					if masterconn.version_at_least(1,7,25):
						out.append("""		<th>Id</th>""")
					if masterconn.version_at_least(2,0,11):
						out.append("""		<th>Maintenance</th>""")
					if (not readonly):
						if leaderfound and deputyfound==0:
							out.append("""		<th class="acid_tab_skip">Remove</th>""")
						else:
							out.append("""		<th class="acid_tab_skip">Temporarily remove</th>""")
					out.append("""	</tr>""")
				elif jsonmode:
					pass
				elif ttymode:
					if masterconn.version_at_least(3,0,38):
						tab.append(("---","",17))
						tab.append(("disconnected servers","1c",17))
						tab.append(("---","",17))
						tab.append(("ip/host","c"),("port","c"),("id","r"),("maintenance","c",2),("change maintenance command","c",6),("remove command","c",6))
						tab.append(("---","",17))
					elif masterconn.version_at_least(2,1,0):
						tab.append(("---","",15))
						tab.append(("disconnected servers","1c",15))
						tab.append(("---","",15))
						tab.append(("ip/host","c"),("port","c"),("id","r"),("maintenance","c"),("change maintenance command","c",5),("remove command","c",6))
						tab.append(("---","",15))
					elif masterconn.version_at_least(2,0,11):
						tab.append(("---","",14))
						tab.append(("disconnected servers","1c",14))
						tab.append(("---","",14))
						tab.append(("ip/host","c"),("port","c"),("id","r"),("maintenance","c"),("change maintenance command","c",5),("remove command","c",5))
						tab.append(("---","",14))
					elif masterconn.version_at_least(1,7,25):
						tab.append(("---","",13))
						tab.append(("disconnected servers","1c",13))
						tab.append(("---","",13))
						tab.append(("ip/host","c"),("port","c"),("id","r"),("remove command","c",10))
						tab.append(("---","",13))
					elif masterconn.version_at_least(1,6,28):
						tab.append(("---","",12))
						tab.append(("disconnected servers","1c",12))
						tab.append(("---","",12))
						tab.append(("ip/host","c"),("port","c"),("remove command","c",10))
						tab.append(("---","",12))
					else:
						tab.append(("---","",11))
						tab.append(("disconnected servers","1c",11))
						tab.append(("---","",11))
						tab.append(("ip/host","c"),("port","c"),("remove command","c",9))
						tab.append(("---","",11))
				else:
					print(myunicode(tab))
					print("")
					if masterconn.version_at_least(2,0,11):
						tab = Table("Disconnected chunk servers",4)
					elif masterconn.version_at_least(1,7,25):
						tab = Table("Disconnected chunk servers",3)
					else:
						tab = Table("Disconnected chunk servers",2)
				
				for sf,hostkey,host,sortip,stroip,strip,port,csid,flags,maintenanceto in dservers:
					if (flags&2)==0:
						mmto = "not applicable"
						mmtooltip = 0
					elif maintenanceto==None:
						mmto = "unknown"
						mmtooltip = 0
					elif maintenanceto==0xFFFFFFFF:
						mmto = "permanent"
						mmtooltip = 1
					else:
						mmto = "%u seconds left" % maintenanceto
						mmtooltip = 1
					if strip!=stroip:
						if cgimode:
							strip = "%s &rightarrow; %s" % (stroip,strip)
						else:
							strip = "%s -> %s" % (stroip,strip)
					if cgimode:
						out.append("""	<tr>""")
						if masterconn.version_at_least(2,0,11):
							if leaderfound==0:
								mmicon = ''
								cl = "DISCONNECTED"
							elif (flags&2)==0:
								mmchecked = ""
								mmstr = "OFF"
								mmurl = createhtmllink({"CSmaintenanceon":("%s:%u" % (stroip,port))})
								mmicon = html_icon('icon-error')
								cl = "DISCONNECTED"
							elif (flags&4)==0:
								mmchecked = "checked"
								mmstr = "ON"
								mmurl = createhtmllink({"CSmaintenanceoff":("%s:%u" % (stroip,port))})
								mmicon = html_icon('icon-warning')+html_icon('icon-wrench')
								cl = "MAINTAINED"
							else:
								mmchecked = "checked"
								# mmstr = "TMP"
								if maintenanceto==0xFFFFFFFF:
									mmstr = "ON" #should not be here
								else:
									mmstr = """%u:%02u""" % (maintenanceto // 60, maintenanceto % 60)
								mmurl = createhtmllink({"CSmaintenanceoff":("%s:%u" % (stroip,port))})
								mmicon = html_icon('icon-warning')+html_icon('icon-wrench')+html_icon('icon-stopwatch', 1, 'icon-blue')
								cl = "TMPMAINTAINED"
							out.append("""		<td align="right"></td><td align="left"><span class="%s text-icon">%s%s</span></td>""" % (cl,host,mmicon))
							out.append("""		<td align="center"><span class="sortkey">%s </span><span class="%s">%s</span></td>""" % (sortip,cl,strip))
							out.append("""		<td align="center"><span class="%s">%u</span></td>""" % (cl,port))
							if masterconn.version_at_least(1,7,25):
								out.append("""		<td align="right"><span class="%s">%u</span></td>""" % (cl,csid))
							if leaderfound: #maintenance
								if readonly:
									out.append("""		<td class="text-center">%s</td>""" % mmstr)
								else:
									# out.append("""		<td class="text-center"><a class="VISIBLELINK" style="cursor:default" title="%s">%s</a>%s</td>""" % (mmto,mmstr,mmlink))
									out.append("""		<td class="text-center">""")
									out.append("""			<span><label class="switch width38" for="maintenance-checkbox-%s"><input type="checkbox" id="maintenance-checkbox-%s" onchange="window.location.href='%s'" %s/><span class="slider round checked_blue"><span class="slider-text countdown">%s</span></span></label></span>""" % (hostkey, hostkey, mmurl, mmchecked, mmstr))
									out.append("""		</td>""")
									#TODO: add tooltip
								# if readonly:
								# 	mmlink = ""
								# else:
								# 	mmlink = """: <a class="VISIBLELINK" href="%s">%s</a>""" % (mmurl,mm)
								# if mmtooltip:
								# 	out.append("""		<td align="center"><span class="%s"><a class="VISIBLELINK" style="cursor:default" title="%s">%s</a>%s</span></td>""" % (cl,mmto,mmstr,mmlink))
								# else:
								# 	out.append("""		<td align="center"><span class="%s">%s%s</span></td>""" % (cl,mmstr,mmlink))
							else:
								out.append("""		<td align="center"><span class="%s">not available</td>""" % cl)
							if (not readonly): #remove cmd
								if leaderfound and deputyfound==0:
									out.append("""		<td align="center"><a class="VISIBLELINK" href="%s">click to remove</a></td>""" % (createhtmllink({"CSremove":("%s:%u" % (stroip,port))})))
								elif masterconn.version_at_least(3,0,67):
									out.append("""		<td align="center"><a class="VISIBLELINK" href="%s">click to temporarily remove</a></td>""" % (createhtmllink({"CStmpremove":("%s:%u" % (stroip,port))})))
								else:
									out.append("""		<td align="center">not available</td>""")
						else:
							out.append("""		<td align="right"></td><td align="left"><span class="DISCONNECTED">%s</span></td>""" % (host))
							out.append("""		<td align="center"><span class="sortkey">%s </span><span class="DISCONNECTED">%s</span></td>""" % (sortip,strip))
							out.append("""		<td align="center"><span class="DISCONNECTED">%u</span></td>""" % (port))
							if masterconn.version_at_least(1,7,25):
								out.append("""		<td align="right"><span class="DISCONNECTED">%u</span></td>""" % (csid))
							if (not readonly): #remove cmd
								if leaderfound:
									out.append("""		<td align="center"><a class="VISIBLELINK" href="%s">click to remove</a></td>""" % (createhtmllink({"CSremove":("%s:%u" % (stroip,port))})))
								else:
									out.append("""		<td align="center">not available</td>""")
						out.append("""	</tr>""")
					elif jsonmode:
						#Disconnected chunk servers
						json_cs_dict = {}
						json_cs_dict["connected"] = False
						json_cs_dict["strver"] = None
						json_cs_dict["version"] = None
						json_cs_dict["pro"] = None
						json_cs_dict["flags"] = flags
						if masterconn.version_at_least(2,0,11):
							json_cs_dict["maintenance_mode_timeout"] = mmto
							if leaderfound==0:
								json_cs_dict["maintenance_mode"] = "not available"
							elif (flags&2)==0:
								json_cs_dict["maintenance_mode"] = "off"
							elif (flags&4)==0:
								json_cs_dict["maintenance_mode"] = "on"
							else:
								json_cs_dict["maintenance_mode"] = "on (temp)"
						else:
							json_cs_dict["maintenance_mode_timeout"] = None
							json_cs_dict["maintenance_mode"] = None
						json_cs_dict["hostname"] = host
						json_cs_dict["ip"] = strip
						json_cs_dict["port"] = port
						if masterconn.version_at_least(1,7,25):
							json_cs_dict["csid"] = csid
						else:
							json_cs_dict["csid"] = None
						json_cs_dict["errors"] = None
						json_cs_dict["load"] = None
						json_cs_dict["load_state"] = "not available"
						json_cs_dict["load_cgi"] = "not available"
						json_cs_dict["labels"] = []
						json_cs_dict["labels_str"] = "not available"
						json_cs_dict["hdd_regular_used"] = None
						json_cs_dict["hdd_regular_used_human"] = ""
						json_cs_dict["hdd_regular_total"] = None
						json_cs_dict["hdd_regular_total_human"] = ""
						json_cs_dict["hdd_regular_free"] = None
						json_cs_dict["hdd_regular_free_human"] = ""
						json_cs_dict["hdd_regular_used_percent"] = 0.0
						json_cs_dict["hdd_regular_chunks"] = None
						json_cs_dict["hdd_removal_used"] = None
						json_cs_dict["hdd_removal_used_human"] = ""
						json_cs_dict["hdd_removal_total"] = None
						json_cs_dict["hdd_removal_total_human"] = ""
						json_cs_dict["hdd_removal_free"] = None
						json_cs_dict["hdd_removal_free_human"] = ""
						json_cs_dict["hdd_removal_used_percent"] = 0.0
						json_cs_dict["hdd_removal_chunks"] = None
						json_cs_dict["hdd_removal_stat"] = None
						json_cs_array.append(json_cs_dict)
					elif ttymode:
						data = [host,port]
						if masterconn.version_at_least(1,7,25):
							data.append(csid)
						if masterconn.version_at_least(2,0,11):
							if leaderfound==0:
								mm = "-"
								mmcmd = "not available"
							elif (flags&2)==0:
								mm = "off"
								mmcmd = "%s -H %s -P %u -CM1/%s/%s" % (sys.argv[0],masterhost,masterport,stroip,port)
							elif (flags&4)==0:
								mm = "on (%s)" % mmto
								mmcmd = "%s -H %s -P %u -CM0/%s/%s" % (sys.argv[0],masterhost,masterport,stroip,port)
							else:
								mm = "on (temp ; %s)" % mmto
								mmcmd = "%s -H %s -P %u -CM0/%s/%s" % (sys.argv[0],masterhost,masterport,stroip,port)
							if masterconn.version_at_least(3,0,38):
								data.append((mm,"c",2))
							else:
								data.append(mm)
							if masterconn.version_at_least(3,0,38):
								data.append((mmcmd,"l",6))
							else:
								data.append((mmcmd,"l",5))
							if leaderfound and deputyfound==0:
								rmcmd = "%s -H %s -P %u -CRC/%s/%s" % (sys.argv[0],masterhost,masterport,stroip,port)
							elif masterconn.version_at_least(3,0,67):
								rmcmd = "%s -H %s -P %u -CTR/%s/%s" % (sys.argv[0],masterhost,masterport,stroip,port)
							else:
								rmcmd = "not available"
							if masterconn.version_at_least(2,1,0):
								data.append((rmcmd,"l",6))
							else:
								data.append((rmcmd,"l",5))
						else:
							if leaderfound:
								rmcmd = "%s -H %s -P %u -CRC/%s/%s" % (sys.argv[0],masterhost,masterport,stroip,port)
							else:
								rmcmd = "not available"
							if masterconn.version_at_least(1,6,28):
								data.append((rmcmd,"l",10))
							else:
								data.append((rmcmd,"l",9))
						tab.append(*data)
					else:
						if masterconn.version_at_least(2,0,11):
							if leaderfound==0:
								mm = "-"
							elif (flags&2)==0:
								mm = "maintenance off"
							elif (flags&4)==0:
								mm = "maintenance on (%s)" % mmto
							else:
								mm = "maintenance on (temp ; %s)" % mmto
							tab.append(host,port,csid,mm)
						elif masterconn.version_at_least(1,7,25):
							tab.append(host,port,csid)
						else:
							tab.append(host,port)
				if cgimode:
					out.append("""</table>""")
			if cgimode:
				print("\n".join(out))
			elif jsonmode:
				jcollect["dataset"]["chunkservers"] = json_cs_array
			else:
				print(myunicode(tab))
		except Exception:
			print_exception()

	#Metadata servers subsection
	if "MB" in sectionsubset and leaderfound:
		try:
			if cgimode:
				out = []
				out.append("""<div class="tab_title">Metadata backup loggers</div>""")
				out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsmbl" cellspacing="0">""")
				out.append("""	<tr>""")
				out.append("""		<th class="acid_tab_enumerate">#</th>""")
				out.append("""		<th>Host</th>""")
				out.append("""		<th>IP</th>""")
				out.append("""		<th>Version</th>""")
				out.append("""	</tr>""")
			elif jsonmode:
				json_mb_array = []
			elif ttymode:
				tab = Table("Metadata Backup Loggers",2,"r")
				tab.header("ip/host","version")
			else:
				tab = Table("metadata backup loggers",2)
			servers = dataprovider.get_metaloggers(MBorder, MBrev)
			for sf,host,sortip,strip,sortver,strver in servers:
				if cgimode:
					if masterconn.is_pro() and not strver.endswith(" PRO"):
						verclass = "BADVERSION"
					elif masterconn.sort_ver() > sortver:
						verclass = "LOWERVERSION"
					elif masterconn.sort_ver() < sortver:
						verclass = "HIGHERVERSION"
					else:
						verclass = "OKVERSION"
					out.append("""	<tr>""")
					out.append("""		<td align="right"></td><td align="left">%s</td><td align="center"><span class="sortkey">%s </span>%s</td><td align="center"><span class="sortkey">%s </span><span class="%s">%s</span></td>""" % (host,sortip,strip,sortver,verclass,strver.replace("PRO","<small>PRO</small>")))
					out.append("""	</tr>""")
				elif jsonmode:
					json_mb_dict = {}

					json_mb_dict["hostname"] = host
					json_mb_dict["ip"] = strip
					json_mb_dict["strver"] = strver
					if strver.endswith(" PRO"):
						json_mb_dict["version"] = strver[:-4]
						json_mb_dict["pro"] = True
					else:
						json_mb_dict["version"] = strver
						json_mb_dict["pro"] = False
					json_mb_array.append(json_mb_dict)
				else:
					tab.append(host,strver)
			if cgimode:
				out.append("""</table>""")
				print("\n".join(out))
			elif jsonmode:
				jcollect["dataset"]["metaloggers"] = json_mb_array
			else:
				print(myunicode(tab))
		except Exception:
			print_exception()

# if masterconn==None:
# 	exit(0)

# Disks section
if "HD" in sectionset and masterconn!=None:
	try:
		if not cgimode:
			HDdata = "ALL"
		# # get cs list
		# hostlist = []
		# for cs in dataprovider.get_chunkservers():
		# 	if (cs.flags&1)==0:
		# 		hostlist.append((cs.ip,cs.port,cs.version,cs.mfrstatus))
		# # get hdd lists one by one
		# hdds = []
		# scanhdds = []
		# for (ip1,ip2,ip3,ip4),port,version,mfrstatus in hostlist:
		# 	hostip = "%u.%u.%u.%u" % (ip1,ip2,ip3,ip4)
		# 	hostkey = "%s:%u" % (hostip,port)
		# 	if HDdata=="ALL" or HDdata=="ERR" or HDdata=="NOK" or HDdata==hostkey or not cgimode:
		# 		hoststr = resolve(hostip)
		# 		if port>0:
		# 			if version<=(1,6,8):
		# 				hdds.append(None,HDD(hostkey,"0","version too old","version too old",0,0,0,0,0,0,[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,0]))
		# 			else:
		# 				conn = MFSConn(hostip,port)
		# 				data,length = conn.command(CLTOCS_HDD_LIST,CSTOCL_HDD_LIST)
		# 				del conn
		# 				while length>0:
		# 					entrysize = struct.unpack(">H",data[:2])[0]
		# 					entry = data[2:2+entrysize]
		# 					data = data[2+entrysize:]
		# 					length -= 2+entrysize
		#					plen = entry[0]
		#					hddpathhex = entry[1:plen+1].hex()
		# 					hddpath = entry[1:plen+1]
		# 					hddpath = hddpath.decode('utf-8','replace')
		# 					hostpath = "%s:%u:%s" % (hoststr,port,hddpath)
		# 					ippath = "%s:%u:%s" % (hostip,port,hddpath)
		# 					clearerrorarg = "%s:%u:%s" % (hostip,port,hddpathhex)
		# 					sortippath = "%03u.%03u.%03u.%03u:%05u:%s" % (ip1,ip2,ip3,ip4,port,hddpath)
		# 					flags,errchunkid,errtime,used,total,chunkscnt = struct.unpack(">BQLQQL",entry[plen+1:plen+34])
		# 					rbytes = [0,0,0]
		# 					wbytes = [0,0,0]
		# 					usecreadsum = [0,0,0]
		# 					usecwritesum = [0,0,0]
		# 					usecfsyncsum = [0,0,0]
		# 					rops = [0,0,0]
		# 					wops = [0,0,0]
		# 					fsyncops = [0,0,0]
		# 					usecreadmax = [0,0,0]
		# 					usecwritemax = [0,0,0]
		# 					usecfsyncmax = [0,0,0]
		# 					if entrysize==plen+34+144:
		# 						rbytes[0],wbytes[0],usecreadsum[0],usecwritesum[0],rops[0],wops[0],usecreadmax[0],usecwritemax[0] = struct.unpack(">QQQQLLLL",entry[plen+34:plen+34+48])
		# 						rbytes[1],wbytes[1],usecreadsum[1],usecwritesum[1],rops[1],wops[1],usecreadmax[1],usecwritemax[1] = struct.unpack(">QQQQLLLL",entry[plen+34+48:plen+34+96])
		# 						rbytes[2],wbytes[2],usecreadsum[2],usecwritesum[2],rops[2],wops[2],usecreadmax[2],usecwritemax[2] = struct.unpack(">QQQQLLLL",entry[plen+34+96:plen+34+144])
		# 					elif entrysize==plen+34+192:
		# 						rbytes[0],wbytes[0],usecreadsum[0],usecwritesum[0],usecfsyncsum[0],rops[0],wops[0],fsyncops[0],usecreadmax[0],usecwritemax[0],usecfsyncmax[0] = struct.unpack(">QQQQQLLLLLL",entry[plen+34:plen+34+64])
		# 						rbytes[1],wbytes[1],usecreadsum[1],usecwritesum[1],usecfsyncsum[1],rops[1],wops[1],fsyncops[1],usecreadmax[1],usecwritemax[1],usecfsyncmax[1] = struct.unpack(">QQQQQLLLLLL",entry[plen+34+64:plen+34+128])
		# 						rbytes[2],wbytes[2],usecreadsum[2],usecwritesum[2],usecfsyncsum[2],rops[2],wops[2],fsyncops[2],usecreadmax[2],usecwritemax[2],usecfsyncmax[2] = struct.unpack(">QQQQQLLLLLL",entry[plen+34+128:plen+34+192])
		# 					rbw = [0,0,0]
		# 					wbw = [0,0,0]
		# 					usecreadavg = [0,0,0]
		# 					usecwriteavg = [0,0,0]
		# 					usecfsyncavg = [0,0,0]
		# 					for i in range(3):
		# 						if usecreadsum[i]>0:
		# 							rbw[i] = rbytes[i]*1000000//usecreadsum[i]
		# 						if usecwritesum[i]+usecfsyncsum[i]>0:
		# 							wbw[i] = wbytes[i]*1000000//(usecwritesum[i]+usecfsyncsum[i])
		# 						if rops[i]>0:
		# 							usecreadavg[i] = usecreadsum[i]//rops[i]
		# 						if wops[i]>0:
		# 							usecwriteavg[i] = usecwritesum[i]//wops[i]
		# 						if fsyncops[i]>0:
		# 							usecfsyncavg[i] = usecfsyncsum[i]//fsyncops[i]
		# 					sf = sortippath
		# 					if HDorder==1:
		# 						sf = sortippath
		# 					elif HDorder==2:
		# 						sf = chunkscnt
		# 					elif HDorder==3:
		# 						sf = errtime
		# 					elif HDorder==4:
		# 						sf = -flags
		# 					elif HDorder==5:
		# 						sf = rbw[HDperiod]
		# 					elif HDorder==6:
		# 						sf = wbw[HDperiod]
		# 					elif HDorder==7:
		# 						if HDtime==1:
		# 							sf = usecreadavg[HDperiod]
		# 						else:
		# 							sf = usecreadmax[HDperiod]
		# 					elif HDorder==8:
		# 						if HDtime==1:
		# 							sf = usecwriteavg[HDperiod]
		# 						else:
		# 							sf = usecwritemax[HDperiod]
		# 					elif HDorder==9:
		# 						if HDtime==1:
		# 							sf = usecfsyncavg[HDperiod]
		# 						else:
		# 							sf = usecfsyncmax[HDperiod]
		# 					elif HDorder==10:
		# 						sf = rops[HDperiod]
		# 					elif HDorder==11:
		# 						sf = wops[HDperiod]
		# 					elif HDorder==12:
		# 						sf = fsyncops[HDperiod]
		# 					elif HDorder==20:
		# 						if flags&CS_HDD_SCANNING==0:
		# 							sf = used
		# 						else:
		# 							sf = 0
		# 					elif HDorder==21:
		# 						if flags&CS_HDD_SCANNING==0:
		# 							sf = total
		# 						else:
		# 							sf = 0
		# 					elif HDorder==22:
		# 						if flags&CS_HDD_SCANNING==0 and total>0:
		# 							sf = (1.0*used)/total
		# 						else:
		# 							sf = 0
		# 					if (HDdata=="ERR" and errtime>0) or (HDdata=="NOK" and flags!=0) or (HDdata!="ERR" and HDdata!="NOK"):
		# 						hdd = HDD(hostkey,hoststr,hostip,port,hddpath,sortippath,ippath,hostpath,flags,clearerrorarg,errchunkid,errtime,used,total,chunkscnt,rbw,wbw,usecreadavg,usecwriteavg,usecfsyncavg,usecreadmax,usecwritemax,usecfsyncmax,rops,wops,fsyncops,rbytes,wbytes,mfrstatus)
		# 						if flags&CS_HDD_SCANNING and not cgimode and ttymode:
		# 							scanhdds.append((sf,hdd))
		# 						else:
		# 							hdds.append((sf,hdd))
		# hdds.sort()
		# scanhdds.sort()
		# if HDrev:
		# 	hdds.reverse()
		# 	scanhdds.reverse()
		servers=dataprovider.get_chunkservers(1)
		(hdds, scanhdds) = dataprovider.get_hdds(HDdata, HDorder, HDrev)

		if cgimode:
			out = []
			class_no_table = ""
			if len(hdds)==0 and len(scanhdds)==0:
				class_no_table = "no_table"
			if selectable:
				out.append("""<form action="#"><div class="tab_title %s">Disks, select: """ % class_no_table )
				out.append("""<div class="select-fl"><select name="server" size="1" onchange="document.location.href='%s&HDdata='+this.options[this.selectedIndex].value">""" % createrawlink({"HDdata":""}))
				entrystr = []
				entrydesc = {}
				entrystr.append("ALL")
				entrydesc["ALL"] = "All disks"
				entrystr.append("ERR")
				entrydesc["ERR"] = "Disks with errors only"
				entrystr.append("NOK")
				entrydesc["NOK"] = "Disks with status other than ok"
				for cs in servers:
					hostx = resolve(cs.strip)
					if hostx==UNRESOLVED:
						host = ""
					else:
						host = " / "+hostx
					entrystr.append(cs.hostkey)
					entrydesc[cs.hostkey] = "Server: %s%s" % (cs.hostkey,host)
				if HDdata not in entrystr:
					out.append("""<option value="" selected="selected">Pick option...</option>""")
				for estr in entrystr:
					if estr==HDdata:
						out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
					else:
						out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
				out.append("""</select><span class="arrow"></span></div>""")
				out.append("""</div></form>""")
			else:
				if len(hdds)>0 or len(scanhdds)>0:
					out.append("""<div class="tab_title %s">Disks</div>""")
			print("\n".join(out))


		if len(hdds)>0 or len(scanhdds)>0:
			if cgimode:
				out = []
				out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfshdd" cellspacing="0" id="mfshdd">""")
				out.append("""	<tr>""")
				out.append("""		<th colspan="5" rowspan="2" class="knob-cell">""")

				options=[(-50, "IP address", None, "acid_tab.switchdisplay('mfshdd','hddaddrname_vis',0);"),
						(-130,"Server name", None,"acid_tab.switchdisplay('mfshdd','hddaddrname_vis',1);")]
				out.append(html_knob_selector("hddaddrname_vis",12,(130,41),(110,20),options))

				options=[(-50,"Last minute", None, "acid_tab.switchdisplay('mfshdd','hddperiod_vis',0);"),
						(50,"Last hour", None, "javascript:acid_tab.switchdisplay('mfshdd','hddperiod_vis',1);"),
						(130,"Last day", None, "javascript:acid_tab.switchdisplay('mfshdd','hddperiod_vis',2);")]
				out.append(html_knob_selector("hddperiod_vis",12,(210,41),(110,20),options))

				options=[(50, "max time", None,"javascript:acid_tab.switchdisplay('mfshdd','hddtime_vis',0);"),
						(130,"avg time", None,"javascript:acid_tab.switchdisplay('mfshdd','hddtime_vis',1)")]
				out.append(html_knob_selector("hddtime_vis",12,(110,41),(20,20),options))

				out.append("""		</th>""")
				out.append("""		<th colspan="8">""")
				out.append("""			<span class="hddperiod_vis0">I/O stats - last minute</span>""")
				out.append("""			<span class="hddperiod_vis1">I/O stats - last hour</span>""")
				out.append("""			<span class="hddperiod_vis2">I/O stats - last day</span>""")
				out.append("""		</th>""")
				out.append("""		<th colspan="3" rowspan="2">Capacity</th>""")
				out.append("""	</tr>""")
				out.append("""	<tr>""")
				out.append("""		<th colspan="2"><a style="cursor:default" title="average data transfer speed">transfer</a></th>""")
				out.append("""		<th colspan="3">""")
				out.append("""			<span class="hddtime_vis0"><a style="cursor:default" title="max time of read or write one chunk block (up to 64kB)">max operation time</a></span>""")
				out.append("""			<span class="hddtime_vis1"><a style="cursor:default" title="average time of read or write chunk block (up to 64kB)">average operation time</a></span>""")
				out.append("""		</th>""")
				out.append("""		<th colspan="3"><a style="cursor:default" title="number of chunk block operations / chunk fsyncs">number of ops</a></th>""")
				out.append("""	</tr>""")
				out.append("""	<tr>""")
				out.append("""		<th class="acid_tab_enumerate">#</th>""")
				out.append("""		<th class="acid_tab_level_1"><span class="hddaddrname_vis0">IP</span><span class="hddaddrname_vis1">server name</span> and path</th>""")
				out.append("""		<th>chunks</th>""")
				out.append("""		<th>last error</th>""")
				out.append("""		<th>status</th>""")
				out.append("""		<th class="acid_tab_level_1">read</th>""")
				out.append("""		<th class="acid_tab_level_1">write</th>""")
				out.append("""		<th class="acid_tab_level_2">read</th>""")
				out.append("""		<th class="acid_tab_level_2">write</th>""")
				out.append("""		<th class="acid_tab_level_2">fsync</th>""")
				out.append("""		<th class="acid_tab_level_1">read</th>""")
				out.append("""		<th class="acid_tab_level_1">write</th>""")
				out.append("""		<th class="acid_tab_level_1">fsync</th>""")
				out.append("""		<th>used</th>""")
				out.append("""		<th>total</th>""")
				out.append("""		<th class="SMPROGBAR">% used</th>""")
				out.append("""	</tr>""")
			elif jsonmode:
				json_hd_array = []
			elif ttymode:
				tab = Table("Disks",15,"r")
				tab.header(("","",4),("I/O stats last %s" % ("day" if HDperiod==2 else "hour" if HDperiod==1 else "min"),"",8),("","",3))
				tab.header(("info","",4),("---","",8),("space","",3))
				tab.header(("","",4),("transfer","",2),("%s time" % ("avg" if HDtime==1 else "max"),"",3),("# of ops","",3),("","",3))
				tab.header(("---","",15))
				if len(hdds)>0 or len(scanhdds)==0:
					tab.header("IP path","chunks","last error","status","read","write","read","write","fsync","read","write","fsync","used","total","used %")
					lscanning = 0
				else:
					tab.header("IP path","chunks","last error","status","read","write","read","write","fsync","read","write","fsync",("progress","c",3))
					lscanning = 1
			else:
				tab = Table("disks",14)
			usedsum = {}
			totalsum = {}
			hostavg = {}
			for _,hdd in hdds+scanhdds:
				if hdd.hostkey not in usedsum:
					usedsum[hdd.hostkey]=0
					totalsum[hdd.hostkey]=0
					hostavg[hdd.hostkey]=0
				if hdd.flags&CS_HDD_SCANNING==0 and hdd.total and hdd.total>0:
					usedsum[hdd.hostkey]+=hdd.used
					totalsum[hdd.hostkey]+=hdd.total
					if totalsum[hdd.hostkey]>0:
						hostavg[hdd.hostkey] = (usedsum[hdd.hostkey] * 100.0) / totalsum[hdd.hostkey]
			for sf,hdd in hdds+scanhdds:
				# (hostkey,hoststr,hostip,port,hddpath,sortippath,ippath,hostpath,flags,clearerrorarg,errchunkid,errtime,used,total,chunkscnt,rbw,wbw,usecreadavg,usecwriteavg,usecfsyncavg,usecreadmax,usecwritemax,usecfsyncmax,rops,wops,fsyncops,rbytes,wbytes,mfrstatus) = disk
				statuslist = []
				clstatuslist = []
				valid = True
				if (hdd.flags&CS_HDD_TOO_OLD):
					statuslist.append('unknown, server too old')
					clstatuslist.append('<span class="ERROR">unknown, server too old %s</span>' % html_icon('icon-error'))
					valid = False
				if (hdd.flags&CS_HDD_UNREACHABLE):
					statuslist.append('server unreachable')
					clstatuslist.append('<span class="ERROR">unknown, server is unreachable %s</span>' % html_icon('icon-error'))
					valid = False
				if (hdd.flags&CS_HDD_INVALID):
					statuslist.append('invalid')
					clstatuslist.append('<span class="ERROR">invalid %s</span>' % html_icon('icon-error'))
				if (hdd.flags&CS_HDD_DAMAGED) and (hdd.flags&CS_HDD_SCANNING)==0 and (hdd.flags&CS_HDD_INVALID)==0:
					statuslist.append('damaged')
					clstatuslist.append('<span class="ERROR">damaged %s</span>' % html_icon('icon-error'))
				if hdd.flags&CS_HDD_MFR:
					if hdd.mfrstatus==MFRSTATUS_INPROGRESS:
						statuslist.append('MFR NOT READY')
						clstatuslist.append('<span class="MFRNOTREADY text-icon">not ready for removal (in progress)%s</span>' % html_icon('icon-warning'))
					elif hdd.mfrstatus==MFRSTATUS_READY:
						statuslist.append('MFR READY')
						clstatuslist.append('<span class="MFRREADY">ready for removal</span>')
					else: #MFRSTATUS_VALIDATING
						statuslist.append('MFR NOT READY')
						clstatuslist.append('<span class="MFRNOTREADY text-icon">not ready for removal (validating)%s</span>' % html_icon('icon-warning'))
				if hdd.flags&CS_HDD_SCANNING:
					statuslist.append('scanning')
					clstatuslist.append('<span class="SCANNING">scanning</scan>')
				if hdd.flags==0:
					statuslist.append('ok')
					clstatuslist.append('<span class="OK">ok</span>')
				status = ", ".join(statuslist)
				clstatus = ", ".join(clstatuslist)
				if not valid:
					lerror= '-'
				elif hdd.errtime==0 and hdd.errchunkid==0:
					lerror = 'no errors'
				else:
					if cgimode:
						errtimetuple = time.localtime(hdd.errtime)
						if readonly:
							lerror = '<a title="Error at %s on chunk: %016X">%s</a>' % (time.strftime("%Y-%m-%d %H:%M:%S",errtimetuple),hdd.errchunkid,time.strftime("%Y-%m-%d %H:%M",errtimetuple))
						else:
							lerror = '<a href="%s" title="Click, to dissmiss error at %s on chunk: %016X">%s</a>' % (createhtmllink({"CSclearerrors":hdd.clearerrorarg}),time.strftime("%Y-%m-%d %H:%M:%S",errtimetuple),hdd.errchunkid,time.strftime("%Y-%m-%d %H:%M",errtimetuple))
						lerror += '&nbsp;%s' % html_icon('icon-warning')
					elif jsonmode:
						lerror = time.asctime(time.localtime(hdd.errtime))
					elif ttymode:
						errtimetuple = time.localtime(hdd.errtime)
						lerror = time.strftime("%Y-%m-%d %H:%M",errtimetuple)
					else:
						lerror = hdd.errtime
				if cgimode:
					if valid:
						chunkscnttxt=decimal_number_html(hdd.chunkscnt)
						usedtxt = humanize_number(hdd.used,"&nbsp;")
						totaltxt = humanize_number(hdd.total,"&nbsp;")
					else:
						chunkscnttxt = '-'
						usedtxt = '-'
						totaltxt = '-'
					out.append("""	<tr>""")
					out.append("""		<td align="right"></td>""")
					out.append("""		<td align="left"><span class="hddaddrname_vis0"><span class="sortkey">%s </span>%s</span><span class="hddaddrname_vis1">%s</span></td>""" % (htmlentities(hdd.sortippath),htmlentities(hdd.ippath).replace(":/", ": /"),htmlentities(hdd.hostpath).replace(":/", ": /")))
					out.append("""		<td align="right">%s</td><td><span class="sortkey">%u </span>%s</td><td>%s</td>""" % (chunkscnttxt,hdd.errtime,lerror,clstatus))
					validdata = [1,1,1]
					for i in range(3):
						if hdd.rbw[i]==0 and hdd.wbw[i]==0 and hdd.usecreadmax[i]==0 and hdd.usecwritemax[i]==0 and hdd.usecfsyncmax[i]==0 and hdd.rops[i]==0 and hdd.wops[i]==0:
							validdata[i] = 0
					# rbw
					out.append("""		<td align="right">""")
					for i in range(3):
						out.append("""			<span class="hddperiod_vis%u">""" % i)
						if validdata[i]:
							out.append("""				<span class="sortkey">%u </span><a style="cursor:default" title="%s B/s">%s/s</a>""" % (hdd.rbw[i],decimal_number(hdd.rbw[i]),humanize_number(hdd.rbw[i],"&nbsp;")))
						else:
							out.append("""				<span class="sortkey">-1 </span>-""")
						out.append("""			</span>""")
					out.append("""		</td>""")
					# wbw
					out.append("""		<td align="right">""")
					for i in range(3):
						out.append("""			<span class="hddperiod_vis%u">""" % i)
						if validdata[i]:
							out.append("""				<span class="sortkey">%u </span><a style="cursor:default" title="%s B/s">%s/s</a>""" % (hdd.wbw[i],decimal_number(hdd.wbw[i]),humanize_number(hdd.wbw[i],"&nbsp;")))
						else:
							out.append("""				<span class="sortkey">-1 </span>-""")
						out.append("""			</span>""")
					out.append("""		</td>""")
					# readtime
					out.append("""		<td align="right">""")
					for i in range(3):
						out.append("""			<span class="hddperiod_vis%u">""" % i)
						if validdata[i]:
							out.append("""				<span class="hddtime_vis0">%s us</span>""" % decimal_number_html(hdd.usecreadmax[i]))
							out.append("""				<span class="hddtime_vis1">%s us</span>""" % decimal_number_html(hdd.usecreadavg[i]))
						else:
							out.append("""				<span><span class="sortkey">-1 </span>-</span>""")
						out.append("""			</span>""")
					out.append("""		</td>""")
					# writetime
					out.append("""		<td align="right">""")
					for i in range(3):
						out.append("""			<span class="hddperiod_vis%u">""" % i)
						if validdata[i]:
							out.append("""				<span class="hddtime_vis0">%s us</span>""" % decimal_number_html(hdd.usecwritemax[i]))
							out.append("""				<span class="hddtime_vis1">%s us</span>""" % decimal_number_html(hdd.usecwriteavg[i]))
						else:
							out.append("""				<span><span class="sortkey">-1 </span>-</span>""")
						out.append("""			</span>""")
					out.append("""		</td>""")
					# fsynctime
					out.append("""		<td align="right">""")
					for i in range(3):
						out.append("""			<span class="hddperiod_vis%u">""" % i)
						if validdata[i]:
							out.append("""				<span class="hddtime_vis0">%s us</span>""" % decimal_number_html(hdd.usecfsyncmax[i]))
							out.append("""				<span class="hddtime_vis1">%s us</span>""" % decimal_number_html(hdd.usecfsyncavg[i]))
						else:
							out.append("""				<span><span class="sortkey">-1 </span>-</span>""")
						out.append("""			</span>""")
					out.append("""		</td>""")
					# rops
					out.append("""		<td align="right">""")
					for i in range(3):
						out.append("""			<span class="hddperiod_vis%u">""" % i)
						if validdata[i]:
							if hdd.rops[i]>0:
								bsize = hdd.rbytes[i]/hdd.rops[i]
							else:
								bsize = 0
							out.append("""				<a style="cursor:default" title="average block size: %u B">%s</a>""" % (bsize,decimal_number_html(hdd.rops[i])))
						else:
							out.append("""				<span class="sortkey">-1 </span>-""")
						out.append("""			</span>""")
					out.append("""		</td>""")
					# wops
					out.append("""		<td align="right">""")
					for i in range(3):
						out.append("""			<span class="hddperiod_vis%u">""" % i)
						if validdata[i]:
							if hdd.wops[i]>0:
								bsize = hdd.wbytes[i]/hdd.wops[i]
							else:
								bsize = 0
							out.append("""				<a style="cursor:default" title="average block size: %u B">%s</a>""" % (bsize,decimal_number_html(hdd.wops[i])))
						else:
							out.append("""				<span class="sortkey">-1 </span>-""")
						out.append("""			</span>""")
					out.append("""		</td>""")
					# fsyncops
					out.append("""		<td align="right">""")
					for i in range(3):
						out.append("""			<span class="hddperiod_vis%u">""" % i)
						if validdata[i]:
							out.append("""				%s""" % decimal_number_html(hdd.fsyncops[i]))
						else:
							out.append("""				<span class="sortkey">-1 </span>-""")
						out.append("""			</span>""")
					out.append("""		</td>""")
#					if rbw==0 and wbw==0 and rtime==0 and wtime==0 and rops==0 and wops==0:
#						out.append("""		<td><span class="sortkey">-1 </span>-</td><td><span class="sortkey">-1 </span>-</td><td><span class="sortkey">-1 </span>-</td><td><span class="sortkey">-1 </span>-</td><td><span class="sortkey">-1 </span>-</td><td><span class="sortkey">-1 </span>-</td><td><span class="sortkey">-1 </span>-</td><td><span class="sortkey">-1 </span>-</td>""")
#					else:
#						if rops>0:
#							rbsize = rbytes/rops
#						else:
#							rbsize = 0
#						if wops>0:
#							wbsize = wbytes/wops
#						else:
#							wbsize = 0
#						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B/s">%s/s</a></td><td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s/s</a></td>""" % (rbw,decimal_number(rbw),humanize_number(rbw,"&nbsp;"),wbw,decimal_number(wbw),humanize_number(wbw,"&nbsp;")))
#						out.append("""		<td align="right">%u us</td><td align="right">%u us</td><td align="right">%u us</td><td align="right"><a style="cursor:default" title="average block size: %u B">%u</a></td><td align="right"><a style="cursor:default" title="average block size: %u B">%u</a></td><td align="right">%u</td>""" % (rtime,wtime,fsynctime,rbsize,rops,wbsize,wops,fsyncops))
					if hdd.flags&CS_HDD_SCANNING:
						out.append("""		<td colspan="3" align="right"><span class="sortkey">0 </span><div class="PROGBOX"><div class="PROGCOVER" style="width:%.0f%%;"></div><div class="PROGVALUE"><span>%.0f%% scanned</span></div></div></td>""" % (100.0-hdd.used,hdd.used))
					else:
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td><td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td>""" % (hdd.used,decimal_number(hdd.used),usedtxt,hdd.total,decimal_number(hdd.total),totaltxt))
						if hdd.total>0:
							usedpercent = (hdd.used*100.0)/hdd.total
							avgpercent = hostavg[hdd.hostkey]
							if usedpercent<avgpercent:
								diffstr = "&#8722;%.2f" % (avgpercent-usedpercent)
							else:
								diffstr = "+%.2f" % (usedpercent-avgpercent)
							out.append("""		<td align="center"><span class="sortkey">%.10f </span><div class="PROGBOX"><div class="PROGCOVER" style="width:%.2f%%;"></div><div class="PROGAVG" style="width:%.2f%%"></div><div class="PROGVALUE"><span><a style="cursor:default" title="%.2f%% = (avg%s%%)">%.1f</a></span></div></div></td>""" % (usedpercent,100.0-usedpercent,avgpercent,usedpercent,diffstr,usedpercent))
						else:
							out.append("""		<td align="center"><span class="sortkey">-1 </span><div class="PROGBOX"><div class="PROGCOVER" style="width:100%;"></div><div class="PROGVALUE"><span>-</span></div></div></td>""")
					out.append("""	</tr>""")
				elif jsonmode:
					json_hd_dict = {}
					json_hd_dict["hostname"] = hdd.hoststr
					json_hd_dict["ip"] = hdd.hostip
					json_hd_dict["port"] = hdd.port
					json_hd_dict["path"] = hdd.hddpath
					json_hd_dict["ip_path"] = hdd.ippath
					json_hd_dict["hostname_path"] = hdd.hostpath
					json_hd_dict["chunks"] = hdd.chunkscnt
					json_hd_dict["flags"] = hdd.flags
					json_hd_dict["mfrstatus"] = hdd.mfrstatus
					json_hd_dict["status"] = statuslist
					json_hd_dict["status_str"] = status
					json_hd_dict["last_error_time"] = hdd.errtime
					json_hd_dict["last_error_time_str"] = lerror
					json_hd_dict["last_error_chunkid"] = hdd.errchunkid
					if hdd.flags&CS_HDD_SCANNING:
						json_hd_dict["scan_progress"] = hdd.used
						json_hd_dict["used"] = None
						json_hd_dict["used_human"] = ""
						json_hd_dict["total"] = None
						json_hd_dict["total_human"] = ""
						json_hd_dict["used_percent"] = 0.0
					else:
						json_hd_dict["scan_progress"] = 100.0
						json_hd_dict["used"] = hdd.used
						json_hd_dict["used_human"] = humanize_number(hdd.used," ")
						json_hd_dict["total"] = hdd.total
						json_hd_dict["total_human"] = humanize_number(hdd.total," ")
						if hdd.total>0:
							json_hd_dict["used_percent"] = (hdd.used*100.0)/hdd.total
						else:
							json_hd_dict["used_percent"] = 0.0
					for i,name in enumerate(['min','hour','day']):
						json_hd_stats = {}
						json_hd_stats["rbw"] = hdd.rbw[i]
						json_hd_stats["wbw"] = hdd.wbw[i]
						json_hd_stats["read_avg_usec"] = hdd.usecreadavg[i]
						json_hd_stats["write_avg_usec"] = hdd.usecwriteavg[i]
						json_hd_stats["fsync_avg_usec"] = hdd.usecfsyncavg[i]
						json_hd_stats["read_max_usec"] = hdd.usecreadmax[i]
						json_hd_stats["write_max_usec"] = hdd.usecwritemax[i]
						json_hd_stats["fsync_max_usec"] = hdd.usecfsyncmax[i]
						json_hd_stats["read_ops"] = hdd.rops[i]
						json_hd_stats["write_ops"] = hdd.wops[i]
						json_hd_stats["fsync_ops"] = hdd.fsyncops[i]
						json_hd_stats["read_bytes"] = hdd.rbytes[i]
						json_hd_stats["write_bytes"] = hdd.wbytes[i]
						json_hd_dict["stats_%s" % name] = json_hd_stats
					json_hd_array.append(json_hd_dict)
				elif ttymode:
					rtime = hdd.usecreadmax[HDperiod] if HDtime==0 else hdd.usecreadavg[HDperiod]
					wtime = hdd.usecwritemax[HDperiod] if HDtime==0 else hdd.usecwriteavg[HDperiod]
					fsynctime = hdd.usecfsyncmax[HDperiod] if HDtime==0 else hdd.usecfsyncavg[HDperiod]
					if valid:
						chunkscnttxt = hdd.chunkscnt
						usedtxt = humanize_number(hdd.used," ")
						totaltxt = humanize_number(hdd.total," ")
					else:
						chunkscnttxt = '-'
						usedtxt = '-'
						totaltxt = '-'
					ldata = [hdd.ippath,chunkscnttxt,lerror,status]
					if hdd.rbw[HDperiod]==0 and hdd.wbw[HDperiod]==0 and hdd.usecreadmax[HDperiod]==0 and hdd.usecwritemax[HDperiod]==0 and hdd.usecfsyncmax[HDperiod]==0 and hdd.rops[HDperiod]==0 and hdd.wops[HDperiod]==0:
						ldata.extend(("-","-","-","-","-","-","-","-"))
					else:
						ldata.extend(("%s/s" % humanize_number(hdd.rbw[HDperiod]," "),"%s/s" % humanize_number(hdd.wbw[HDperiod]," "),"%u us" % rtime,"%u us" % wtime,"%u us" % fsynctime,hdd.rops[HDperiod],hdd.wops[HDperiod],hdd.fsyncops[HDperiod]))
					if hdd.flags&CS_HDD_SCANNING:
						if lscanning==0:
							lscanning=1
							tab.append(("---","",15))
							tab.append("IP path","chunks","last error","status","read","write","read","write","fsync","read","write","fsync",("progress","c",3))
							tab.append(("---","",15))
						ldata.append(("%.0f%%" % hdd.used,"r",3))
					else:
						if hdd.total>0:
							perc = "%.2f%%" % ((hdd.used*100.0)/hdd.total)
						else:
							perc = "-"
						ldata.extend((usedtxt,totaltxt,perc))
					tab.append(*ldata)
				else:
					if not valid:
						hdd.chunkscnt = '-'
						hdd.used = '-'
						hdd.total = '-'
					rtime = hdd.usecreadmax[HDperiod] if HDtime==0 else hdd.usecreadavg[HDperiod]
					wtime = hdd.usecwritemax[HDperiod] if HDtime==0 else hdd.usecwriteavg[HDperiod]
					fsynctime = hdd.usecfsyncmax[HDperiod] if HDtime==0 else hdd.usecfsyncavg[HDperiod]
					ldata = [hdd.ippath,hdd.chunkscnt,lerror,status]
					if hdd.rbw[HDperiod]==0 and hdd.wbw[HDperiod]==0 and hdd.usecreadmax[HDperiod]==0 and hdd.usecwritemax[HDperiod]==0 and hdd.usecfsyncmax[HDperiod]==0 and hdd.rops[HDperiod]==0 and hdd.wops[HDperiod]==0:
						ldata.extend(("-","-","-","-","-","-","-","-"))
					else:
						ldata.extend((hdd.rbw[HDperiod],hdd.wbw[HDperiod],rtime,wtime,fsynctime,hdd.rops[HDperiod],hdd.wops[HDperiod],hdd.fsyncops[HDperiod]))
					if hdd.flags&CS_HDD_SCANNING:
						ldata.extend(("progress:",hdd.used))
					else:
						ldata.extend((hdd.used,hdd.total))
					tab.append(*ldata)
			if cgimode:
				out.append("""</table>""")
				print("\n".join(out))
			elif jsonmode:
				jcollect["dataset"]["disks"] = json_hd_array
			else:
				print(myunicode(tab))
	except TimeoutError:
		print_error("Timeout connecting chunk servers. Hint: check if chunk servers are available from this server (the one hosting GUI/CLI).")
	except Exception:
		print_exception()

# Exports section
if "EX" in sectionset and masterconn!=None:
	try:
		if cgimode:
			out = []
			out.append("""<div class="tab_title">Exports</div>""")
			out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsexports" cellspacing="0">""")
			out.append("""	<tr>""")
			out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
			out.append("""		<th colspan="2">IP&nbsp;range</th>""")
			out.append("""		<th rowspan="2">Path</th>""")
			out.append("""		<th rowspan="2">Minversion</th>""")
			out.append("""		<th rowspan="2">Alldirs</th>""")
			out.append("""		<th rowspan="2">Password</th>""")
			out.append("""		<th rowspan="2">RO/RW</th>""")
			out.append("""		<th rowspan="2">Restricted&nbsp;IP</th>""")
			out.append("""		<th rowspan="2">Ignore&nbsp;gid</th>""")
			if masterconn.version_at_least(1,7,0):
				out.append("""		<th rowspan="2">Admin</th>""")
			out.append("""		<th colspan="2">Map&nbsp;root</th>""")
			out.append("""		<th colspan="2">Map&nbsp;users</th>""")
			if masterconn.version_at_least(1,6,26):
				out.append("""		<th rowspan="2">Allowed&nbsp;sclasses</th>""")
				out.append("""		<th colspan="2">Trashretention&nbsp;limit</th>""")
			if masterconn.has_feature(FEATURE_EXPORT_UMASK):
				out.append("""		<th rowspan="2">Global&nbsp;umask</th>""")
			if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				out.append("""		<th rowspan="2">Disables&nbsp;mask</th>""")
			out.append("""	</tr>""")
			out.append("""	<tr>""")
			out.append("""		<th style="min-width:80px;">from</th>""")
			out.append("""		<th style="min-width:80px;">to</th>""")
			out.append("""		<th style="min-width:40px;">uid</th>""")
			out.append("""		<th style="min-width:40px;">gid</th>""")
			out.append("""		<th style="min-width:40px;">uid</th>""")
			out.append("""		<th style="min-width:40px;">gid</th>""")
			if masterconn.version_at_least(1,6,26):
				out.append("""		<th style="min-width:40px;">min</th>""")
				out.append("""		<th style="min-width:40px;">max</th>""")
			out.append("""	</tr>""")
		elif jsonmode:
			json_ex_array = []
		elif ttymode:
			tab = Table("Exports",(19 if masterconn.has_feature(FEATURE_EXPORT_DISABLES) else 18 if masterconn.has_feature(FEATURE_EXPORT_UMASK) else 17 if masterconn.version_at_least(1,7,0) else 16 if masterconn.version_at_least(1,6,26) else 13))

			dline = ["r","r","l","c","c","c","c","c","c"]
			if masterconn.version_at_least(1,7,0):
				dline.append("c")
			dline.extend(("r","r","r","r"))
			if masterconn.version_at_least(1,6,26):
				dline.extend(("c","r","r"))
			if masterconn.has_feature(FEATURE_EXPORT_UMASK):
				dline.append("c")
			if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("c")
			tab.defattr(*dline)

			dline = [("ip range","",2),"","","","","","",""]
			if masterconn.version_at_least(1,7,0):
				dline.append("")
			dline.extend((("map root","",2),("map users","",2)))
			if masterconn.version_at_least(1,6,26):
				dline.extend(("",("trashretention limit","",2)))
			if masterconn.has_feature(FEATURE_EXPORT_UMASK):
				dline.append("")
			if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("")
			tab.header(*dline)

			dline = [("---","",2),"path","minversion","alldirs","password","ro/rw","restrict ip","ignore gid"]
			if masterconn.version_at_least(1,7,0):
				dline.append("admin")
			if masterconn.version_at_least(1,6,26):
				dline.append(("---","",4))
				dline.append("allowed sclasses")
				dline.append(("---","",2))
			else:
				dline.append(("---","",4))
			if masterconn.has_feature(FEATURE_EXPORT_UMASK):
				dline.append("global umask")
			if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("disables mask")
			tab.header(*dline)

			dline = ["from","to","","","","","","",""]
			if masterconn.version_at_least(1,7,0):
				dline.append("")
			dline.extend(("uid","gid","uid","gid"))
			if masterconn.version_at_least(1,6,26):
				dline.extend(("","min","max"))
			if masterconn.has_feature(FEATURE_EXPORT_UMASK):
				dline.append("")
			if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("")
			tab.header(*dline)

		else:
			tab = Table("exports",(19 if masterconn.has_feature(FEATURE_EXPORT_DISABLES) else 18 if masterconn.has_feature(FEATURE_EXPORT_UMASK) else 17 if masterconn.version_at_least(1,7,0) else 16 if masterconn.version_at_least(1,6,26) else 13))
		servers = []
		for expe in dataprovider.get_exports():
			sf = expe.ipfrom + expe.ipto
			if EXorder==1:
				sf = expe.sortipfrom
			elif EXorder==2:
				sf = expe.sortipto
			elif EXorder==3:
				sf = expe.path
			elif EXorder==4:
				sf = expe.sortver
			elif EXorder==5:
				if expe.meta:
					sf = None
				else:
					sf = expe.exportflags&1
			elif EXorder==6:
				sf = expe.exportflags&2
			elif EXorder==7:
				sf = expe.sesflags&1
			elif EXorder==8:
				sf = 2-(expe.sesflags&2)
			elif EXorder==9:
				if expe.meta:
					sf = None
				else:
					sf = expe.sesflags&4
			elif EXorder==10:
				if expe.meta:
					sf = None
				else:
					sf = expe.sesflags&8
			elif EXorder==11:
				if expe.meta:
					sf = None
				else:
					sf = expe.rootuid
			elif EXorder==12:
				if expe.meta:
					sf = None
				else:
					sf = expe.rootgid
			elif EXorder==13:
				if expe.meta or (expe.sesflags&16)==0:
					sf = None
				else:
					sf = expe.mapalluid
			elif EXorder==14:
				if expe.meta or (expe.sesflags&16)==0:
					sf = None
				else:
					sf = expe.mapalguid
			elif EXorder==15:
				sf = expe.sclassgroups
			elif EXorder==16:
				sf = expe.sclassgroups
			elif EXorder==17:
				sf = expe.mintrashretention
			elif EXorder==18:
				sf = expe.maxtrashretention
			elif EXorder==19:
				sf = expe.umaskval
			elif EXorder==20:
				sf = expe.disables
			servers.append((sf,expe.sortipfrom,expe.stripfrom,expe.sortipto,expe.stripto,expe.path,expe.meta,expe.sortver,expe.strver,expe.exportflags,expe.sesflags,expe.umaskval,expe.rootuid,expe.rootgid,expe.mapalluid,expe.mapallgid,expe.sclassgroups,expe.mintrashretention,expe.maxtrashretention,expe.disables))
		servers.sort()
		if EXrev:
			servers.reverse()
		for sf,sortipfrom,ipfrom,sortipto,ipto,path,meta,sortver,strver,exportflags,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables in servers:
			if sclassgroups==0:
				sclassgroups_sort = 0
				sclassgroups_str = 'NONE'
			elif sclassgroups==0xFFFF:
				sclassgroups_sort = 0xFFFF
				sclassgroups_str = "ALL"
			else:
				sclassgroups_sort = 0
				sclassgroups_list = []
				for b in range(16):
					sclassgroups_sort<<=1
					if sclassgroups & (1<<b):
						sclassgroups_list.append(b)
						sclassgroups_sort |= 1
				sclassgroups_str = ",".join(map(str,sclassgroups_list))
			if cgimode:
				out.append("""	<tr>""")
				out.append("""		<td align="right"></td>""")
				out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (sortipfrom,ipfrom))
				out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (sortipto,ipto))
				out.append("""		<td align="left">%s</td>""" % (".&nbsp;(META)" if meta else htmlentities(path)))
				out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (sortver,strver))
				out.append("""		<td align="center">%s</td>""" % ("-" if meta else "yes" if exportflags&1 else "no"))
				out.append("""		<td align="center">%s</td>""" % ("yes" if exportflags&2 else "no"))
				out.append("""		<td align="center">%s</td>""" % ("ro" if sesflags&1 else "rw"))
				out.append("""		<td align="center">%s</td>""" % ("no" if sesflags&2 else "yes"))
				out.append("""		<td align="center">%s</td>""" % ("-" if meta else "yes" if sesflags&4 else "no"))
				if masterconn.version_at_least(1,7,0):
					out.append("""		<td align="center">%s</td>""" % ("-" if meta else "yes" if sesflags&8 else "no"))
				if meta:
					out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
				else:
					out.append("""		<td align="right">%u</td>""" % rootuid)
					out.append("""		<td align="right">%u</td>""" % rootgid)
				if meta or (sesflags&16)==0:
					out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
				else:
					out.append("""		<td align="right">%u</td>""" % mapalluid)
					out.append("""		<td align="right">%u</td>""" % mapallgid)
				if masterconn.version_at_least(1,6,26):
					out.append("""		<td align="center"><span class="sortkey">%u</span>%s</td>""" % (sclassgroups_sort,sclassgroups_str))
					if mintrashretention!=None and maxtrashretention!=None:
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s</a></td>""" % (mintrashretention,timeduration_to_fullstr(mintrashretention),timeduration_to_shortstr(mintrashretention)))
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s</a></td>""" % (maxtrashretention,timeduration_to_fullstr(maxtrashretention),timeduration_to_shortstr(maxtrashretention)))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
				if masterconn.has_feature(FEATURE_EXPORT_UMASK):
					if umaskval==None:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					else:
						out.append("""		<td align="center">%03o</td>""" % umaskval)
				if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
					out.append("""		<td align="center"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%08X</a></td>""" % (disables,disablesmask_to_string(disables),disables))
				out.append("""	</tr>""")
			elif jsonmode:
				json_ex_dict = {}
				json_ex_dict["ip_range_from"] = ipfrom
				json_ex_dict["ip_range_to"] = ipto
				json_ex_dict["meta"] = meta
				json_ex_dict["path"] = "META" if meta else path
				json_ex_dict["export_flags"] = exportflags
				json_ex_dict["session_flags"] = sesflags
				json_ex_dict["minver"] = strver
				json_ex_dict["alldirs"] = None if meta else True if exportflags&1 else False
				json_ex_dict["password"] = True if exportflags&2 else False
				json_ex_dict["access_rw"] = False if sesflags&1 else True
				json_ex_dict["restricted"] = False if sesflags&2 else True
				json_ex_dict["ignore_gid"] = None if meta else True if sesflags&4 else False
				if masterconn.version_at_least(1,7,0):
					json_ex_dict["admin"] = None if meta else True if sesflags&8 else False
				else:
					json_ex_dict["admin"] = None
				if meta:
					json_ex_dict["map_root_uid"] = None
					json_ex_dict["map_root_gid"] = None
				else:
					json_ex_dict["map_root_uid"] = rootuid
					json_ex_dict["map_root_gid"] = rootgid
				if meta or (sesflags&16)==0:
					json_ex_dict["map_user_uid"] = None
					json_ex_dict["map_user_gid"] = None
				else:
					json_ex_dict["map_user_uid"] = mapalluid
					json_ex_dict["map_user_gid"] = mapallgid
				if masterconn.version_at_least(1,6,26):
					json_ex_dict["allowed_storage_classes"] = sclassgroups
					json_ex_dict["allowed_storage_classes_str"] = sclassgroups_str
					if mintrashretention!=None and maxtrashretention!=None:
						json_ex_dict["trash_retention_min"] = mintrashretention
						json_ex_dict["trash_retention_max"] = maxtrashretention
					else:
						json_ex_dict["trash_retention_min"] = None
						json_ex_dict["trash_retention_max"] = None
				else:
					json_ex_dict["allowed_storage_classes"] = None
					json_ex_dict["allowed_storage_classes_str"] = None
					json_ex_dict["trash_retention_min"] = None
					json_ex_dict["trash_retention_max"] = None
#				json_ex_dict["goal_limit_min"] = None
#				json_ex_dict["goal_limit_max"] = None
				if masterconn.has_feature(FEATURE_EXPORT_UMASK):
					if umaskval==None:
						json_ex_dict["global_umask"] = None
					else:
						json_ex_dict["global_umask"] = "%03o" % umaskval
				else:
					json_ex_dict["global_umask"] = None
				if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
					json_ex_dict["disables_mask"] = None if meta else "%08X" % disables
					json_ex_dict["disables_str"] = "" if meta else disablesmask_to_string(disables)
					json_ex_dict["disables"] = [] if meta else disablesmask_to_string_list(disables)
				else:
					json_ex_dict["disables_mask"] = None
					json_ex_dict["disables_str"] = ""
					json_ex_dict["disables"] = []
				json_ex_array.append(json_ex_dict)
			elif ttymode:
				dline = [ipfrom,ipto,". (META)" if meta else path,strver,"-" if meta else "yes" if exportflags&1 else "no","yes" if exportflags&2 else "no","ro" if sesflags&1 else "rw","no" if sesflags&2 else "yes","-" if meta else "yes" if sesflags&4 else "no"]
				if masterconn.version_at_least(1,7,0):
					dline.append("-" if meta else "yes" if sesflags&8 else "no")
				if meta:
					dline.extend(("-","-"))
				else:
					dline.extend((rootuid,rootgid))
				if meta or (sesflags&16)==0:
					dline.extend(("-","-"))
				else:
					dline.extend((mapalluid,mapallgid))
				if masterconn.version_at_least(1,6,26):
					dline.append("%04X (%s)" % (sclassgroups,sclassgroups_str))
					if mintrashretention!=None and maxtrashretention!=None:
						dline.extend((timeduration_to_shortstr(mintrashretention),timeduration_to_shortstr(maxtrashretention)))
					else:
						dline.extend(("-","-"))
				if masterconn.has_feature(FEATURE_EXPORT_UMASK):
					if umaskval==None:
						dline.append("-")
					else:
						dline.append("%03o" % umaskval)
				if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
					dline.append("%08X (%s)" % (disables,disablesmask_to_string(disables)))
				tab.append(*dline)
			else:
				dline = [ipfrom,ipto,". (META)" if meta else path,strver,"-" if meta else "yes" if exportflags&1 else "no","yes" if exportflags&2 else "no","ro" if sesflags&1 else "rw","no" if sesflags&2 else "yes","-" if meta else "yes" if sesflags&4 else "no"]
				if masterconn.version_at_least(1,7,0):
					dline.append("-" if meta else "yes" if sesflags&8 else "no")
				if meta:
					dline.extend(("-","-"))
				else:
					dline.extend((rootuid,rootgid))
				if meta or (sesflags&16)==0:
					dline.extend(("-","-"))
				else:
					dline.extend((mapalluid,mapallgid))
				if masterconn.version_at_least(1,6,26):
					dline.append("%04X" % sclassgroups)
					if mintrashretention!=None and maxtrashretention!=None:
						dline.extend((mintrashretention,maxtrashretention))
					else:
						dline.extend(("-","-"))
				if masterconn.has_feature(FEATURE_EXPORT_UMASK):
					if umaskval==None:
						dline.append("-")
					else:
						dline.append("%03o" % umaskval)
				if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
					dline.append("%08X" % disables)
				tab.append(*dline)
		if cgimode:
			out.append("""</table>""")
			print("\n".join(out))
		elif jsonmode:
			jcollect["dataset"]["exports"] = json_ex_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

if "MS" in sectionset and leaderfound:
	try:
		if cgimode:
			out = []
			out.append("""<div class="tab_title">Active mounts (parameters)</div>""")
			out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsmounts" cellspacing="0">""")
			out.append("""	<tr>""")
			out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
			out.append("""		<th rowspan="2" class="wrap">Session id</th>""")
			out.append("""		<th rowspan="2">Host</th>""")
			out.append("""		<th rowspan="2">IP</th>""")
			out.append("""		<th rowspan="2" class="wrap">Mount point</th>""")
			if masterconn.version_at_least(1,7,8):
				out.append("""		<th rowspan="2" class="wrap">Open files</th>""")
				out.append("""		<th rowspan="2" class="wrap">Number of connections</th>""")
			out.append("""		<th rowspan="2">Version</th>""")
			out.append("""		<th rowspan="2">Root dir</th>""")
			out.append("""		<th rowspan="2">RO/RW</th>""")
			out.append("""		<th rowspan="2" class="wrap">Restricted IP</th>""")
			out.append("""		<th rowspan="2" class="wrap">Ignore gid</th>""")
			if masterconn.version_at_least(1,7,0):
#				out.append("""		<th rowspan="2"><a href="%s">can&nbsp;change&nbsp;quota</a></th>""" % (createorderlink("MS",10)))
				out.append("""		<th rowspan="2">Admin</th>""")
			out.append("""		<th colspan="2">Map&nbsp;root</th>""")
			out.append("""		<th colspan="2">Map&nbsp;users</th>""")
			if masterconn.version_at_least(1,6,26):
				out.append("""		<th rowspan="2">Allowed&nbsp;sclasses</th>""")
				out.append("""		<th colspan="2">Trashretention&nbsp;limits</th>""")
			if masterconn.has_feature(FEATURE_EXPORT_UMASK):
				out.append("""		<th rowspan="2" class="wrap">Global umask</th>""")
			if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				out.append("""		<th rowspan="2" class="wrap">Disables mask</th>""")
			out.append("""	</tr>""")
			out.append("""	<tr>""")
			out.append("""		<th>uid</th>""")
			out.append("""		<th>gid</th>""")
			out.append("""		<th>uid</th>""")
			out.append("""		<th>gid</th>""")
			if masterconn.version_at_least(1,6,26):
				out.append("""		<th>min</th>""")
				out.append("""		<th>max</th>""")
			out.append("""	</tr>""")
		elif ttymode:
			tab = Table("Active mounts (parameters)",(20 if masterconn.has_feature(FEATURE_EXPORT_DISABLES) else 19 if masterconn.has_feature(FEATURE_EXPORT_UMASK) else 18 if masterconn.version_at_least(1,7,8) else 16 if masterconn.version_at_least(1,7,0) else 15 if masterconn.version_at_least(1,6,26) else 12))

			dline = ["r","r","l"]
			if masterconn.version_at_least(1,7,8):
				dline.extend(("r","r"))
			dline.extend(("r","l","c","c","c"))
			if masterconn.version_at_least(1,7,0):
				dline.append("c")
			dline.extend(("r","r","r","r"))
			if masterconn.version_at_least(1,6,26):
				dline.extend(("l","r","r"))
			if masterconn.has_feature(FEATURE_EXPORT_UMASK):
				dline.append("c")
			if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("c")
			tab.defattr(*dline)

			dline = ["","","","","","","",""]
			if masterconn.version_at_least(1,7,0):
				if masterconn.version_at_least(1,7,8):
					dline.extend(("",""))
				dline.append("")
			dline.extend((("map root","",2),("map users","",2)))
			if masterconn.version_at_least(1,6,26):
				dline.extend(("",("trashretention limit","",2)))
			if masterconn.has_feature(FEATURE_EXPORT_UMASK):
				dline.append("")
			if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("")
			tab.header(*dline)

			dline = ["session id","ip/host","mount point"]
			if masterconn.version_at_least(1,7,8):
				dline.extend(("open files","# of connections"))
			dline.extend(("version","root dir","ro/rw","restrict ip","ignore gid"))
			if masterconn.version_at_least(1,7,0):
				dline.append("admin")
			if masterconn.version_at_least(1,6,26):
				dline.append(("---","",4))
				dline.append("allowed sclasses")
				dline.append(("---","",2))
			else:
				dline.append(("---","",4))
			if masterconn.has_feature(FEATURE_EXPORT_UMASK):
				dline.append("global umask")
			if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("disables mask")
			tab.header(*dline)

			dline = ["","","","","","","",""]
			if masterconn.version_at_least(1,7,0):
				if masterconn.version_at_least(1,7,8):
					dline.extend(("",""))
				dline.append("")
			dline.extend(("uid","gid","uid","gid"))
			if masterconn.version_at_least(1,6,26):
				dline.extend(("","min","max"))
			if masterconn.has_feature(FEATURE_EXPORT_UMASK):
				dline.append("")
			if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
				dline.append("")
			tab.header(*dline)
		elif jsonmode:
			json_ms_array = []
		else:
			tab = Table("active mounts, parameters",(20 if masterconn.has_feature(FEATURE_EXPORT_DISABLES) else 19 if masterconn.has_feature(FEATURE_EXPORT_UMASK) else 18 if masterconn.version_at_least(1,7,8) else 16 if masterconn.version_at_least(1,7,0) else 15 if masterconn.version_at_least(1,6,26) else 12))

		servers = []
		dservers = []
		for ses in dataprovider.get_sessions():
			sf = ses.sortip
			if MSorder==1:
				sf = ses.sessionid
			elif MSorder==2:
				sf = ses.host
			elif MSorder==3:
				sf = ses.sortip
			elif MSorder==4:
				sf = ses.info
			elif MSorder==5:
				sf = ses.openfiles
			elif MSorder==6:
				if ses.nsocks>0:
					sf = ses.nsocks
				else:
					sf = ses.expire
			elif MSorder==7:
				sf = ses.sortver
			elif MSorder==8:
				sf = ses.path
			elif MSorder==9:
				sf = ses.sesflags&1
			elif MSorder==10:
				sf = 2-(ses.sesflags&2)
			elif MSorder==11:
				if ses.meta:
					sf = None
				else:
					sf = ses.sesflags&4
			elif MSorder==12:
				if ses.meta:
					sf = None
				else:
					sf = ses.sesflags&8
			elif MSorder==13:
				if ses.meta:
					sf = None
				else:
					sf = ses.rootuid
			elif MSorder==14:
				if ses.meta:
					sf = None
				else:
					sf = ses.rootgid
			elif MSorder==15:
				if ses.meta or (ses.sesflags&16)==0:
					sf = None
				else:
					sf = ses.mapalluid
			elif MSorder==16:
				if ses.meta or (ses.sesflags&16)==0:
					sf = None
				else:
					sf = ses.mapallgid
			elif MSorder==17:
				sf = ses.sclassgroups
			elif MSorder==18:
				sf = ses.sclassgroups
			elif MSorder==19:
				sf = ses.mintrashretention
			elif MSorder==20:
				sf = ses.maxtrashretention
			elif MSorder==21:
				sf = ses.umaskval
			elif MSorder==22:
				sf = ses.disables
			if ses.nsocks>0:
				servers.append((sf,ses.sessionid,ses.host,ses.sortip,ses.strip,ses.info,ses.openfiles,ses.nsocks,ses.sortver,ses.strver,ses.meta,ses.path,ses.sesflags,ses.umaskval,ses.rootuid,ses.rootgid,ses.mapalluid,ses.mapallgid,ses.sclassgroups,ses.mintrashretention,ses.maxtrashretention,ses.disables))
			else:
				dservers.append((sf,ses.sessionid,ses.host,ses.sortip,ses.strip,ses.info,ses.openfiles,ses.expire))
		servers.sort()
		dservers.sort()
		if MSrev:
			servers.reverse()
			dservers.reverse()
		for sf,sessionid,host,sortipnum,ipnum,info,openfiles,nsocks,sortver,strver,meta,path,sesflags,umaskval,rootuid,rootgid,mapalluid,mapallgid,sclassgroups,mintrashretention,maxtrashretention,disables in servers:
			if sessionid & 0x80000000:
				sessionstr = "TMP/%u" % (sessionid&0x7FFFFFFF)
				sessiontmp = True
			else:
				sessionstr = "%u" % (sessionid)
				sessiontmp = False
			if sclassgroups==0:
				sclassgroups_sort = 0
				sclassgroups_str = 'NONE'
			elif sclassgroups==0xFFFF:
				sclassgroups_sort = 0xFFFF
				sclassgroups_str = "ALL"
			else:
				sclassgroups_sort = 0
				sclassgroups_list = []
				for b in range(16):
					sclassgroups_sort<<=1
					if sclassgroups & (1<<b):
						sclassgroups_list.append(b)
						sclassgroups_sort |= 1
				sclassgroups_str = ",".join(map(str,sclassgroups_list))
			if cgimode:
				if masterconn.is_pro() and not strver.endswith(" PRO"):
					verclass = "BADVERSION"
				elif masterconn.sort_ver() > sortver:
					verclass = "LOWERVERSION"
				elif masterconn.sort_ver() < sortver:
					verclass = "HIGHERVERSION"
				else:
					verclass = "OKVERSION"
				out.append("""	<tr>""")
				out.append("""		<td align="right"></td>""")
				out.append("""		<td align="center">%s</td>""" % sessionstr)
				out.append("""		<td align="left">%s</td>""" % host)
				out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (sortipnum,ipnum))
				out.append("""		<td align="left">%s</td>""" % htmlentities(info))
				if masterconn.version_at_least(1,7,8):
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(openfiles))
					out.append("""		<td align="center">%s</td>""" % decimal_number_html(nsocks))
				out.append("""		<td align="center"><span class="sortkey">%s </span><span class="%s">%s</span></td>""" % (sortver,verclass,strver.replace("PRO","<small>PRO</small>")))
				out.append("""		<td align="left">%s</td>""" % (".&nbsp;(META)" if meta else htmlentities(path)))
				out.append("""		<td align="center">%s</td>""" % ("ro" if sesflags&1 else "rw"))
				out.append("""		<td align="center">%s</td>""" % ("no" if sesflags&2 else "yes"))
				out.append("""		<td align="center">%s</td>""" % ("-" if meta else "yes" if sesflags&4 else "no"))
				if masterconn.version_at_least(1,7,0):
					out.append("""		<td align="center">%s</td>""" % ("-" if meta else "yes" if sesflags&8 else "no"))
				if meta:
					out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
				else:
					out.append("""		<td align="right">%u</td>""" % rootuid)
					out.append("""		<td align="right">%u</td>""" % rootgid)
				if meta or (sesflags&16)==0:
					out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
				else:
					out.append("""		<td align="right">%u</td>""" % mapalluid)
					out.append("""		<td align="right">%u</td>""" % mapallgid)
				if masterconn.version_at_least(1,6,26):
					out.append("""		<td align="center"><span class="sortkey">%u</span>%s</td>""" % (sclassgroups_sort,sclassgroups_str))
					if mintrashretention!=None and maxtrashretention!=None:
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s</a></td>""" % (mintrashretention,timeduration_to_fullstr(mintrashretention),timeduration_to_shortstr(mintrashretention)))
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s</a></td>""" % (maxtrashretention,timeduration_to_fullstr(maxtrashretention),timeduration_to_shortstr(maxtrashretention)))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
				if masterconn.has_feature(FEATURE_EXPORT_UMASK):
					if umaskval==None:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					else:
						out.append("""		<td align="center">%03o</td>""" % umaskval)
				if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
					out.append("""		<td align="center"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%08X</a></td>""" % (disables,disablesmask_to_string(disables),disables))
				out.append("""	</tr>""")
			elif ttymode:
				dline = [sessionstr,host,info]
				if masterconn.version_at_least(1,7,8):
					dline.extend((openfiles,nsocks))
				dline.extend((strver,".&nbsp;(META)" if meta else path,"ro" if sesflags&1 else "rw","no" if sesflags&2 else "yes","-" if meta else "yes" if sesflags&4 else "no"))
				if masterconn.version_at_least(1,7,0):
					dline.append("-" if meta else "yes" if sesflags&8 else "no")
				if meta:
					dline.extend(("-","-"))
				else:
					dline.extend((rootuid,rootgid))
				if meta or (sesflags&16)==0:
					dline.extend(("-","-"))
				else:
					dline.extend((mapalluid,mapallgid))
				if masterconn.version_at_least(1,6,26):
					dline.append("%04X (%s)" % (sclassgroups,sclassgroups_str))
					if mintrashretention!=None and maxtrashretention!=None:
						dline.extend((timeduration_to_shortstr(mintrashretention),timeduration_to_shortstr(maxtrashretention)))
					else:
						dline.extend(("-","-"))
				if masterconn.has_feature(FEATURE_EXPORT_UMASK):
					if umaskval==None:
						dline.append("-")
					else:
						dline.append("%03o" % umaskval)
				if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
					dline.append("%08X (%s)" % (disables,disablesmask_to_string(disables)))
				tab.append(*dline)
			elif jsonmode:
				json_ms_dict = {}
				json_ms_dict["connected"] = True
				json_ms_dict["temporary"] = sessiontmp
				json_ms_dict["session_id"] = sessionid
				json_ms_dict["session_id_str"] = sessionstr
				json_ms_dict["hostname"] = host
				json_ms_dict["ip"] = ipnum
				json_ms_dict["mount_point"] = info
				if masterconn.version_at_least(1,7,8):
					json_ms_dict["open_files"] = openfiles
					json_ms_dict["number_of_sockets"] = nsocks
				else:
					json_ms_dict["open_files"] = None
					json_ms_dict["number_of_sockets"] = None
				json_ms_dict["seconds_to_expire"] = None
				json_ms_dict["strver"] = strver
				if strver.endswith(" PRO"):
					json_ms_dict["version"] = strver[:-4]
					json_ms_dict["pro"] = True
				else:
					json_ms_dict["version"] = strver
					json_ms_dict["pro"] = False
				json_ms_dict["meta"] = meta
				json_ms_dict["path"] = "META" if meta else path
				json_ms_dict["session_flags"] = sesflags
				json_ms_dict["access_rw"] = False if sesflags&1 else True
				json_ms_dict["restricted"] = False if sesflags&2 else True
				json_ms_dict["ignore_gid"] = None if meta else True if sesflags&4 else False
				if masterconn.version_at_least(1,7,0):
					json_ms_dict["admin"] = None if meta else True if sesflags&8 else False
				else:
					json_ms_dict["admin"] = None
				if meta:
					json_ms_dict["map_root_uid"] = None
					json_ms_dict["map_root_gid"] = None
				else:
					json_ms_dict["map_root_uid"] = rootuid
					json_ms_dict["map_root_gid"] = rootgid
				if meta or (sesflags&16)==0:
					json_ms_dict["map_user_uid"] = None
					json_ms_dict["map_user_gid"] = None
				else:
					json_ms_dict["map_user_uid"] = mapalluid
					json_ms_dict["map_user_gid"] = mapallgid
				if masterconn.version_at_least(1,6,26):
					json_ms_dict["allowed_storage_classes"] = sclassgroups
					json_ms_dict["allowed_storage_classes_str"] = sclassgroups_str
					if mintrashretention!=None and maxtrashretention!=None:
						json_ms_dict["trash_retention_min"] = mintrashretention
						json_ms_dict["trash_retention_max"] = maxtrashretention
					else:
						json_ms_dict["trash_retention_min"] = None
						json_ms_dict["trash_retention_max"] = None
				else:
					json_ms_dict["allowed_storage_classes"] = None
					json_ms_dict["allowed_storage_classes_str"] = None
					json_ms_dict["trash_retention_min"] = None
					json_ms_dict["trash_retention_max"] = None
#				json_ms_dict["goal_limit_min"] = None
#				json_ms_dict["goal_limit_max"] = None
				if masterconn.has_feature(FEATURE_EXPORT_UMASK):
					if umaskval==None:
						json_ms_dict["global_umask"] = None
					else:
						json_ms_dict["global_umask"] = "%03o" % umaskval
				else:
					json_ms_dict["global_umask"] = None
				if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
					json_ms_dict["disables_mask"] = None if meta else "%08X" % disables
					json_ms_dict["disables_str"] = "" if meta else disablesmask_to_string(disables)
					json_ms_dict["disables"] = [] if meta else disablesmask_to_string_list(disables)
				else:
					json_ms_dict["disables_mask"] = None
					json_ms_dict["disables_str"] = ""
					json_ms_dict["disables"] = []
				json_ms_array.append(json_ms_dict)
			else:
				dline = [sessionstr,host,info]
				if masterconn.version_at_least(1,7,8):
					dline.extend((openfiles,nsocks))
				dline.extend((strver,".&nbsp;(META)" if meta else path,"ro" if sesflags&1 else "rw","no" if sesflags&2 else "yes","-" if meta else "yes" if sesflags&4 else "no"))
				if masterconn.version_at_least(1,7,0):
					dline.append("-" if meta else "yes" if sesflags&8 else "no")
				if meta:
					dline.extend(("-","-"))
				else:
					dline.extend((rootuid,rootgid))
				if meta or (sesflags&16)==0:
					dline.extend(("-","-"))
				else:
					dline.extend((mapalluid,mapallgid))
				if masterconn.version_at_least(1,6,26):
					dline.append("%04X" % sclassgroups)
					if mintrashretention!=None and maxtrashretention!=None:
						dline.extend((mintrashretention,maxtrashretention))
					else:
						dline.extend(("-","-"))
				if masterconn.has_feature(FEATURE_EXPORT_UMASK):
					if umaskval==None:
						dline.append("-")
					else:
						dline.append("%03o" % umaskval)
				if masterconn.has_feature(FEATURE_EXPORT_DISABLES):
					dline.append("%08X" % disables)
				tab.append(*dline)
		if len(dservers)>0 and masterconn.version_at_least(1,7,8):
			if cgimode:
				out.append("""</table>""")
				out.append("""<div class="tab_title">Inactive mounts (parameters)</div>""")
				out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsmounts" cellspacing="0">""")
				out.append("""	<tr>""")
				out.append("""		<th class="acid_tab_enumerate">#</th>""")
				out.append("""		<th>Session&nbsp;id</th>""")
				out.append("""		<th>Host</th>""")
				out.append("""		<th>IP</th>""")
				out.append("""		<th>Mount&nbsp;point</th>""")
				out.append("""		<th>Open files</th>""")
				out.append("""		<th>Expires</th>""")
				if (not readonly):
					out.append("""		<th>cmd</th>""")
				out.append("""	</tr>""")
			elif ttymode:
				tabcols = (20 if masterconn.has_feature(FEATURE_EXPORT_DISABLES) else 19 if masterconn.has_feature(FEATURE_EXPORT_UMASK) else 18 if masterconn.version_at_least(1,7,8) else 16 if masterconn.version_at_least(1,7,0) else 15 if masterconn.version_at_least(1,6,26) else 12)
				tab.append(("---","",tabcols))
				tab.append(("Inactive mounts (parameters)","1c",tabcols))
				tab.append(("---","",tabcols))
				dline = [("session id","c"),("ip/host","c"),("mount point","c"),("open files","c"),("expires","c"),("command to remove","c",tabcols-5)]
				tab.append(*dline)
				tab.append(("---","",tabcols))
			elif jsonmode:
				pass
			else:
				print(myunicode(tab))
				print("")
				tab = Table("inactive mounts, parameters",5)
		for sf,sessionid,host,sortipnum,ipnum,info,openfiles,expire in dservers:
			if cgimode:
				out.append("""	<tr>""")
				out.append("""		<td align="right"></td>""")
				out.append("""		<td align="center">%u</td>""" % sessionid)
				out.append("""		<td align="left">%s</td>""" % host)
				out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (sortipnum,ipnum))
				out.append("""		<td align="left">%s</td>""" % info)
				out.append("""		<td align="center">%u</td>""" % openfiles)
				out.append("""		<td align="center">%u</td>""" % expire)
				if (not readonly):
					out.append("""		<td align="center"><a href="%s">click to remove</a></td>""" % createhtmllink({"MSremove":("%u" % (sessionid))}))
				out.append("""	</tr>""")
			elif ttymode:
				tabcols = (20 if masterconn.has_feature(FEATURE_EXPORT_DISABLES) else 19 if masterconn.has_feature(FEATURE_EXPORT_UMASK) else 18 if masterconn.version_at_least(1,7,8) else 16 if masterconn.version_at_least(1,7,0) else 15 if masterconn.version_at_least(1,6,26) else 12)
				dline = [sessionid,host,info,openfiles,expire,("%s -H %s -P %u -CRS/%u" % (sys.argv[0],masterhost,masterport,sessionid),"l",tabcols-5)]
				tab.append(*dline)
			elif jsonmode:
				json_ms_dict = {}
				json_ms_dict["connected"] = False
				json_ms_dict["temporary"] = False
				json_ms_dict["session_id"] = sessionid
				json_ms_dict["session_id_str"] = ("%u" % sessionid)
				json_ms_dict["hostname"] = host
				json_ms_dict["ip"] = ipnum
				json_ms_dict["mount_point"] = info
				if masterconn.version_at_least(1,7,8):
					json_ms_dict["open_files"] = openfiles
					json_ms_dict["number_of_sockets"] = 0
				else:
					json_ms_dict["open_files"] = None
					json_ms_dict["number_of_sockets"] = None
				json_ms_dict["seconds_to_expire"] = expire
				json_ms_dict["strver"] = None
				json_ms_dict["version"] = None
				json_ms_dict["pro"] = None
				json_ms_dict["meta"] = None
				json_ms_dict["path"] = None
				json_ms_dict["session_flags"] = None
				json_ms_dict["access_rw"] = None
				json_ms_dict["restricted"] = None
				json_ms_dict["ignore_gid"] = None
				json_ms_dict["admin"] = None
				json_ms_dict["map_root_uid"] = None
				json_ms_dict["map_root_gid"] = None
				json_ms_dict["map_user_uid"] = None
				json_ms_dict["map_user_gid"] = None
				json_ms_dict["allowed_storage_classes"] = None
				json_ms_dict["allowed_storage_classes_str"] = None
#				json_ms_dict["goal_limit_min"] = None
#				json_ms_dict["goal_limit_max"] = None
				json_ms_dict["trash_retention_min"] = None
				json_ms_dict["trash_retention_max"] = None
				json_ms_dict["global_umask"] = None
				json_ms_dict["disables_mask"] = None
				json_ms_dict["disables_str"] = ""
				json_ms_dict["disables"] = []
				json_ms_array.append(json_ms_dict)
			else:
				dline = [sessionid,host,info,openfiles,expire]
				tab.append(*dline)
		if cgimode:
			out.append("""</table>""")
			print("\n".join(out))
		elif jsonmode:
			jcollect["dataset"]["mounts"] = json_ms_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

if "MO" in sectionset and leaderfound:
	try:
		if cgimode:
			out = []
			out.append("""<div class="tab_title">Active mounts (operations)</div>""")
			out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsops" cellspacing="0" id="mfsops">""")
			out.append("""	<tr>""")
			out.append("""		<th colspan="4" class="knob-cell">""")
			options=[(-90,"Last hour",None,"acid_tab.switchdisplay('mfsops','opshour_vis',0);"),
					(90, "Current hour",None,"acid_tab.switchdisplay('mfsops','opshour_vis',1);")]
			out.append(html_knob_selector("opshour_vis",9,(210,22),(100,11),options))

			out.append("""		</th>""")
			out.append("""		<th colspan="%u" style="vertical-align: middle;">""" % (1+dataprovider.stats_to_show))
			out.append("""			<span class="opshour_vis0">Last hour operations</span>""")
			out.append("""			<span class="opshour_vis1">Current hour operations</span>""")
			out.append("""		</th>""")
			out.append("""	</tr>""")
			out.append("""	<tr>""")
			out.append("""		<th rowspan="1" class="acid_tab_enumerate">#</th>""")
			out.append("""		<th rowspan="1">Host</th>""")
			out.append("""		<th rowspan="1">IP</th>""")
			out.append("""		<th rowspan="1">Mount&nbsp;point</th>""")
			out.append("""		<th class="acid_tab_level_1">statfs</th>""")
			out.append("""		<th class="acid_tab_level_1">getattr</th>""")
			out.append("""		<th class="acid_tab_level_1">setattr</th>""")
			out.append("""		<th class="acid_tab_level_1">lookup</th>""")
			out.append("""		<th class="acid_tab_level_1">mkdir</th>""")
			out.append("""		<th class="acid_tab_level_1">rmdir</th>""")
			out.append("""		<th class="acid_tab_level_1">symlink</th>""")
			out.append("""		<th class="acid_tab_level_1">readlink</th>""")
			out.append("""		<th class="acid_tab_level_1">mknod</th>""")
			out.append("""		<th class="acid_tab_level_1">unlink</th>""")
			out.append("""		<th class="acid_tab_level_1">rename</th>""")
			out.append("""		<th class="acid_tab_level_1">link</th>""")
			out.append("""		<th class="acid_tab_level_1">readdir</th>""")
			out.append("""		<th class="acid_tab_level_1">open</th>""")
			out.append("""		<th class="acid_tab_level_1">rchunk</th>""")
			out.append("""		<th class="acid_tab_level_1">wchunk</th>""")
			if (dataprovider.stats_to_show>16):
				out.append("""		<th class="acid_tab_level_1">read</th>""")
				out.append("""		<th class="acid_tab_level_1">write</th>""")
				out.append("""		<th class="acid_tab_level_1">fsync</th>""")
				out.append("""		<th class="acid_tab_level_1">snapshot</th>""")
				out.append("""		<th class="acid_tab_level_1">truncate</th>""")
				out.append("""		<th class="acid_tab_level_1">getxattr</th>""")
				out.append("""		<th class="acid_tab_level_1">setxattr</th>""")
				out.append("""		<th class="acid_tab_level_1">getfacl</th>""")
				out.append("""		<th class="acid_tab_level_1">setfacl</th>""")
				out.append("""		<th class="acid_tab_level_1">create</th>""")
				out.append("""		<th class="acid_tab_level_1">lock</th>""")
				out.append("""		<th class="acid_tab_level_1">meta</th>""")
			out.append("""		<th class="acid_tab_level_1">total</th>""")
			out.append("""	</tr>""")
		elif jsonmode:
			if (dataprovider.stats_to_show<=16):
				json_stats_keys = ["statfs","getattr","setattr","lookup","mkdir","rmdir","symlink","readlink","mknod","unlink","rename","link","readdir","open","read","write"]
			else:
				json_stats_keys = ["statfs","getattr","setattr","lookup","mkdir","rmdir","symlink","readlink","mknod","unlink","rename","link","readdir","open","rchunk","wchunk","read","write","fsync","snapshot","truncate","getxattr","setxattr","getfacl","setfacl","create","lock","meta"]
			json_mo_array = []
		elif ttymode:
			tab = Table("Active mounts (operations)",3+dataprovider.stats_to_show)
			tab.header("","",("operations %s hour" % ("last" if MOdata==0 else "current"),"",1+dataprovider.stats_to_show))
			tab.header("host/ip","mount point",("---","",1+dataprovider.stats_to_show))
			if (dataprovider.stats_to_show<=16):
				tab.header("","","statfs","getattr","setattr","lookup","mkdir","rmdir","symlink","readlink","mknod","unlink","rename","link","readdir","open","read","write","total")
				tab.defattr("r","l","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r")
			else:
				tab.header("","","statfs","getattr","setattr","lookup","mkdir","rmdir","symlink","readlink","mknod","unlink","rename","link","readdir","open","rchunk","wchunk","read","write","fsync","snapshot","truncate","getxattr","setxattr","getfacl","setfacl","create","lock","meta","total")
				tab.defattr("r","l","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r")
		else:
			tab = Table("active mounts, operations",3+dataprovider.stats_to_show)
		servers = []
		for ses in dataprovider.get_sessions():
			sf = ses.sortip
			if MOorder==1:
				sf = ses.host
			elif MOorder==2:
				sf = ses.sortip
			elif MOorder==3:
				sf = ses.info
			elif MOorder>=100 and MOorder<100+dataprovider.stats_to_show:
				sfmul = -1 if cgimode else 1
				if MOdata==0:
					sf = sfmul * ses.stats_l[MOorder-100]
				else:
					sf = sfmul * ses.stats_c[MOorder-100]
			elif MOorder==150:
				sfmul = -1 if cgimode else 1
				if MOdata==0:
					sf = sfmul * sum(ses.stats_l)
				else:
					sf = sfmul * sum(ses.stats_c)
			if ses.path!='.':
				servers.append((sf,ses.host,ses.sortip,ses.strip,ses.info,ses.stats_c,ses.stats_l))
		servers.sort()
		if MOrev:
			servers.reverse()
		for sf,host,sortipnum,ipnum,info,stats_c,stats_l in servers:
			if cgimode:
				out.append("""	<tr>""")
				out.append("""		<td align="right"></td>""")
				out.append("""		<td align="left">%s</td>""" % host)
				out.append("""		<td align="center"><span class="sortkey">%s</span>%s</td>""" % (sortipnum,ipnum))
				out.append("""		<td align="left">%s</td>""" % htmlentities(info))
				for st in range(dataprovider.stats_to_show):
					out.append("""		<td align="right">""")
					out.append("""			<span class="opshour_vis0"><a style="cursor:default" title="current:%u last:%u">%s</a></span>""" % (stats_c[st],stats_l[st],decimal_number_html(stats_l[st])))
					out.append("""			<span class="opshour_vis1"><a style="cursor:default" title="current:%u last:%u">%s</a></span>""" % (stats_c[st],stats_l[st],decimal_number_html(stats_c[st])))
					out.append("""		</td>""")
				out.append("""		<td align="right">""")
				out.append("""			<span class="opshour_vis0"><a style="cursor:default" title="current:%u last:%u">%s</a></span>""" % (sum(stats_c),sum(stats_l),decimal_number_html(sum(stats_l))))
				out.append("""			<span class="opshour_vis1"><a style="cursor:default" title="current:%u last:%u">%s</a></span>""" % (sum(stats_c),sum(stats_l),decimal_number_html(sum(stats_c))))
				out.append("""		</td>""")
				out.append("""	</tr>""")
			elif jsonmode:
				json_mo_dict = {}
				json_mo_dict["hostname"] = host
				json_mo_dict["ip"] = ipnum
				json_mo_dict["mount_point"] = info
				json_stats_c_dict = {}
				json_stats_l_dict = {}
				for i,name in enumerate(json_stats_keys):
					json_stats_c_dict[name] = stats_c[i]
					json_stats_l_dict[name] = stats_l[i]
				json_stats_c_dict["total"] = sum(stats_c)
				json_stats_l_dict["total"] = sum(stats_l)
				json_mo_dict["stats_current_hour"] = json_stats_c_dict
				json_mo_dict["stats_last_hour"] = json_stats_l_dict
				json_mo_array.append(json_mo_dict)
			else:
				ldata = [host,info]
				if MOdata==0:
					ldata.extend(stats_l)
					ldata.append(sum(stats_l))
				else:
					ldata.extend(stats_c)
					ldata.append(sum(stats_c))
				tab.append(*ldata)
		if cgimode:
			out.append("""</table>""")
			print("\n".join(out))
		elif jsonmode:
			jcollect["dataset"]["operations"] = json_mo_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

if "RS" in sectionset and leaderfound:
	if "SC" in sectionsubset:
		try:
			show_copy_and_ec = 0
			show_arch_min_size = 0
			show_labelmode_overrides = 0
			show_export_group_and_priority = 0
			if masterconn.version_at_least(4,5,0):
				show_copy_and_ec = 1
			if masterconn.version_at_least(4,34,0):
				show_arch_min_size = 1
			if masterconn.has_feature(FEATURE_LABELMODE_OVERRIDES):
				show_labelmode_overrides = 1
			if masterconn.has_feature(FEATURE_SCLASSGROUPS):
				show_export_group_and_priority = 1
			if cgimode:
				out = []
				out.append("""<div class="tab_title">Storage classes</div>""")
				out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfssc" cellspacing="0">""")
				if show_copy_and_ec:
					out.append("""	<tr>""")
					out.append("""		<th rowspan="3" class="acid_tab_enumerate">#</th>""")
					out.append("""		<th rowspan="3">Id</th>""")
					out.append("""		<th rowspan="3" class="acid_tab_alpha">Name</th>""")
					if show_export_group_and_priority:
						out.append("""		<th rowspan="3">Join<br/>priority</th>""")
						out.append("""		<th rowspan="3">Export<br/>group</th>""")
					out.append("""		<th rowspan="3">Admin<br/>only</th>""")
					out.append("""		<th rowspan="3">Labels<br/>mode</th>""")
					out.append("""		<th colspan="%d" class="acid_tab_skip">Archive state transition</th>""" % (2 + show_arch_min_size))
					out.append("""		<th rowspan="3">Min<br/>trash<br/>retention</th>""") #min trash retension
					out.append("""		<th colspan="2" class="acid_tab_skip"># of inodes</th>""")
					out.append("""		<th colspan="%u" class="acid_tab_skip">States statistics and definitions</th>""" % (11 + show_labelmode_overrides))
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					out.append("""		<th rowspan="2">mode</th>""")
					out.append("""		<th rowspan="2" style="min-width:32px;">delay</th>""")
					if show_arch_min_size:
						out.append("""		<th rowspan="2" style="min-width:32px;">min<br/>size</th>""")
					out.append("""		<th rowspan="2">files</th>""")
					out.append("""		<th rowspan="2">dirs</th>""")
					out.append("""		<th rowspan="2" class="acid_tab_skip">state</th>""")
					out.append("""		<th colspan="2" class="acid_tab_skip">chunks under</th>""")
					out.append("""		<th colspan="2" class="acid_tab_skip">chunks exact</th>""")
					out.append("""		<th colspan="2" class="acid_tab_skip">chunks over</th>""")
					out.append("""		<th rowspan="2" class="acid_tab_skip">achiev-<br/>able</th>""")
					if show_labelmode_overrides:
						out.append("""		<th rowspan="2" class="acid_tab_skip">labels mode<br/>override</th>""")
					out.append("""		<th rowspan="2" class="acid_tab_skip">redundancy level</th>""")
					out.append("""		<th rowspan="2" class="acid_tab_skip">labels</th>""")
					out.append("""		<th rowspan="2" class="acid_tab_skip">distribution</th>""")
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					out.append("""		<th class="acid_tab_skip" style="min-width:32px;">COPY</th>""")
					out.append("""		<th class="acid_tab_skip" style="min-width:32px;">EC</th>""")
					out.append("""		<th class="acid_tab_skip" style="min-width:32px;">COPY</th>""")
					out.append("""		<th class="acid_tab_skip" style="min-width:32px;">EC</th>""")
					out.append("""		<th class="acid_tab_skip" style="min-width:32px;">COPY</th>""")
					out.append("""		<th class="acid_tab_skip" style="min-width:32px;">EC</th>""")
					out.append("""	</tr>""")
				else:
					out.append("""	<tr>""")
					out.append("""		<th rowspan="3" class="acid_tab_enumerate">#</th>""")
					out.append("""		<th rowspan="2">id</th>""")
					out.append("""		<th rowspan="2" classname="acid_tab_alpha">name</th>""")
					out.append("""		<th rowspan="2">admin only</th>""")
					out.append("""		<th rowspan="2">labels mode</th>""")
					out.append("""		<th rowspan="2">arch mode</th>""")
					out.append("""		<th rowspan="2">arch delay</th>""")
					out.append("""		<th rowspan="2">min trashretention</th>""")
					out.append("""		<th colspan="2" class="acid_tab_skip"># of inodes</th>""")
					out.append("""		<th colspan="8" class="acid_tab_skip">state statistics and definitions</th>""")
					out.append("""	</tr>""")
					out.append("""	<tr>""")
					out.append("""		<th>files</th>""")
					out.append("""		<th>dirs</th>""")
					out.append("""		<th class="acid_tab_skip">state</th>""")
					out.append("""		<th class="acid_tab_skip">chunks under</th>""")
					out.append("""		<th class="acid_tab_skip">chunks exact</th>""")
					out.append("""		<th class="acid_tab_skip">chunks over</th>""")
					out.append("""		<th class="acid_tab_skip">achievable</th>""")
					out.append("""		<th class="acid_tab_skip">redundancy level</th>""")
					out.append("""		<th class="acid_tab_skip">labels</th>""")
					out.append("""		<th class="acid_tab_skip">distribution</th>""")
					out.append("""	</tr>""")
			elif jsonmode:
				json_sc_array = []
			elif ttymode:
				if show_copy_and_ec:
					if show_arch_min_size:
						if show_labelmode_overrides:
							if show_export_group_and_priority:
								tab = Table("Storage Classes",24,"r")
								tab.header("","","","","","","","","","",("# of inodes","",2),("state statistics and definitions","",12))
								tab.header("","","","","","","","","","",("---","",14))
								tab.header("id","name","join priority","export group","admin only","labels mode","arch mode","arch delay","arch min size","min trashretention","","","",("chunks under","",2),("chunks exact","",2),("chunks over","",2),"","","","","")
								tab.header("","","","","","","","","","","files","dirs","state",("---","",6),"can be fulfilled","labels mode override","redundancy level","labels","distribution")
								tab.header("","","","","","","","","","","","","","COPY","EC","COPY","EC","COPY","EC","","","","","")
								tab.defattr("r","l","r","r","c","c","c","r","r","r","r","r","c","r","r","r","r","r","r","c","c","c","c","c")
							else:
								tab = Table("Storage Classes",22,"r")
								tab.header("","","","","","","","",("# of inodes","",2),("state statistics and definitions","",12))
								tab.header("","","","","","","","",("---","",14))
								tab.header("id","name","admin only","labels mode","arch mode","arch delay","arch min size","min trashretention","","","",("chunks under","",2),("chunks exact","",2),("chunks over","",2),"","","","","")
								tab.header("","","","","","","","","files","dirs","state",("---","",6),"can be fulfilled","labels mode override","redundancy level","labels","distribution")
								tab.header("","","","","","","","","","","","COPY","EC","COPY","EC","COPY","EC","","","","","")
								tab.defattr("r","l","c","c","c","r","r","r","r","r","c","r","r","r","r","r","r","c","c","c","c","c")
						else:
							tab = Table("Storage Classes",21,"r")
							tab.header("","","","","","","","",("# of inodes","",2),("state statistics and definitions","",11))
							tab.header("","","","","","","","",("---","",13))
							tab.header("id","name","admin only","labels mode","arch mode","arch delay","arch min size","min trashretention","","","",("chunks under","",2),("chunks exact","",2),("chunks over","",2),"","","","")
							tab.header("","","","","","","","","files","dirs","state",("---","",6),"can be fulfilled","redundancy level","labels","distribution")
							tab.header("","","","","","","","","","","","COPY","EC","COPY","EC","COPY","EC","","","","")
							tab.defattr("r","l","c","c","c","r","r","r","r","r","c","r","r","r","r","r","r","c","c","c","c")
					else:
						tab = Table("Storage Classes",20,"r")
						tab.header("","","","","","","",("# of inodes","",2),("state statistics and definitions","",11))
						tab.header("","","","","","","",("---","",13))
						tab.header("id","name","admin only","labels mode","arch mode","arch delay","min trashretention","","","",("chunks under","",2),("chunks exact","",2),("chunks over","",2),"","","","")
						tab.header("","","","","","","","files","dirs","state",("---","",6),"can be fulfilled","redundancy level","labels","distribution")
						tab.header("","","","","","","","","","","COPY","EC","COPY","EC","COPY","EC","","","","")
						tab.defattr("r","l","c","c","c","r","r","r","r","c","r","r","r","r","r","r","c","c","c","c")
				else:
					tab = Table("Storage Classes",17,"r")
					tab.header("","","","","","","",("# of inodes","",2),("state statistics and definitions","",8))
					tab.header("id","name","admin only","labels mode","arch mode","arch delay","min trashretention",("---","",10))
					tab.header("","","","","","","","files","dirs","state","chunks under","chunks exact","chunks over","can be fulfilled","redundancy level","labels","distribution")
					tab.defattr("r","l","c","c","c","r","r","r","r","c","r","r","r","c","c","c","c")
			else:
				if show_copy_and_ec:
					tab = Table("storage classes",13)
				else:
					tab = Table("storage classes",10)
			sclasses = []
			if masterconn.has_feature(FEATURE_SCLASSGROUPS):
				data,length = masterconn.command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",5))
				fver = 5
			elif masterconn.has_feature(FEATURE_LABELMODE_OVERRIDES):
				data,length = masterconn.command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",4))
				fver = 4
			elif masterconn.version_at_least(4,34,0):
				data,length = masterconn.command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",3))
				fver = 3
			elif masterconn.version_at_least(4,5,0):
				data,length = masterconn.command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",2))
				fver = 2
			elif masterconn.version_at_least(4,2,0):
				data,length = masterconn.command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO,struct.pack(">B",1))
				fver = 1
			else:
				data,length = masterconn.command(CLTOMA_SCLASS_INFO,MATOCL_SCLASS_INFO)
				fver = 0
			scount = struct.unpack(">H",data[:2])[0]
			pos = 2
			while pos < length:
				if show_copy_and_ec:
					createcounters = 6*[None]
					keepcounters = 6*[None]
					archcounters = 6*[None]
					trashcounters = 6*[None]
				else:
					createcounters = 3*[None]
					keepcounters = 3*[None]
					archcounters = 3*[None]
					trashcounters = 3*[None]
				if masterconn.version_at_least(4,0,0):
					sclassid,sclassnleng = struct.unpack_from(">BB",data,pos)
					pos += 2
					sclassname = data[pos:pos+sclassnleng]
					sclassname = sclassname.decode('utf-8','replace')
					pos += sclassnleng
					if fver>=5:
						sclassdleng = data[pos]
						pos+=1
						sclassdesc = data[pos:pos+sclassdleng]
						sclassdesc = sclassdesc.decode('utf-8','replace')
						pos += sclassdleng
					else:
						sclassdesc = ""
						sclassdleng = 0
					files,dirs = struct.unpack_from(">LL",data,pos)
					pos += 8
					if fver>=2:
						for cntid in xrange(6):
							keepcounters[cntid],archcounters[cntid],trashcounters[cntid] = struct.unpack_from(">QQQ",data,pos)
							pos += 3*8
					else:
						for cntid in xrange(3):
							keepcounters[cntid],archcounters[cntid],trashcounters[cntid] = struct.unpack_from(">QQQ",data,pos)
							pos += 3*8
					if fver>=5:
						priority,export_group = struct.unpack_from(">LB",data,pos)
						pos += 5
					else:
						priority = None
						export_group = sclassid if sclassid<10 else 0
					if fver>=3:
						admin_only,labels_mode,arch_mode,arch_delay,arch_min_size,min_trashretention = struct.unpack_from(">BBBHQH",data,pos)
						pos += 15
					elif fver>=1:
						admin_only,labels_mode,arch_mode,arch_delay,min_trashretention = struct.unpack_from(">BBBHH",data,pos)
						arch_min_size = 0
						pos += 7
					else:
						admin_only,labels_mode,arch_delay,min_trashretention = struct.unpack_from(">BBHH",data,pos)
						arch_mode = 1
						arch_min_size = 0
						pos += 6
					if fver>=4:
						create_canbefulfilled,create_labelscnt,create_uniqmask,create_labelsmode = struct.unpack_from(">BBLB",data,pos)
						pos += 7
						keep_canbefulfilled,keep_labelscnt,keep_uniqmask,keep_labelsmode = struct.unpack_from(">BBLB",data,pos)
						pos += 7
						arch_canbefulfilled,arch_labelscnt,arch_ec_level,arch_uniqmask,arch_labelsmode = struct.unpack_from(">BBBLB",data,pos)
						pos += 8
						trash_canbefulfilled,trash_labelscnt,trash_ec_level,trash_uniqmask,trash_labelsmode = struct.unpack_from(">BBBLB",data,pos)
						pos += 8
					else:
						create_canbefulfilled,create_labelscnt,create_uniqmask = struct.unpack_from(">BBL",data,pos)
						pos += 6
						keep_canbefulfilled,keep_labelscnt,keep_uniqmask = struct.unpack_from(">BBL",data,pos)
						pos += 6
						arch_canbefulfilled,arch_labelscnt,arch_ec_level,arch_uniqmask = struct.unpack_from(">BBBL",data,pos)
						pos += 7
						trash_canbefulfilled,trash_labelscnt,trash_ec_level,trash_uniqmask = struct.unpack_from(">BBBL",data,pos)
						pos += 7
						create_labelsmode = -1
						keep_labelsmode = -1
						arch_labelsmode = -1
						trash_labelsmode = -1
				elif masterconn.version_at_least(3,0,75):
					sclassid,sclassnleng = struct.unpack_from(">BB",data,pos)
					pos += 2
					sclassname = data[pos:pos+sclassnleng]
					sclassname = sclassname.decode('utf-8','replace')
					sclassdesc = ""
					sclassdleng = 0
					priority = None
					export_group = sclassid if sclassid<10 else 0
					pos += sclassnleng
					files,dirs,keepcounters[0],archcounters[0],keepcounters[1],archcounters[1],keepcounters[2],archcounters[2],admin_only,labels_mode,arch_delay,create_canbefulfilled,create_labelscnt,keep_canbefulfilled,keep_labelscnt,arch_canbefulfilled,arch_labelscnt = struct.unpack_from(">LLQQQQQQBBHBBBBBB",data,pos)
					pos += 18 + 3 * 16
					if arch_delay==0:
						for cntid in xrange(3):
							keepcounters[cntid] += archcounters[cntid]
							archcounters[cntid] = None
					min_trashretention = 0
					trash_canbefulfilled = None
					trash_labelscnt = None
					trash_ec_level = 0
					arch_ec_level = 0
					create_uniqmask = 0
					keep_uniqmask = 0
					arch_uniqmask = 0
					trash_uniqmask = 0
					create_labelsmode = -1
					keep_labelsmode = -1
					arch_labelsmode = -1
					trash_labelsmode = -1
					arch_mode = 1
					arch_min_size = 0
				elif masterconn.version_at_least(3,0,9):
					sclassid,files,dirs,keepcounters[0],archcounters[0],keepcounters[1],archcounters[1],keepcounters[2],archcounters[2],labels_mode,arch_delay,create_canbefulfilled,create_labelscnt,keep_canbefulfilled,keep_labelscnt,arch_canbefulfilled,arch_labelscnt = struct.unpack_from(">BLLQQQQQQBHBBBBBB",data,pos)
					pos += 18 + 3 * 16
					sclassdesc = ""
					sclassdleng = 0
					priority = None
					export_group = sclassid if sclassid<10 else 0
					admin_only = 0
					if sclassid<10:
						sclassname = str(sclassid)
					else:
						sclassname = "sclass_%u" % (sclassid-9)
					if arch_delay==0:
						for cntid in xrange(3):
							keepcounters[cntid] += archcounters[cntid]
							archcounters[cntid] = None
					min_trashretention = 0
					trash_canbefulfilled = None
					trash_labelscnt = None
					trash_ec_level = 0
					arch_ec_level = 0
					create_uniqmask = 0
					keep_uniqmask = 0
					arch_uniqmask = 0
					trash_uniqmask = 0
					create_labelsmode = -1
					keep_labelsmode = -1
					arch_labelsmode = -1
					trash_labelsmode = -1
					arch_mode = 1
					arch_min_size = 0
				else:
					sclassid,files,create_canbefulfilled,create_labelscnt = struct.unpack_from(">BLBB",data,pos)
					pos+=7
					sclassdesc = ""
					sclassdleng = 0
					priority = None
					export_group = sclassid if sclassid<10 else 0
					admin_only = 0
					if sclassid<10:
						sclassname = str(sclassid)
					else:
						sclassname = "sclass_%u" % (sclassid-9)
					dirs = 0
					if create_canbefulfilled:
						create_canbefulfilled = 3
					keep_canbefulfilled = create_canbefulfilled
					arch_canbefulfilled = create_canbefulfilled
					keep_labelscnt = create_labelscnt
					arch_labelscnt = create_labelscnt
					labels_mode = 1
					arch_mode = 1
					arch_delay = 0
					arch_min_size = 0
					min_trashretention = 0
					trash_canbefulfilled = None
					trash_labelscnt = None
					trash_ec_level = 0
					arch_ec_level = 0
					create_uniqmask = 0
					keep_uniqmask = 0
					arch_uniqmask = 0
					trash_uniqmask = 0
					create_labelsmode = -1
					keep_labelsmode = -1
					arch_labelsmode = -1
					trash_labelsmode = -1
				if masterconn.version_at_least(4,0,0):
					create_labellist = []
					keep_labellist = []
					arch_labellist = []
					trash_labellist = []
					for i in xrange(create_labelscnt):
						labelmasks = struct.unpack_from(">"+"B"*SCLASS_EXPR_MAX_SIZE,data,pos)
						pos+=SCLASS_EXPR_MAX_SIZE
						matchingservers = struct.unpack_from(">H",data,pos)[0]
						pos+=2
						create_labellist.append((labelexpr_to_str(labelmasks),matchingservers))
					for i in xrange(keep_labelscnt):
						labelmasks = struct.unpack_from(">"+"B"*SCLASS_EXPR_MAX_SIZE,data,pos)
						pos+=SCLASS_EXPR_MAX_SIZE
						matchingservers = struct.unpack_from(">H",data,pos)[0]
						pos+=2
						keep_labellist.append((labelexpr_to_str(labelmasks),matchingservers))
					for i in xrange(arch_labelscnt):
						labelmasks = struct.unpack_from(">"+"B"*SCLASS_EXPR_MAX_SIZE,data,pos)
						pos+=SCLASS_EXPR_MAX_SIZE
						matchingservers = struct.unpack_from(">H",data,pos)[0]
						pos+=2
						arch_labellist.append((labelexpr_to_str(labelmasks),matchingservers))
					for i in xrange(trash_labelscnt):
						labelmasks = struct.unpack_from(">"+"B"*SCLASS_EXPR_MAX_SIZE,data,pos)
						pos+=SCLASS_EXPR_MAX_SIZE
						matchingservers = struct.unpack_from(">H",data,pos)[0]
						pos+=2
						trash_labellist.append((labelexpr_to_str(labelmasks),matchingservers))
				else:
					create_labellist = []
					for i in xrange(create_labelscnt):
						labelmasks = struct.unpack_from(">"+"L"*MASKORGROUP,data,pos)
						pos+=4*MASKORGROUP
						matchingservers = struct.unpack_from(">H",data,pos)[0]
						pos+=2
						create_labellist.append((labelmasks_to_str(labelmasks),matchingservers))
					if masterconn.version_at_least(3,0,9):
						keep_labellist = []
						for i in xrange(keep_labelscnt):
							labelmasks = struct.unpack_from(">"+"L"*MASKORGROUP,data,pos)
							pos+=4*MASKORGROUP
							matchingservers = struct.unpack_from(">H",data,pos)[0]
							pos+=2
							keep_labellist.append((labelmasks_to_str(labelmasks),matchingservers))
						arch_labellist = []
						for i in xrange(arch_labelscnt):
							labelmasks = struct.unpack_from(">"+"L"*MASKORGROUP,data,pos)
							pos+=4*MASKORGROUP
							matchingservers = struct.unpack_from(">H",data,pos)[0]
							pos+=2
							arch_labellist.append((labelmasks_to_str(labelmasks),matchingservers))
					else:
						keep_labellist = create_labellist
						arch_labellist = create_labellist
					trash_labellist = []
				states = []
				if len(create_labellist)>0:
					states.append((1,"CREATE",createcounters,None,create_labellist,create_uniqmask,create_labelsmode,create_canbefulfilled))
				else:
					states.append((0,"CREATE",createcounters,None,keep_labellist,keep_uniqmask,keep_labelsmode,keep_canbefulfilled))
				states.append((1,"KEEP",keepcounters,None,keep_labellist,keep_uniqmask,keep_labelsmode,keep_canbefulfilled))
				archive_str = "ARCHIVE" if cgimode else "ARCH"
				if len(arch_labellist)>0 or arch_ec_level>0:
					states.append((1,archive_str,archcounters,arch_ec_level,arch_labellist,arch_uniqmask,arch_labelsmode,arch_canbefulfilled))
				else:
					states.append((0,archive_str,archcounters,None,keep_labellist,keep_uniqmask,keep_labelsmode,keep_canbefulfilled))
				if len(trash_labellist)>0 or trash_ec_level>0:
					states.append((1,"TRASH",trashcounters,trash_ec_level,trash_labellist,trash_uniqmask,trash_labelsmode,trash_canbefulfilled))
				else:
					states.append((0,"TRASH",trashcounters,None,keep_labellist,keep_uniqmask,keep_labelsmode,keep_canbefulfilled))
				sf = sclassid
				if SCorder==2:
					sf = sclassname
				elif SCorder==3:
					sf = admin_only
				elif SCorder==4:
					sf = labels_mode
				elif SCorder==5:
					sf = arch_mode
				elif SCorder==6:
					sf = arch_delay
				elif SCorder==7:
					sf = arch_min_size
				elif SCorder==8:
					sf = min_trashretention
				elif SCorder==9:
					sf = files
				elif SCorder==10:
					sf = dirs
				sclasses.append((sf,sclassid,sclassname,sclassdesc,priority,export_group,admin_only,labels_mode,arch_mode,arch_delay,arch_min_size,min_trashretention,files,dirs,states))
			sclasses.sort()
			if SCrev:
				sclasses.reverse()
			firstrow = 1
			for sf,sclassid,sclassname,sclassdesc,priority,export_group,admin_only,labels_mode,arch_mode,arch_delay,arch_min_size,min_trashretention,files,dirs,states in sclasses:
				admin_only_str = "YES" if admin_only else "NO"
				labels_mode_str = "LOOSE" if labels_mode==0 else "STD" if labels_mode==1 else "STRICT"
				arch_mode_list = []
				if arch_mode&SCLASS_ARCH_MODE_CHUNK:
					arch_mode_list.append("CHUNK")
				elif arch_mode&SCLASS_ARCH_MODE_FAST:
					arch_mode_list.append("FAST")
				else:
					arch_mode_list.append("[")
					if arch_mode&SCLASS_ARCH_MODE_CTIME:
						arch_mode_list.append("C")
					if arch_mode&SCLASS_ARCH_MODE_MTIME:
						arch_mode_list.append("M")
					if arch_mode&SCLASS_ARCH_MODE_ATIME:
						arch_mode_list.append("A")
					arch_mode_list.append("]TIME")
					if arch_mode&SCLASS_ARCH_MODE_REVERSIBLE:
						arch_mode_list.append("/REVERSIBLE")
					else:
						arch_mode_list.append("/ONEWAY")
				arch_mode_str = "".join(arch_mode_list)
				arch_delay_str = hours_to_str(arch_delay) if (arch_delay>0 and (arch_mode&SCLASS_ARCH_MODE_FAST)==0) else "-"
				min_trashretention_str = hours_to_str(min_trashretention) if min_trashretention>0 else "-"
				if cgimode:
					out.append("""	<tr>""")
					rowcnt = len(states)
					out.append("""		<td rowspan="%u" align="right"></td>""" % (rowcnt,))
					out.append("""		<td rowspan="%u" align="right">%u</td>""" % (rowcnt,sclassid))

					name = htmlentities(sclassname)
#					if len(name)>12:
#						name=name[:10] + "<br/>" + name[10:20] + "<br/>" + name[20:]
					if sclassdesc!=None and sclassdesc!="":
						desc = htmlentities(sclassdesc)
						out.append("""		<td rowspan="%u" align="right"><a style="cursor:default" title="%s">%s</a></td>""" % (rowcnt,desc,name))
					else:
						out.append("""		<td rowspan="%u" align="right">%s</td>""" % (rowcnt,name))
					if show_export_group_and_priority:
						out.append("""		<td rowspan="%u" align="center">%u</td>""" % (rowcnt,priority))
						out.append("""		<td rowspan="%u" align="center">%u</td>""" % (rowcnt,export_group))
					out.append("""		<td rowspan="%u" align="center">%s</td>""" % (rowcnt,admin_only_str))
					out.append("""		<td rowspan="%u" align="center">%s</td>""" % (rowcnt,labels_mode_str))
					out.append("""		<td rowspan="%u" align="center"><span class="sortkey">%u </span>%s</td>""" % (rowcnt,arch_mode,arch_mode_str.replace("/","<br/>")))
					out.append("""		<td rowspan="%u" align="center"><span class="sortkey">%u </span>%s</td>""" % (rowcnt,arch_delay,arch_delay_str))
					if show_arch_min_size:
						out.append("""		<td rowspan="%u" align="center"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a>""" % (rowcnt,arch_min_size,decimal_number(arch_min_size),humanize_number(arch_min_size,"&nbsp;")))
					out.append("""		<td rowspan="%u" align="center"><span class="sortkey">%u </span>%s</td>""" % (rowcnt,min_trashretention,min_trashretention_str))
					out.append("""		<td rowspan="%u" align="right">%s</td>""" % (rowcnt,decimal_number_html(files)))
					out.append("""		<td rowspan="%u" align="right">%s</td>""" % (rowcnt,decimal_number_html(dirs)))
					newrow = 0
					for defined,name,counters,ec_level,labellist,uniqmask,labelsmodeover,canbefulfilled in states:
						ec_data_parts = 0
						ec_chksum_parts = 0
						if ec_level!=None and ec_level>0:
							ec_data_parts = ec_level >> 4
							ec_chksum_parts = ec_level & 0xF
							if ec_data_parts==0:
								ec_data_parts = 8
						if newrow:
							out.append("""	</tr>""")
							out.append("""	<tr>""")
						else:
							newrow=1
						colorclass = "DEFINED" if defined else "UNDEFINED"
						wcolorclass = "WARNING" if defined else "UNDEFINED"
						ecolorclass = "ERROR" if defined else "UNDEFINED"
						labels_mode_over_str = "LOOSE" if labelsmodeover==0 else "STD" if labelsmodeover==1 else "STRICT" if labelsmodeover==2 else labels_mode_str
						lcolorclass = "UNDEFINED" if (not (labelsmodeover>=0 and labelsmodeover<=2 and defined)) else "OVERRIDEN" if (labelsmodeover!=labels_mode) else "DEFINED"
						out.append("""		<td align="center"><span class="%s">%s</span></td>""" % (colorclass,name))
						if show_copy_and_ec:
							if counters[0]!=None and counters[1]!=None and counters[2]!=None and counters[3]!=None and counters[4]!=None and counters[5]!=None and defined:
								out.append("""		<td align="right"><span class="UNDERGOAL">%s</span></td>""" % (("%s" % decimal_number_html(counters[0])) if counters[0]>0 else "&nbsp;"))
								out.append("""		<td align="right"><span class="UNDERGOAL">%s</span></td>""" % (("%s" % decimal_number_html(counters[1])) if counters[1]>0 else "&nbsp;"))
								if ec_level!=None and ec_level>0:
									out.append("""		<td align="right"><span class="WRONGFORMAT">%s</span></td>""" % (("%s" % decimal_number_html(counters[2])) if counters[2]>0 else "&nbsp;"))
									out.append("""		<td align="right"><span class="NORMAL">%s</span></td>""" % decimal_number_html(counters[3]))
								else:
									out.append("""		<td align="right"><span class="NORMAL">%s</span></td>""" % decimal_number_html(counters[2]))
									out.append("""		<td align="right"><span class="WRONGFORMAT">%s</span></td>""" % (("%s" % decimal_number_html(counters[3])) if counters[3]>0 else "&nbsp;"))
								out.append("""		<td align="right"><span class="OVERGOAL">%s</span></td>""" % (("%s" % decimal_number_html(counters[4])) if counters[4]>0 else "&nbsp;"))
								out.append("""		<td align="right"><span class="OVERGOAL">%s</span></td>""" % (("%s" % decimal_number_html(counters[5])) if counters[5]>0 else "&nbsp;"))
							else:
								out.append("""		<td align="center"></td>""")
								out.append("""		<td align="center"></td>""")
								out.append("""		<td align="center"></td>""")
								out.append("""		<td align="center"></td>""")
								out.append("""		<td align="center"></td>""")
								out.append("""		<td align="center"></td>""")
						else:
							if counters[0]!=None and counters[1]!=None and counters[2]!=None and defined:
								out.append("""		<td align="right"><span class="UNDERGOAL">%s</span></td>""" % (("%s" % decimal_number_html(counters[0])) if counters[0]>0 else "&nbsp;"))
								out.append("""		<td align="right"><span class="NORMAL">%s</span></td>""" % decimal_number_html(counters[1]))
								out.append("""		<td align="right"><span class="OVERGOAL">%s</span></td>""" % (("%s" % decimal_number_html(counters[2])) if counters[2]>0 else "&nbsp;"))
							else:
								out.append("""		<td align="center"></td>""")
								out.append("""		<td align="center"></td>""")
								out.append("""		<td align="center"></td>""")
						if canbefulfilled==3:
							out.append("""		<td align="center"><span class="%s">YES</span></td>""" % colorclass)
						elif canbefulfilled==2:
							out.append("""		<td align="center"><span class="%s">OVERLOADED</span></td>""" % wcolorclass)
						elif canbefulfilled==1:
							out.append("""		<td align="center"><span class="%s">NO SPACE</span></td>""" % wcolorclass)
						elif canbefulfilled==4:
							out.append("""		<td align="center"><span class="%s">EC KEEP ONLY</span></td>""" % wcolorclass)
						else:
							out.append("""		<td align="center"><span class="%s">NO</span></td>""" % ecolorclass)
						if show_labelmode_overrides:
							out.append("""		<td align="center"><span class="%s">%s</span></td>""" % (lcolorclass,labels_mode_over_str))
						labelsarr = []
						for labelstr,mscount in labellist_fold(labellist):
							if defined:
								if scount==0:
									msperc = 0
								else:
									msperc = (1.0 * mscount) / scount
								zerocolor = (40,80,128)
								allcolor = (40,150,224)
								perccolor = (int(zerocolor[0]+(allcolor[0]-zerocolor[0])*msperc),int(zerocolor[1]+(allcolor[1]-zerocolor[1])*msperc),int(zerocolor[2]+(allcolor[2]-zerocolor[2])*msperc))
								color = "#%02X%02X%02X" % perccolor
								labelsarr.append("""<span style="color:%s"><a style="cursor:default" title="%u/%u servers">%s</a></span>""" % (color,mscount,scount,htmlentities(labelstr)))
							else:
								labelsarr.append("""<a style="cursor:default" title="%u/%u servers">%s</a>""" % (mscount,scount,htmlentities(labelstr)))
						if ec_level!=None and ec_level>0:
							out.append("""		<td align="center"><span class="%s">EC: @%u+%u (%u+%u)</span></td>""" % (colorclass,ec_data_parts,ec_chksum_parts,ec_data_parts,ec_chksum_parts))
						else:
							out.append("""		<td align="center"><span class="%s">COPIES: %u (1+%u)</span></td>""" % (colorclass,len(labellist),len(labellist)-1))
						if defined:
							out.append("""		<td align="center">%s</td>""" % (",".join(labelsarr)))
						else:
							out.append("""		<td align="center"><span class="UNDEFINED">%s</span></td>""" % (",".join(labelsarr)))
						out.append("""		<td align="center"><span class="%s">%s</span></td>""" % (colorclass,uniqmask_to_str(uniqmask).replace("-","")))
					out.append("""	</tr>""")
				elif jsonmode:
					json_sc_dict = {}

					json_sc_dict["sclassid"] = sclassid
					json_sc_dict["sclassname"] = sclassname
					if show_export_group_and_priority:
						json_sc_dict["sclassdesc"] = sclassdesc
						json_sc_dict["priority"] = priority
						json_sc_dict["export_group"] = export_group
					else:
						json_sc_dict["sclassdesc"] = None
						json_sc_dict["priority"] = None
						json_sc_dict["export_group"] = None
					json_sc_dict["admin_only"] = True if admin_only else False
					json_sc_dict["labels_mode"] = labels_mode
					json_sc_dict["labels_mode_str"] = labels_mode_str
					json_sc_dict["arch_mode"] = arch_mode
					json_sc_dict["arch_mode_str"] = arch_mode_str
					json_sc_dict["arch_delay"] = arch_delay
					json_sc_dict["arch_delay_str"] = arch_delay_str
					json_sc_dict["arch_min_size"] = arch_min_size
					json_sc_dict["arch_min_size_human"] = humanize_number(arch_min_size," ")
					json_sc_dict["min_trashretention"] = min_trashretention
					json_sc_dict["min_trashretention_str"] = min_trashretention_str
					json_sc_dict["files"] = files
					json_sc_dict["dirs"] = dirs

					json_sc_def_dict = {}
					for defined,name,counters,ec_level,labellist,uniqmask,labelsmodeover,canbefulfilled in states:
						ec_data_parts = 0
						ec_chksum_parts = 0
						if ec_level!=None and ec_level>0:
							ec_data_parts = ec_level >> 4
							ec_chksum_parts = ec_level & 0xF
							if ec_data_parts==0:
								ec_data_parts = 8
						labels_mode_over_str = "LOOSE" if labelsmodeover==0 else "STD" if labelsmodeover==1 else "STRICT" if labelsmodeover==2 else labels_mode_str
						if defined:
							json_sc_state_dict = {}
							if show_copy_and_ec:
								json_sc_state_dict["chunks_undergoal_copy"] = counters[0]
								json_sc_state_dict["chunks_undergoal_ec"] = counters[1]
								json_sc_state_dict["chunks_exactgoal_copy"] = counters[2]
								json_sc_state_dict["chunks_exactgoal_ec"] = counters[3]
								json_sc_state_dict["chunks_overgoal_copy"] = counters[4]
								json_sc_state_dict["chunks_overgoal_ec"] = counters[5]
							else:
								json_sc_state_dict["chunks_undergoal_copy"] = counters[0]
								json_sc_state_dict["chunks_undergoal_ec"] = None
								json_sc_state_dict["chunks_exactgoal_copy"] = counters[1]
								json_sc_state_dict["chunks_exactgoal_ec"] = None
								json_sc_state_dict["chunks_overgoal_copy"] = counters[2]
								json_sc_state_dict["chunks_overgoal_ec"] = None
							json_sc_state_dict["can_be_fulfilled"] = canbefulfilled
							if canbefulfilled==3:
								json_sc_state_dict["can_be_fulfilled_str"] = "YES"
							elif canbefulfilled==2:
								json_sc_state_dict["can_be_fulfilled_str"] = "OVERLOADED"
							elif canbefulfilled==1:
								json_sc_state_dict["can_be_fulfilled_str"] = "NO SPACE"
							elif canbefulfilled==4:
								json_sc_state_dict["can_be_fulfilled_str"] = "EC KEEP ONLY"
							else:
								json_sc_state_dict["can_be_fulfilled_str"] = "NO"
							if ec_level!=None and ec_level>0:
								json_sc_state_dict["ec_data_parts"] = ec_data_parts
								json_sc_state_dict["ec_chksum_parts"] = ec_chksum_parts
								json_sc_state_dict["full_copies"] = 0
								json_sc_state_dict["redundancy_level_str"] = "EC: @%u+%u (%u+%u)" % (ec_data_parts,ec_chksum_parts,ec_data_parts,ec_chksum_parts)
							else:
								json_sc_state_dict["ec_data_parts"] = 0
								json_sc_state_dict["ec_chksum_parts"] = 0
								json_sc_state_dict["full_copies"] = len(labellist)
								json_sc_state_dict["redundancy_level_str"] = "COPIES: %u (1+%u)" % (len(labellist),len(labellist)-1)
							json_sc_labels_array = []
							for labelstr,mscount in labellist:
								json_sc_labels_dict = {}
								json_sc_labels_dict["definition"] = labelstr
								json_sc_labels_dict["matching_servers"] = mscount
								json_sc_labels_dict["available_servers"] = scount
								json_sc_labels_array.append(json_sc_labels_dict)
							json_sc_state_dict["labels_list"] = json_sc_labels_array
							json_sc_state_dict["labels_str"] = "%s" % (",".join([x for x,y in labellist_fold(labellist)]))
							json_sc_state_dict["uniqmask"] = uniqmask
							json_sc_state_dict["uniqmask_str"] = uniqmask_to_str(uniqmask)
							json_sc_state_dict["labels_mode"] = labelsmodeover
							json_sc_state_dict["labels_mode_str"] = labels_mode_over_str
							json_sc_def_dict[name.lower()] = json_sc_state_dict
					json_sc_dict["storage_modes"] = json_sc_def_dict
					json_sc_array.append(json_sc_dict)
				elif ttymode:
					if firstrow:
						firstrow = 0
					else:
						if show_copy_and_ec:
							if show_arch_min_size:
								if show_labelmode_overrides:
									if show_export_group_and_priority:
										tab.append(("---","",24))
									else:
										tab.append(("---","",22))
								else:
									tab.append(("---","",21))
							else:
								tab.append(("---","",20))
						else:
							tab.append(("---","",17))
					first = 1
					for defined,name,counters,ec_level,labellist,uniqmask,labelsmodeover,canbefulfilled in states:
						ec_data_parts = 0
						ec_chksum_parts = 0
						if ec_level!=None and ec_level>0:
							ec_data_parts = ec_level >> 4
							ec_chksum_parts = ec_level & 0xF
							if ec_data_parts==0:
								ec_data_parts = 8
						if first:
							first = 0
							if show_export_group_and_priority:
								data = [sclassid,sclassname,priority,export_group,admin_only_str,labels_mode_str,arch_mode_str,arch_delay_str,humanize_number(arch_min_size," "),min_trashretention_str,files,dirs]
							elif show_arch_min_size:
								data = [sclassid,sclassname,admin_only_str,labels_mode_str,arch_mode_str,arch_delay_str,humanize_number(arch_min_size," "),min_trashretention_str,files,dirs]
							else:
								data = [sclassid,sclassname,admin_only_str,labels_mode_str,arch_mode_str,arch_delay_str,min_trashretention_str,files,dirs]
						else:
							data = ["","","","","","","","",""]
							if show_arch_min_size:
								data.append("")
							if show_export_group_and_priority:
								data.append("")
								data.append("")
						if defined:
							data.append(name)
						else:
							data.append((name,'8'))
						if show_copy_and_ec:
							if counters[0]!=None and counters[1]!=None and counters[2]!=None and counters[3]!=None and counters[4]!=None and counters[5]!=None and defined:
								data.append((counters[0],'3') if counters[0]>0 else "-")
								data.append((counters[1],'3') if counters[1]>0 else "-")
								if ec_level!=None and ec_level>0:
									data.append((counters[2],'5') if counters[2]>0 else "-")
									data.append((counters[3],'4'))
								else:
									data.append((counters[2],'4'))
									data.append((counters[3],'5') if counters[3]>0 else "-")
								data.append((counters[4],'6') if counters[4]>0 else "-")
								data.append((counters[5],'6') if counters[5]>0 else "-")
							else:
								data.extend(["-","-","-","-","-","-"])
						else:
							if counters[0]!=None and counters[1]!=None and counters[2]!=None and defined:
								data.append((counters[0],'3') if counters[0]>0 else "-")
								data.append((counters[1],'4'))
								data.append((counters[2],'6') if counters[2]>0 else "-")
							else:
								data.extend(["-","-","-"])
						if canbefulfilled==3:
							data.append(("YES",('4' if defined else '8')))
						elif canbefulfilled==2:
							data.append(("OVERLOADED",('3' if defined else '8')))
						elif canbefulfilled==1:
							data.append(("NO SPACE",('2' if defined else '8')))
						elif canbefulfilled==4:
							data.append(("EC KEEP ONLY",('2' if defined else '8')))
						else:
							data.append(("NO",('1' if defined else '8')))
						if show_labelmode_overrides:
							labels_mode_over_str = "LOOSE" if labelsmodeover==0 else "STD" if labelsmodeover==1 else "STRICT" if labelsmodeover==2 else labels_mode_str
							if (labelsmodeover>=0 and labelsmodeover<=2 and defined):
								if (labelsmodeover!=labels_mode):
									data.append((labels_mode_over_str,'3'))
								else:
									data.append(labels_mode_over_str)
							else:
								data.append((labels_mode_over_str,'8'))
						if ec_level!=None and ec_level>0:
							rlstr = "EC: @%u+%u (%u+%u)" % (ec_data_parts,ec_chksum_parts,ec_data_parts,ec_chksum_parts)
						else:
							rlstr = "COPIES: %u (1+%u)" % (len(labellist),len(labellist)-1)
						labstr = "%s" % (",".join([x for x,y in labellist_fold(labellist)]))
						uniqstr = uniqmask_to_str(uniqmask)
						if defined:
							data.append(rlstr)
							data.append(labstr)
							data.append(uniqstr)
						else:
							data.append((rlstr,'8'))
							data.append((labstr,'8'))
							data.append((uniqstr,'8'))
						tab.append(*data)
				else:
					if show_copy_and_ec:
						if show_arch_min_size:
							if show_export_group_and_priority:
								data = ["COMMON",sclassid,sclassname,priority,export_group,admin_only_str,labels_mode_str,arch_mode_str,arch_delay_str,arch_min_size,min_trashretention_str,files,dirs]
							else:
								data = ["COMMON",sclassid,sclassname,admin_only_str,labels_mode_str,arch_mode_str,arch_delay_str,arch_min_size,min_trashretention_str,files,dirs,"",""]
						else:
							data = ["COMMON",sclassid,sclassname,admin_only_str,labels_mode_str,arch_mode_str,arch_delay_str,min_trashretention_str,files,dirs,"","",""]
					else:
						data = ["COMMON",sclassid,sclassname,admin_only_str,labels_mode_str,arch_mode_str,arch_delay_str,min_trashretention_str,files,dirs]
					tab.append(*data)
					for defined,name,counters,ec_level,labellist,uniqmask,labelsmodeover,canbefulfilled in states:
						ec_data_parts = 0
						ec_chksum_parts = 0
						if ec_level!=None and ec_level>0:
							ec_data_parts = ec_level >> 4
							ec_chksum_parts = ec_level & 0xF
							if ec_data_parts==0:
								ec_data_parts = 8
						if defined:
							data = [name,sclassid]
							if show_copy_and_ec:
								if counters[0]!=None and counters[1]!=None and counters[2]!=None and counters[3]!=None and counters[4]!=None and counters[5]!=None and defined:
									data.append(counters[0])
									data.append(counters[1])
									data.append(counters[2])
									data.append(counters[3])
									data.append(counters[4])
									data.append(counters[5])
								else:
									data.extend(["-","-","-","-","-","-"])
							else:
								if counters[0]!=None and counters[1]!=None and counters[2]!=None and defined:
									data.append(counters[0])
									data.append(counters[1])
									data.append(counters[2])
								else:
									data.extend(["-","-","-"])
							if canbefulfilled==3:
								data.append("YES")
							elif canbefulfilled==2:
								data.append("OVERLOADED")
							elif canbefulfilled==1:
								data.append("NO SPACE")
							elif canbefulfilled==4:
								data.append("EC KEEP ONLY")
							else:
								data.append("NO")
							if show_labelmode_overrides:
								data.append("LOOSE" if labelsmodeover==0 else "STD" if labelsmodeover==1 else "STRICT" if labelsmodeover==2 else labels_mode_str)
							if ec_level!=None and ec_level>0:
								rlstr = "EC: @%u+%u (%u+%u)" % (ec_data_parts,ec_chksum_parts,ec_data_parts,ec_chksum_parts)
							else:
								rlstr = "COPIES: %u (1+%u)" % (len(labellist),len(labellist)-1)
							labstr = "%s" % (",".join([x for x,y in labellist_fold(labellist)]))
							uniqstr = uniqmask_to_str(uniqmask)
							data.append(rlstr)
							data.append(labstr)
							data.append(uniqstr)
							if not show_labelmode_overrides:
								data.append("")
							tab.append(*data)

			if cgimode:
				out.append("""</table>""")
				print("\n".join(out))
			elif jsonmode:
				jcollect["dataset"]["storage_classes"] = json_sc_array
			else:
				print(myunicode(tab))
		except Exception:
			print_exception()

	if "PA" in sectionsubset and masterconn.version_at_least(4,2,0):
		try:
			if cgimode:
				out = []
				out.append("""<div class="tab_title">Override patterns</div>""")
				out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfspatterns" cellspacing="0">""")
				out.append("""	<tr>""")
				out.append("""		<th class="acid_tab_enumerate">#</th>""")
				out.append("""		<th>Pattern (GLOB)</th>""")
				out.append("""		<th>euid</th>""")
				out.append("""		<th>egid</th>""")
				out.append("""		<th>Priority</th>""")
				out.append("""		<th>Storage&nbsp;class</th>""")
				out.append("""		<th>Trash&nbsp;retention</th>""")
				out.append("""		<th>Extra&nbsp;attributes</th>""")
				out.append("""	</tr>""")
			elif jsonmode:
				json_pa_array = []
			elif ttymode:
				tab = Table("Override Patterns",7)
				tab.header("pattern","euid","egid","priority","storage class","trash retention","extra attributes")
				tab.defattr("r","r","r","r","r","r","r")
			else:
				tab = Table("override patterns",7)
			data,length = masterconn.command(CLTOMA_PATTERN_INFO,MATOCL_PATTERN_INFO)
			opatterns = []
			pos = 0
			while pos < length:
				globleng = struct.unpack_from(">B",data,pos)[0]
				pos += 1
				globname = data[pos:pos+globleng]
				globname = globname.decode('utf-8','replace')
				pos += globleng
				euid,egid,priority,omask,sclassleng = struct.unpack_from(">LLBBB",data,pos)
				pos += 11
				sclassname = data[pos:pos+sclassleng]
				sclassname = sclassname.decode('utf-8','replace')
				pos += sclassleng
				trashretention,seteattr,clreattr = struct.unpack_from(">HBB",data,pos)
				if (omask&PATTERN_OMASK_SCLASS)==0:
					sclassname = None
				if (omask&PATTERN_OMASK_TRASHRETENTION)==0:
					trashretention = None
				if (omask&PATTERN_OMASK_EATTR)==0:
					seteattr = 0
					clreattr = 0
				pos += 4
				sf = globname
				if PAorder==2:
					sf = euid
				elif PAorder==3:
					sf = egid
				elif PAorder==4:
					sf = priority
				elif PAorder==5:
					sf = sclassname
				elif PAorder==6:
					sf = trashretention
				elif PAorder==7:
					sf = seteattr|clreattr
				opatterns.append((sf,globname,euid,egid,priority,sclassname,trashretention,seteattr,clreattr))
			opatterns.sort()
			if PArev:
				opatterns.reverse()
			for sf,globname,euid,egid,priority,sclassname,trashretention,seteattr,clreattr in opatterns:
				euidstr = "ANY" if euid==PATTERN_EUGID_ANY else ("%u" % euid)
				egidstr = "ANY" if egid==PATTERN_EUGID_ANY else ("%u" % egid)
				eattrstr = eattr_to_str(seteattr,clreattr)
				if cgimode:
					if sclassname==None:
						sclassname = "-"
					out.append("""	<tr>""")
					out.append("""		<td align="right"></td>""")
					out.append("""		<td align="center">%s</td>""" % htmlentities(globname))
					out.append("""		<td align="center">%s</td>""" % euidstr)
					out.append("""		<td align="center">%s</td>""" % egidstr)
					out.append("""		<td align="center">%u</td>""" % priority)
					out.append("""		<td align="center">%s</td>""" % htmlentities(sclassname))
					if trashretention==None:
						out.append("""		<td align="center"><span class="sortkey">0 </span>-</td>""")
					else:
						out.append("""		<td align="center"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s</a></td>""" % (trashretention,hours_to_str(trashretention),timeduration_to_shortstr(trashretention*3600)))
					out.append("""		<td align="center"><span class="sortkey">%u </span>%s</td>""" % (seteattr|clreattr,eattrstr))
					out.append("""	</tr>""")
				elif jsonmode:
					json_pa_dict = {}
					json_pa_dict["pattern"] = globname
					json_pa_dict["euid"] = euidstr
					json_pa_dict["egid"] = egidstr
					json_pa_dict["priority"] = priority
					json_pa_dict["storage_class_name"] = sclassname
					json_pa_dict["trash_retention"] = trashretention
					if trashretention==None:
						json_pa_dict["trash_retention_human"] = ""
					else:
						json_pa_dict["trash_retention_human"] = hours_to_str(trashretention)
					json_pa_dict["set_eattr"] = seteattr
					json_pa_dict["clear_eattr"] = clreattr
					if seteattr|clreattr:
						json_pa_dict["eattr_str"] = eattrstr
					else:
						json_pa_dict["eattr_str"] = None
					json_pa_array.append(json_pa_dict)
				else:
					if sclassname==None:
						sclassname = "-"
					dline = [globname,euidstr,egidstr,priority,sclassname]
					if trashretention==None:
						dline.append("-")
					elif ttymode:
						dline.append(hours_to_str(trashretention))
					else:
						dline.append(trashretention)
					if eattrstr=="-":
						dline.append("-")
					elif ttymode:
						dline.append(eattrstr)
					else:
						dline.append("%02X,%02X:%s" % (seteattr,clreattr,eattrstr))
					tab.append(*dline)
			if cgimode:
				out.append("""</table>""")
				print("\n".join(out))
			elif jsonmode:
				jcollect["dataset"]["patterns"] = json_pa_array
			else:
				print(myunicode(tab))
		except Exception:
			print_exception()

	inodes = set()
	if "OF" in sectionsubset:
		try:
			sessionsdata = {}
			for ses in dataprovider.get_sessions():
				if ses.sessionid>0 and ses.sessionid < 0x80000000:
					sessionsdata[ses.sessionid]=(ses.host,ses.sortip,ses.strip,ses.info,ses.openfiles)
			if cgimode:
				out = []
				class_no_table=""
				if OFsessionid==0:
					class_no_table="no_table"
				out.append("""<form action="#"><div class="tab_title %s">Open files for client: """ % class_no_table)
				out.append("""<div class="select-fl"><select name="server" size="1" onchange="document.location.href='%s&OFsessionid='+this.options[this.selectedIndex].value">""" % createrawlink({"OFsessionid":""}))
				if OFsessionid==0:
					out.append("""<option value="0" selected="selected"> select session</option>""")
				sessions = list(sessionsdata.keys())
				sessions.sort()
				for sessionid in sessions:
					host,sortipnum,ipnum,info,openfiles = sessionsdata[sessionid]
					if OFsessionid==sessionid:
						out.append("""<option value="%s" selected="selected">%s: %s:%s (open files: ~%u)</option>""" % (sessionid,sessionid,host,info,openfiles))
					else:
						out.append("""<option value="%s">%s: %s:%s (open files: ~%u)</option>""" % (sessionid,sessionid,host,info,openfiles))
				out.append("""</select><span class="arrow"></span></div></div></form>""")

				if OFsessionid!=0:
					out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsopenfiles" cellspacing="0">""")
					out.append("""	<tr>""")
					out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
					out.append("""		<th rowspan="2">Session&nbsp;id</th>""")
					out.append("""		<th rowspan="2">Host</th>""")
					out.append("""		<th rowspan="2">IP</th>""")
					out.append("""		<th rowspan="2">Mount&nbsp;point</th>""")
					out.append("""		<th rowspan="2">Inode</th>""")
					out.append("""		<th rowspan="2">Paths</th>""")
					out.append("""	</tr>""")
			elif jsonmode:
				json_of_array = []
			elif ttymode:
				tab = Table("Open Files",5)
				tab.header("session id","ip/host","mount point","inode","path")
				tab.defattr("r","r","l","r","l")
			else:
				tab = Table("open file",5)
			if cgimode and OFsessionid==0:
				ofdata = []
			else:
				data,length = masterconn.command(CLTOMA_LIST_OPEN_FILES,MATOCL_LIST_OPEN_FILES,struct.pack(">L",OFsessionid))
				openfiles = []
				if OFsessionid==0:
					n = length//8
					for x in xrange(n):
						sessionid,inode = struct.unpack(">LL",data[x*8:x*8+8])
						openfiles.append((sessionid,inode))
						inodes.add(inode)
				else:
					n = length//4
					for x in xrange(n):
						inode = struct.unpack(">L",data[x*4:x*4+4])[0]
						openfiles.append((OFsessionid,inode))
						inodes.add(inode)
				inodepaths = resolve_inodes_paths(masterconn,inodes)
				ofdata = []
				for sessionid,inode in openfiles:
					if sessionid in sessionsdata:
						host,sortipnum,ipnum,info,openfiles = sessionsdata[sessionid]
					else:
						host = 'unknown'
						sortipnum = ''
						ipnum = ''
						info = 'unknown'
					if inode in inodepaths:
						paths = inodepaths[inode]
					else:
						paths = []
					sf = sortipnum
					if OForder==1:
						sf = sessionid
					elif OForder==2:
						sf = hostip
					elif OForder==3:
						sf = sortipnum
					elif OForder==4:
						sf = info
					elif OForder==5:
						sf = inode
					elif OForder==6:
						sf = paths
					ofdata.append((sf,sessionid,host,sortipnum,ipnum,info,inode,paths))
				ofdata.sort()
				if OFrev:
					ofdata.reverse()
			for sf,sessionid,host,sortipnum,ipnum,info,inode,paths in ofdata:
				if cgimode:
					for path in paths:
						out.append("""	<tr>""")
						out.append("""		<td align="right"></td>""")
						out.append("""		<td align="center">%u</td>""" % sessionid)
						out.append("""		<td align="left">%s</td>""" % host)
						out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (sortipnum,ipnum))
						out.append("""		<td align="left">%s</td>""" % htmlentities(info))
						out.append("""		<td align="center">%u</td>""" % inode)
						out.append("""		<td align="left">%s</td>""" % htmlentities(path))
						out.append("""	</tr>""")
				elif jsonmode:
					json_of_dict = {}
					json_of_dict["session_id"] = sessionid
					json_of_dict["hostname"] = host
					json_of_dict["ip"] = ipnum
					json_of_dict["mount_point"] = info
					json_of_dict["inode"] = inode
					json_of_dict["paths"] = paths
					json_of_array.append(json_of_dict)
				else:
					if len(paths)==0:
						dline = [sessionid,host,info,inode,"unknown"]
						tab.append(*dline)
					else:
						for path in paths:
							dline = [sessionid,host,info,inode,path]
							tab.append(*dline)
			if cgimode:
				if OFsessionid!=0:
					out.append("""</table>""")
				print("\n".join(out))
			elif jsonmode:
				jcollect["dataset"]["openfiles"] = json_of_array
			else:
				#print(openfiles)
				print(myunicode(tab))
		except Exception:
			print_exception()

	if "AL" in sectionsubset:
		try:
			sessionsdata = {}
			for ses in dataprovider.get_sessions():
				if ses.sessionid>0 and ses.sessionid < 0x80000000:
					sessionsdata[ses.sessionid]=(ses.host,ses.sortip,ses.strip,ses.info,ses.openfiles)
			if cgimode:
				if ALinode not in inodes:
					ALinode = 0
				out = []
				if len(inodes)>0:
					class_no_table = ""
					if ALinode == 0:
						class_no_table = "no_table"
					out.append("""<form action="#"><div class="tab_title %s">Acquired locks for inode: """ % class_no_table)
					out.append("""<div class="select-fl"><select name="server" size="1" onchange="document.location.href='%s&ALinode='+this.options[this.selectedIndex].value">""" % createrawlink({"ALinode":""}))
					if ALinode==0:
						out.append("""<option value="0" selected="selected"> select inode</option>""")
					inodeslist = list(inodes)
					inodeslist.sort()
					for inode in inodeslist:
						if ALinode==inode:
							out.append("""<option value="%u" selected="selected">%u</option>""" % (inode,inode))
						else:
							out.append("""<option value="%u">%u</option>""" % (inode,inode))
					out.append("""</select><span class="arrow"></span></div></div></form>""")
					if ALinode!=0:
						out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_acquiredlocks" cellspacing="0">""")
						out.append("""	<tr>""")
						out.append("""		<th rowspan="2" class="acid_tab_enumerate">#</th>""")
						out.append("""		<th rowspan="2">Session&nbsp;id</th>""")
						out.append("""		<th rowspan="2">Host</th>""")
						out.append("""		<th rowspan="2">IP</th>""")
						out.append("""		<th rowspan="2">Mount&nbsp;point</th>""")
						out.append("""		<th rowspan="2">Lock type</th>""")
						out.append("""		<th rowspan="2">Owner id</th>""")
						out.append("""		<th rowspan="2">Pid</th>""")
						out.append("""		<th rowspan="2">Start</th>""")
						out.append("""		<th rowspan="2">End</th>""")
						out.append("""		<th rowspan="2">R/W</th>""")
						out.append("""	</tr>""")
			elif jsonmode:
				json_al_array = []
			elif ttymode:
				tab = Table("Acquired Locks",10,"r")
				tab.header("inode","session id","ip/host","mount point","lock type","owner","pid","start","end","r/w")
			else:
				tab = Table("acquired locks",10)
			if cgimode and ALinode==0:
				aldata = []
			else:
				data,length = masterconn.command(CLTOMA_LIST_ACQUIRED_LOCKS,MATOCL_LIST_ACQUIRED_LOCKS,struct.pack(">L",ALinode))
				locks = []
				if ALinode==0:
					n = length//37
					for x in xrange(n):
						inode,sessionid,owner,pid,start,end,ctype = struct.unpack(">LLQLQQB",data[x*37:x*37+37])
						locks.append((inode,sessionid,owner,pid,start,end,ctype))
				else:
					n = length//33
					for x in xrange(n):
						sessionid,owner,pid,start,end,ctype = struct.unpack(">LQLQQB",data[x*33:x*33+33])
						locks.append((ALinode,sessionid,owner,pid,start,end,ctype))
				aldata = []
				for inode,sessionid,owner,pid,start,end,ctype in locks:
					if sessionid in sessionsdata:
						host,sortipnum,ipnum,info,openfiles = sessionsdata[sessionid]
					else:
						host = 'unknown'
						sortipnum = ''
						ipnum = ''
						info = 'unknown'
					if pid==0 and start==0 and end==0:
						locktype = "FLOCK"
					else:
						locktype = "POSIX"
					sf = inode
					if ALorder==1:
						sf = inode
					elif ALorder==2:
						sf = sessionid
					elif ALorder==3:
						sf = hostip
					elif ALorder==4:
						sf = sortipnum
					elif ALorder==5:
						sf = info
					elif ALorder==6:
						sf = locktype
					elif ALorder==7:
						sf = owner
					elif ALorder==8:
						sf = pid
					elif ALorder==9:
						sf = start
					elif ALorder==10:
						sf = end
					elif ALorder==11:
						sf = ctype
					aldata.append((sf,inode,sessionid,host,sortipnum,ipnum,info,locktype,owner,pid,start,end,ctype))
				aldata.sort()
				if ALrev:
					aldata.reverse()
			for sf,inode,sessionid,host,sortipnum,ipnum,info,locktype,owner,pid,start,end,ctype in aldata:
				if cgimode:
					out.append("""	<tr>""")
					out.append("""		<td align="right"></td>""")
					out.append("""		<td align="center">%u</td>""" % sessionid)
					out.append("""		<td align="left">%s</td>""" % host)
					out.append("""		<td align="center"><span class="sortkey">%s </span>%s</td>""" % (sortipnum,ipnum))
					out.append("""		<td align="left">%s</td>""" % htmlentities(info))
					out.append("""		<td align="center">%s</td>""" % locktype)
					out.append("""		<td align="right">%u</td>""" % owner)
					if pid==0 and start==0 and end==0:
						out.append("""		<td align="right">-1</td>""")
						out.append("""		<td align="right">0</td>""")
						out.append("""		<td align="right">EOF</td>""")
					else:
						out.append("""		<td align="right">%u</td>""" % pid)
						out.append("""		<td align="right">%u</td>""" % start)
						if end > 0x7FFFFFFFFFFFFFFF:
							out.append("""		<td align="right">EOF</td>""")
						else:
							out.append("""		<td align="right">%u</td>""" % end)
					out.append("""		<td align="right">%s</td>""" % ("READ(SHARED)" if ctype==1 else "WRITE(EXCLUSIVE)" if ctype==2 else "???"))
					out.append("""	</tr>""")
				elif jsonmode:
					json_al_dict = {}
					json_al_dict["inode"] = inode
					json_al_dict["session_id"] = sessionid
					json_al_dict["hostname"] = host
					json_al_dict["ip"] = ipnum
					json_al_dict["mount_point"] = info
					json_al_dict["lock_type"] = ctype
					if ctype==1:
						json_al_dict["lock_type_str"] = "READ(SHARED)"
					elif ctype==2:
						json_al_dict["lock_type_str"] = "WRITE(EXCLUSIVE)"
					else:
						json_al_dict["lock_type_str"] = "???"
					json_al_dict["owner"] = owner
					json_al_dict["pid"] = pid
					json_al_dict["start"] = start
					json_al_dict["end"] = end
					if pid==0 and start==0 and end==0:
						json_al_dict["pid_str"] = "-1"
						json_al_dict["start_str"] = "0"
						json_al_dict["end_str"] = "EOF"
					elif end > 0x7FFFFFFFFFFFFFFF:
						json_al_dict["pid_str"] = "%u" % pid
						json_al_dict["start_str"] = "%u" % start
						json_al_dict["end_str"] = "EOF"
					else:
						json_al_dict["pid_str"] = "%u" % pid
						json_al_dict["start_str"] = "%u" % start
						json_al_dict["end_str"] = "%u" % end
					json_al_array.append(json_al_dict)
				else:
					if pid==0 and start==0 and end==0:
						pid = "-1"
						start = "0"
						end = "EOF"
					elif end > 0x7FFFFFFFFFFFFFFF:
						end = "EOF"
					if ctype==1:
						ctypestr = "READ(SHARED)"
					elif ctype==2:
						ctypestr = "WRITE(EXCLUSIVE)"
					else:
						ctypestr = "???"
					dline = [inode,sessionid,host,info,locktype,owner,pid,start,end,ctypestr]
					tab.append(*dline)
			if cgimode:
				if ALinode!=0:
					out.append("""</table>""")
				print("\n".join(out))
			elif jsonmode:
				jcollect["dataset"]["locks"] = json_al_array
			else:
#				print(locks)
				print(myunicode(tab))
		except Exception:
			print_exception()

if "QU" in sectionset and masterconn!=None:
	try:
		if cgimode:
			out = []
			out.append("""<div class="tab_title">Active quotas</div>""")
			out.append("""<table class="acid_tab acid_tab_zebra_C1_C2 acid_tab_storageid_mfsquota" cellspacing="0">""")
			out.append("""	<tr>""")
			out.append("""		<th rowspan="3" class="acid_tab_enumerate">#</th>""")
#			out.append("""		<th rowspan="2"><a href="%s">path</a></th>""" % (createorderlink("QU",11)))
#			out.append("""		<th rowspan="2"><a href="%s">exceeded</a></th>""" % (createorderlink("QU",2)))
			out.append("""		<th rowspan="3">Path</th>""")
			out.append("""	<th colspan="6">Soft&nbsp;quota</th>""")
			out.append("""	<th colspan="4">Hard&nbsp;quota</th>""")
			out.append("""	<th colspan="12">Current&nbsp;values</th>""")
			out.append("""	</tr>""")
			out.append("""	<tr>""")
#			out.append("""		<th><a href="%s">time&nbsp;to&nbsp;expire</a></th>""" % (createorderlink("QU",10)))
#			out.append("""		<th><a href="%s">inodes</a></th>""" % (createorderlink("QU",11)))
#			out.append("""		<th><a href="%s">length</a></th>""" % (createorderlink("QU",12)))
#			out.append("""		<th><a href="%s">size</a></th>""" % (createorderlink("QU",13)))
#			out.append("""		<th><a href="%s">real&nbsp;size</a></th>""" % (createorderlink("QU",14)))
#			out.append("""		<th><a href="%s">inodes</a></th>""" % (createorderlink("QU",21)))
#			out.append("""		<th><a href="%s">length</a></th>""" % (createorderlink("QU",22)))
#			out.append("""		<th><a href="%s">size</a></th>""" % (createorderlink("QU",23)))
#			out.append("""		<th><a href="%s">real&nbsp;size</a></th>""" % (createorderlink("QU",24)))
#			out.append("""		<th><a href="%s">inodes</a></th>""" % (createorderlink("QU",31)))
#			out.append("""		<th><a href="%s">length</a></th>""" % (createorderlink("QU",32)))
#			out.append("""		<th><a href="%s">size</a></th>""" % (createorderlink("QU",33)))
#			out.append("""		<th><a href="%s">real&nbsp;size</a></th>""" % (createorderlink("QU",34)))
#			out.append("""		<th>exceeded</th>""")
			out.append("""		<th rowspan="2" class="wrap">grace period</th>""")
			out.append("""		<th rowspan="2" class="wrap">time to expire</th>""")
			out.append("""		<th rowspan="2">inodes</th>""")
			out.append("""		<th rowspan="2">length</th>""")
			out.append("""		<th rowspan="2">size</th>""")
			out.append("""		<th rowspan="2" class="wrap">real size</th>""")
			out.append("""		<th rowspan="2">inodes</th>""")
			out.append("""		<th rowspan="2">length</th>""")
			out.append("""		<th rowspan="2">size</th>""")
			out.append("""		<th rowspan="2" class="wrap">real size</th>""")
			out.append("""		<th colspan="3">inodes</th>""")
			out.append("""		<th colspan="3">length</th>""")
			out.append("""		<th colspan="3">size</th>""")
			out.append("""		<th colspan="3">real&nbsp;size</th>""")
			out.append("""	</tr>""")
			out.append("""	<tr>""")
			out.append("""		<th>value</th>""")
			out.append("""		<th>% soft</th>""")
			out.append("""		<th>% hard</th>""")
			out.append("""		<th>value</th>""")
			out.append("""		<th>% soft</th>""")
			out.append("""		<th>% hard</th>""")
			out.append("""		<th>value</th>""")
			out.append("""		<th>% soft</th>""")
			out.append("""		<th>% hard</th>""")
			out.append("""		<th>value</th>""")
			out.append("""		<th>% soft</th>""")
			out.append("""		<th>% hard</th>""")
			out.append("""	</tr>""")
		elif jsonmode:
			json_qu_array = []
		elif ttymode:
#			tab = Table("Active quotas",14)
#			tab.header("",("soft quota","",5),("hard quota","",4),("current values","",4))
#			tab.header("path",("---","",13))
#			tab.header("","time to expire","inodes","length","size","real size","inodes","length","size","real size","inodes","length","size","real size")
#			tab.defattr("l","r","r","r","r","r","r","r","r","r","r","r","r","r")
			tab = Table("Active quotas",23)
			tab.header("",("soft quota","",6),("hard quota","",4),("current values","",12))
			tab.header("",("---","",22))
			tab.header("path","","","","","","","","","","",("inodes","",3),("length","",3),("size","",3),("real size","",3))
			tab.header("","grace period","time to expire","inodes","length","size","real size","inodes","length","size","real size",("---","",12))
			tab.header("","","","","","","","","","","","value","% soft","% hard","value","% soft","% hard","value","% soft","% hard","value","% soft","% hard")
			tab.defattr("l","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r","r")
		else:
			tab = Table("active quotas",16)
		if masterconn.has_feature(FEATURE_DEFAULT_GRACEPERIOD):
			data,length = masterconn.command(CLTOMA_QUOTA_INFO,MATOCL_QUOTA_INFO,struct.pack(">B",1))
		else:
			data,length = masterconn.command(CLTOMA_QUOTA_INFO,MATOCL_QUOTA_INFO)
		if length>=4 and masterconn.version_at_least(1,7,0):
			quotas = []
			maxperc = 0.0
			pos = 0
			while pos<length:
				inode,pleng = struct.unpack(">LL",data[pos:pos+8])
				pos+=8
				path = data[pos:pos+pleng]
				path = path.decode('utf-8','replace')
				pos+=pleng
				if masterconn.version_at_least(3,0,9):
					graceperiod,exceeded,qflags,timetoblock = struct.unpack(">LBBL",data[pos:pos+10])
					pos+=10
				else:
					exceeded,qflags,timetoblock = struct.unpack(">BBL",data[pos:pos+6])
					pos+=6
					graceperiod = 0
				sinodes,slength,ssize,srealsize = struct.unpack(">LQQQ",data[pos:pos+28])
				pos+=28
				hinodes,hlength,hsize,hrealsize = struct.unpack(">LQQQ",data[pos:pos+28])
				pos+=28
				cinodes,clength,csize,crealsize = struct.unpack(">LQQQ",data[pos:pos+28])
				pos+=28
				if (qflags&1) and sinodes>0:
					perc = 100.0*cinodes/sinodes
					if perc>maxperc:
						maxperc = perc
				if (qflags&2) and slength>0:
					perc = 100.0*clength/slength
					if perc>maxperc:
						maxperc = perc
				if (qflags&4) and ssize>0:
					perc = 100.0*csize/ssize
					if perc>maxperc:
						maxperc = perc
				if (qflags&8) and srealsize>0:
					perc = 100.0*crealsize/srealsize
					if perc>maxperc:
						maxperc = perc
				if (qflags&16) and hinodes>0:
					perc = 100.0*cinodes/hinodes
					if perc>maxperc:
						maxperc = perc
				if (qflags&32) and hlength>0:
					perc = 100.0*clength/hlength
					if perc>maxperc:
						maxperc = perc
				if (qflags&64) and hsize>0:
					perc = 100.0*csize/hsize
					if perc>maxperc:
						maxperc = perc
				if (qflags&128) and hrealsize>0:
					perc = 100.0*crealsize/hrealsize
					if perc>maxperc:
						maxperc = perc
				sf = path
				if QUorder==1:
					sf = path
				elif QUorder==2:
					sf = exceeded
				elif QUorder==9:
					sf = graceperiod
				elif QUorder==10:
					sf = timetoblock
				elif QUorder==11:
					sf = sinodes
				elif QUorder==12:
					sf = slength
				elif QUorder==13:
					sf = ssize
				elif QUorder==14:
					sf = srealsize
				elif QUorder==21:
					sf = hinodes
				elif QUorder==22:
					sf = hlength
				elif QUorder==23:
					sf = hsize
				elif QUorder==24:
					sf = hrealsize
				elif QUorder==31:
					sf = cinodes
				elif QUorder==32:
					sf = clength
				elif QUorder==33:
					sf = csize
				elif QUorder==34:
					sf = crealsize
				elif QUorder==41:
					sf = (-1,0) if (qflags&1)==0 else (1,0) if sinodes==0 else (0,1.0*cinodes/sinodes)
				elif QUorder==42:
					sf = (-1,0) if (qflags&2)==0 else (1,0) if slength==0 else (0,1.0*clength/slength)
				elif QUorder==43:
					sf = (-1,0) if (qflags&4)==0 else (1,0) if ssize==0 else (0,1.0*csize/ssize)
				elif QUorder==44:
					sf = (-1,0) if (qflags&8)==0 else (1,0) if srealsize==0 else (0,1.0*crealsize/srealsize)
				elif QUorder==51:
					sf = (-1,0) if (qflags&16)==0 else (1,0) if hinodes==0 else (0,1.0*cinodes/hinodes)
				elif QUorder==52:
					sf = (-1,0) if (qflags&32)==0 else (1,0) if hlength==0 else (0,1.0*clength/hlength)
				elif QUorder==53:
					sf = (-1,0) if (qflags&64)==0 else (1,0) if hsize==0 else (0,1.0*csize/hsize)
				elif QUorder==54:
					sf = (-1,0) if (qflags&128)==0 else (1,0) if hrealsize==0 else (0,1.0*crealsize/hrealsize)
				quotas.append((sf,path,exceeded,qflags,graceperiod,timetoblock,sinodes,slength,ssize,srealsize,hinodes,hlength,hsize,hrealsize,cinodes,clength,csize,crealsize))
			quotas.sort()
			if QUrev:
				quotas.reverse()
			maxperc += 0.01
			for sf,path,exceeded,qflags,graceperiod,timetoblock,sinodes,slength,ssize,srealsize,hinodes,hlength,hsize,hrealsize,cinodes,clength,csize,crealsize in quotas:
				graceperiod_default = 0
				if masterconn.has_feature(FEATURE_DEFAULT_GRACEPERIOD):
					if exceeded & 2:
						exceeded &= 1
						graceperiod_default = 1
				if cgimode:
					out.append("""	<tr>""")
					out.append("""		<td align="right"></td>""")
					out.append("""		<td align="left">%s</td>""" % htmlentities(path))
	#				out.append("""		<td align="center">%s</td>""" % ("yes" if exceeded else "no"))
					if graceperiod>0:
						out.append("""		<td align="center"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s%s</a></td>""" % (graceperiod,timeduration_to_fullstr(graceperiod),timeduration_to_shortstr(graceperiod)," (default)" if graceperiod_default else ""))
					else:
						out.append("""		<td align="center"><span class="sortkey">0 </span>default</td>""")
					if timetoblock<0xFFFFFFFF:
						if timetoblock>0:
	#						days,rest = divmod(timetoblock,86400)
	#						hours,rest = divmod(rest,3600)
	#						min,sec = divmod(rest,60)
	#						if days>0:
	#							tbstr = "%ud,&nbsp;%uh&nbsp;%um&nbsp;%us" % (days,hours,min,sec)
	#						elif hours>0:
	#							tbstr = "%uh&nbsp;%um&nbsp;%us" % (hours,min,sec)
	#						elif min>0:
	#							tbstr = "%um&nbsp;%us" % (min,sec)
	#						else:
	#							tbstr = "%us" % sec
							out.append("""		<td align="center"><span class="SEXCEEDED"><span class="sortkey">%u </span><a style="cursor:default" title="%s">%s</a></span></td>""" % (timetoblock,timeduration_to_fullstr(timetoblock),timeduration_to_shortstr(timetoblock)))
						else:
							out.append("""		<td align="center"><span class="EXCEEDED"><span class="sortkey">0 </span>expired</span></td>""")
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&1:
						out.append("""		<td align="right"><span>%s</span></td>""" % decimal_number_html(sinodes))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&2:
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (slength,decimal_number(slength),humanize_number(slength,"&nbsp;")))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&4:
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (ssize,decimal_number(ssize),humanize_number(ssize,"&nbsp;")))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&8:
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (srealsize,decimal_number(srealsize),humanize_number(srealsize,"&nbsp;")))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&16:
						out.append("""		<td align="right"><span>%s</span></td>""" % decimal_number_html(hinodes))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&32:
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (hlength,decimal_number(hlength),humanize_number(hlength,"&nbsp;")))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&64:
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (hsize,decimal_number(hsize),humanize_number(hsize,"&nbsp;")))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&128:
						out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B"><span>%s</span></a></td>""" % (hrealsize,decimal_number(hrealsize),humanize_number(hrealsize,"&nbsp;")))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					out.append("""		<td align="right">%s</td>""" % decimal_number_html(cinodes))
					if qflags&1:
						if sinodes>0:
							if sinodes>=cinodes:
								cl="NOTEXCEEDED"
							elif timetoblock>0:
								cl="SEXCEEDED"
							else:
								cl="EXCEEDED"
							out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (cl,(100.0*cinodes)/sinodes))
						else:
							out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&16:
						if hinodes>0:
							if hinodes>cinodes:
								cl="NOTEXCEEDED"
							else:
								cl="EXCEEDED"
							out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (cl,(100.0*cinodes)/hinodes))
						else:
							out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td>""" % (clength,decimal_number(clength),humanize_number(clength,"&nbsp;")))
					if qflags&2:
						if slength>0:
							if slength>=clength:
								cl="NOTEXCEEDED"
							elif timetoblock>0:
								cl="SEXCEEDED"
							else:
								cl="EXCEEDED"
							out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (cl,(100.0*clength)/slength))
						else:
							out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&32:
						if hlength>0:
							if hlength>clength:
								cl="NOTEXCEEDED"
							else:
								cl="EXCEEDED"
							out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (cl,(100.0*clength)/hlength))
						else:
							out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td>""" % (csize,decimal_number(csize),humanize_number(csize,"&nbsp;")))
					if qflags&4:
						if ssize>0:
							if ssize>=csize:
								cl="NOTEXCEEDED"
							elif timetoblock>0:
								cl="SEXCEEDED"
							else:
								cl="EXCEEDED"
							out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (cl,(100.0*csize)/ssize))
						else:
							out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&64:
						if hsize>0:
							if hsize>csize:
								cl="NOTEXCEEDED"
							else:
								cl="EXCEEDED"
							out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (cl,(100.0*csize)/hsize))
						else:
							out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					out.append("""		<td align="right"><span class="sortkey">%u </span><a style="cursor:default" title="%s B">%s</a></td>""" % (crealsize,decimal_number(crealsize),humanize_number(crealsize,"&nbsp;")))
					if qflags&8:
						if srealsize>0:
							if srealsize>=crealsize:
								cl="NOTEXCEEDED"
							elif timetoblock>0:
								cl="SEXCEEDED"
							else:
								cl="EXCEEDED"
							out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (cl,(100.0*crealsize)/srealsize))
						else:
							out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					if qflags&128:
						if hrealsize>0:
							if hrealsize>crealsize:
								cl="NOTEXCEEDED"
							else:
								cl="EXCEEDED"
							out.append("""		<td align="right"><span class="%s">%.2f</span></td>""" % (cl,(100.0*crealsize)/hrealsize))
						else:
							out.append("""		<td align="right"><span class="sortkey">%.2f </span><span class="EXCEEDED">inf</span></td>""" % (maxperc))
					else:
						out.append("""		<td align="center"><span class="sortkey">-1 </span>-</td>""")
					out.append("""	</tr>""")
				elif jsonmode:
					json_qu_dict = {}
					json_qu_dict["path"] = path
					json_qu_dict["exceeded"] = True if exceeded else False
					json_qu_dict["grace_period_default"] = True if graceperiod_default else False
					json_qu_dict["grace_period"] = graceperiod
					if graceperiod>0:
						json_qu_dict["grace_period_str"] = timeduration_to_shortstr(graceperiod) + (" (default)" if graceperiod_default else "")
					else:
						json_qu_dict["grace_period_str"] = "default"
					if timetoblock<0xFFFFFFFF:
						json_qu_dict["time_to_expire"] = timetoblock
						if timetoblock>0:
							json_qu_dict["time_to_expire_str"] = timeduration_to_shortstr(timetoblock)
						else:
							json_qu_dict["time_to_expire_str"] = "expired"
					else:
						json_qu_dict["time_to_expire"] = None
						json_qu_dict["time_to_expire_str"] = ""
					json_qu_dict["flags"] = qflags
					json_qu_dict["soft_quota_inodes"] = sinodes if qflags&1 else None
					json_qu_dict["soft_quota_length"] = slength if qflags&2 else None
					json_qu_dict["soft_quota_length_human"] = humanize_number(slength," ") if qflags&2 else None
					json_qu_dict["soft_quota_size"] = ssize if qflags&4 else None
					json_qu_dict["soft_quota_size_human"] = humanize_number(ssize," ") if qflags&4 else None
					json_qu_dict["soft_quota_realsize"] = srealsize if qflags&8 else None
					json_qu_dict["soft_quota_realsize_human"] = humanize_number(srealsize," ") if qflags&8 else None
					json_qu_dict["hard_quota_inodes"] = hinodes if qflags&16 else None
					json_qu_dict["hard_quota_length"] = hlength if qflags&32 else None
					json_qu_dict["hard_quota_length_human"] = humanize_number(hlength," ") if qflags&32 else None
					json_qu_dict["hard_quota_size"] = hsize if qflags&64 else None
					json_qu_dict["hard_quota_size_human"] = humanize_number(hsize," ") if qflags&64 else None
					json_qu_dict["hard_quota_realsize"] = hrealsize if qflags&128 else None
					json_qu_dict["hard_quota_realsize_human"] = humanize_number(hrealsize," ") if qflags&128 else None
					json_qu_dict["current_quota_inodes"] = cinodes
					json_qu_dict["current_quota_length"] = clength
					json_qu_dict["current_quota_length_human"] = humanize_number(clength," ")
					json_qu_dict["current_quota_size"] = csize
					json_qu_dict["current_quota_size_human"] = humanize_number(csize," ")
					json_qu_dict["current_quota_realsize"] = crealsize
					json_qu_dict["current_quota_realsize_human"] = humanize_number(crealsize," ")
					json_qu_array.append(json_qu_dict)
				elif ttymode:
					dline = [path] #,"yes" if exceeded else "no"]
					if graceperiod>0:
						dline.append(timeduration_to_shortstr(graceperiod) + (" (default)" if graceperiod_default else ""))
					else:
						dline.append("default")
					if timetoblock<0xFFFFFFFF:
						if timetoblock>0:
							dline.append((timeduration_to_shortstr(timetoblock),"2"))
						else:
							dline.append(("expired","1"))
					else:
						dline.append("-")
					if qflags&1:
						dline.append(sinodes)
					else:
						dline.append("-")
					if qflags&2:
						dline.append(humanize_number(slength," "))
					else:
						dline.append("-")
					if qflags&4:
						dline.append(humanize_number(ssize," "))
					else:
						dline.append("-")
					if qflags&8:
						dline.append(humanize_number(srealsize," "))
					else:
						dline.append("-")
					if qflags&16:
						dline.append(hinodes)
					else:
						dline.append("-")
					if qflags&32:
						dline.append(humanize_number(hlength," "))
					else:
						dline.append("-")
					if qflags&64:
						dline.append(humanize_number(hsize," "))
					else:
						dline.append("-")
					if qflags&128:
						dline.append(humanize_number(hrealsize," "))
					else:
						dline.append("-")
					dline.append(cinodes)
					if qflags&1:
						if sinodes>0:
							dline.append(("%.2f" % ((100.0*cinodes)/sinodes),"4" if sinodes>=cinodes else "2" if timetoblock>0 else "1"))
						else:
							dline.append(("inf","1"))
					else:
						dline.append("-")
					if qflags&16:
						if hinodes>0:
							dline.append(("%.2f" % ((100.0*cinodes)/hinodes),"4" if hinodes>cinodes else "1"))
						else:
							dline.append(("inf","1"))
					else:
						dline.append("-")
					dline.append(humanize_number(clength," "))
					if qflags&2:
						if slength>0:
							dline.append(("%.2f" % ((100.0*clength)/slength),"4" if slength>=clength else "2" if timetoblock>0 else "1"))
						else:
							dline.append(("inf","1"))
					else:
						dline.append("-")
					if qflags&32:
						if hlength>0:
							dline.append(("%.2f" % ((100.0*clength)/hlength),"4" if hlength>clength else "1"))
						else:
							dline.append(("inf","1"))
					else:
						dline.append("-")
					dline.append(humanize_number(csize," "))
					if qflags&4:
						if ssize>0:
							dline.append(("%.2f" % ((100.0*csize)/ssize),"4" if ssize>=csize else "2" if timetoblock>0 else "1"))
						else:
							dline.append(("inf","1"))
					else:
						dline.append("-")
					if qflags&64:
						if hsize>0:
							dline.append(("%.2f" % ((100.0*csize)/hsize),"4" if hsize>csize else "1"))
						else:
							dline.append(("inf","1"))
					else:
						dline.append("-")
					dline.append(humanize_number(crealsize," "))
					if qflags&8:
						if srealsize>0:
							dline.append(("%.2f" % ((100.0*crealsize)/srealsize),"4" if srealsize>=crealsize else "2" if timetoblock>0 else "1"))
						else:
							dline.append(("inf","1"))
					else:
						dline.append("-")
					if qflags&128:
						if hrealsize>0:
							dline.append(("%.2f" % ((100.0*crealsize)/hrealsize),"4" if hrealsize>crealsize else "1"))
						else:
							dline.append(("inf","1"))
					else:
						dline.append("-")
					tab.append(*dline)
				else:
					dline = [path,"yes" if exceeded else "no"]
					if graceperiod>0:
						dline.append(graceperiod)
					else:
						dline.append("default")
					if timetoblock<0xFFFFFFFF:
						if timetoblock>0:
							dline.append(timetoblock)
						else:
							dline.append("expired")
					else:
						dline.append("-")
					dline.append(sinodes if qflags&1 else "-")
					dline.append(slength if qflags&2 else "-")
					dline.append(ssize if qflags&4 else "-")
					dline.append(srealsize if qflags&8 else "-")
					dline.append(hinodes if qflags&16 else "-")
					dline.append(hlength if qflags&32 else "-")
					dline.append(hsize if qflags&64 else "-")
					dline.append(hrealsize if qflags&128 else "-")
					dline.extend((cinodes,clength,csize,crealsize))
					tab.append(*dline)
		if cgimode:
			out.append("""</table>""")
			print("\n".join(out))
		elif jsonmode:
			jcollect["dataset"]["quotas"] = json_qu_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

if cgimode and not ajax_request:
	print("""</div><!-- end of container-ajax -->""")
	# content from now on (i.e., charts) is not subject of the full DOM ajax update

# charts are ajax-updated individually, without resending the page content
if "MC" in sectionset and masterconn!=None and not ajax_request:
	out = []
	try:
		if cgimode:
			icharts = (
				(100,0,'cpu','cpu usage (percent)','<b>cpu usage</b>, sys: BOXGR2A user: BOXGR2B','sys: BOXGR2A user: BOXGR2B'),
				(101,0,'memory','memory usage (if available) rss + virt)','<b>memory usage</b> (if available), rss: BOXGR2A + virt: BOXGR2B','rss: BOXGR2A + virt: BOXGR2B'),
				(102,0,'space','raw disk space usage (used/total)','<b>raw disk space usage</b>, used: BOXGR2A total: BOXGR2B','used: BOXGR2A total: BOXGR2B'),
				(108,0,'objects','number of meta objects (others/files)','<b>number of meta objects</b>, others: BOXGR2A files: BOXGR2B','others: BOXGR2A files: BOXGR2B'),
				(109,0,'chunks','number of chunks (ec8/ec4/copy)','<b>number of chunks</b>, ec8: BOXGR3A ec4: BOXGR3B copy: BOXGR3C','ec8: BOXGR3A ec4: BOXGR3B copy: BOXGR3C'),
				(110,0,'regunder','number of regular chunks in danger','<b>number of regular chunks</b>, endangered: BOXGR2A undergoal: BOXGR2B','endangered: BOXGR2A undergoal: BOXGR2B'),
				(111,0,'allunder','number of all chunks in danger','<b>number of all chunks</b>, endangered: BOXGR2A undergoal: BOXGR2B ','endangered: BOXGR2A undergoal: BOXGR2B'),
				(112,0,'cservers','number of chunk servers','<b>number of chunk servers</b>, disconnected: BOXGR3A disconnected in maintenance: BOXGR3B working: BOXGR3C','disconnected: BOXGR3A disconnected in maintenance: BOXGR3B working: BOXGR3C'),
				(67,0,'udiff','space usage difference','<b>difference in space usage between the most and the least used chunk server</b>',''),
				(63,0,'delay','master max delay in seconds','<b>master max delay in seconds</b>',''),
				(103,0,'dels','chunk deletions per minute','<b>chunk deletions</b> per minute, unsuccessful: BOXGR2A successful: BOXGR2B','unsuccessful: BOXGR2A successful: BOXGR2B'),
				(104,0,'repl','chunk replications per minute','<b>chunk replications</b> per minute, unsuccessful: BOXGR2A successful: BOXGR2B','unsuccessful: BOXGR2A successful: BOXGR2B'),
				(105,0,'creat','chunk creations per minute','<b>chunk creations</b> per minute, unsuccessful: BOXGR2A successful: BOXGR2B','unsuccessful: BOXGR2A successful: BOXGR2B'),
				(106,0,'change','chunk internal operations per minute','<b>chunk internal operations</b> per minute, unsuccessful: BOXGR2A successful: BOXGR2B','unsuccessful: BOXGR2A successful: BOXGR2B'),
				(107,0,'split','chunk local split operations per minute','<b>chunk local split operations</b> per minute, unsuccessful: BOXGR2A successful: BOXGR2B','unsuccessful: BOXGR2A successful: BOXGR2B'),
				(68,0,'mountbytrcvd','traffic from cluster, data only (bytes per second)','',''),
				(69,0,'mountbytsent','traffic to cluster, data only (bytes per second)','',''),
				(49,0,'bread','traffic from cluster, data+overhead (bytes per second)','',''),
				(50,0,'bwrite','traffic to cluster, data+overhead (bytes per second)','',''),
				(21,0,'prcvd','packets received (per second)','',''),
				(22,0,'psent','packets sent (per second)','',''),
				(23,0,'brcvd','bits received (per second)','',''),
				(24,0,'bsent','bits sent (per second)','',''),
				(4,1,'statfs','statfs operations (per minute)','',''),
				(5,1,'getattr','getattr operations (per minute)','',''),
				(6,1,'setattr','setattr operations (per minute)','',''),
				(7,1,'lookup','lookup operations (per minute)','',''),
				(8,1,'mkdir','mkdir operations (per minute)','',''),
				(9,1,'rmdir','rmdir operations (per minute)','',''),
				(10,1,'symlink','symlink operations (per minute)','',''),
				(11,1,'readlink','readlink operations (per minute)','',''),
				(12,1,'mknod','mknod operations (per minute)','',''),
				(13,1,'unlink','unlink operations (per minute)','',''),
				(14,1,'rename','rename operations (per minute)','',''),
				(15,1,'link','link operations (per minute)','',''),
				(16,1,'readdir','readdir operations (per minute)','',''),
				(17,1,'open','open operations (per minute)','',''),
				(51,1,'read','read operations (per minute)','',''),
				(52,1,'write','write operations (per minute)','',''),
				(53,1,'fsync','fsync operations (per minute)','',''),
				(56,1,'truncate','truncate operations (per minute)','',''),
				(61,1,'create','file create operations (per minute)','',''),
				(54,1,'lock','file lock operations (per minute)','',''),
				(55,1,'snapshot','snapshot operations (per minute)','',''),
				(57,1,'getxattr','getxattr operations (per minute)','',''),
				(58,1,'setxattr','setxattr operations (per minute)','',''),
				(59,1,'getfacl','getfacl operations (per minute)','',''),
				(60,1,'setfacl','setfacl operations (per minute)','',''),
				(62,1,'meta','all meta data operations (per minute)','<b>all meta operations</b> - sclass, trashretention, eattr, etc. (per minute)','')
			)

			charts = ([],[])
			for id,sheet,oname,desc,fdesc,sdesc in icharts:
				if not masterconn.is_pro() and ( id==63 ):
					continue
				if fdesc=='':
					fdesc = '<b>' + desc.replace(' (per', '</b> (per').replace(' (bytes per', '</b> (bytes per')
					if not '</b>' in fdesc:
						fdesc = fdesc + '</b>'
				fdeschtml = fdesc.replace('BOXGR1A','<span class="CBOX GR1A"></span>').replace('BOXGR2A','<span class="CBOX GR2A"></span>').replace('BOXGR2B','<span class="CBOX GR2B"></span>').replace('BOXGR3A','<span class="CBOX GR3A"></span>').replace('BOXGR3B','<span class="CBOX GR3B"></span>').replace('BOXGR3C','<span class="CBOX GR3C"></span>')
				sdeschtml = sdesc.replace('BOXGR1A','<span class="CBOX GR1A"></span>').replace('BOXGR2A','<span class="CBOX GR2A"></span>').replace('BOXGR2B','<span class="CBOX GR2B"></span>').replace('BOXGR3A','<span class="CBOX GR3A"></span>').replace('BOXGR3B','<span class="CBOX GR3B"></span>').replace('BOXGR3C','<span class="CBOX GR3C"></span>')
				charts[sheet].append((id,oname,desc,fdeschtml,sdeschtml))
			if MCdata=="" and leaderfound:
				MCdata="%s:%u:%u" % (masterconn.host,masterconn.port,10 if masterconn.version_at_least(4,31,0) else 11)
			servers = []
			entrystr = []
			entrydesc = {}
			if len(masterlistver)>0:
				masterlistver.sort()
				for sheet in [0,1]:
					for id,oname,desc,fdesc,sdesc in charts[sheet]:
						name = oname.replace(":","")
						entrystr.append(name)
						entrydesc[name] = desc
				for strip,port,version in masterlistver:
					if version>=(2,0,15):
						chmode = 10 if version>=(4,31,0) else 11
						name = "%s:%u" % (strip,port)
						namearg_res = "%s:%u" % (name,chmode)
						namearg_ops = "%s:%u" % (name,chmode+10)
						hostx = resolve(strip)
						if hostx==UNRESOLVED:
							host = "Server"
						else:
							host = hostx
						entrystr.append(namearg_res)
						entrystr.append(namearg_ops)
						entrydesc[namearg_res] = "%s: %s - resources %s" % (host, name," (leader)" if (leaderfound and strip==masterconn.host) else "")
						entrydesc[namearg_ops] = "%s: %s - operations %s" % (host,name," (leader)" if (leaderfound and strip==masterconn.host) else "")
						# entrydesc[namearg_res] = "Server: %s%s%s / resources" % (name,host," (leader)" if (leaderfound and strip==masterconn.host) else "")
						# entrydesc[namearg_ops] = "Server: %s%s%s / operations" % (name,host," (leader)" if (leaderfound and strip==masterconn.host) else "")
						servers.append((strip,port,"ma_"+name.replace(".","_").replace(":","_"),"Server: <b>%s</b>" % (name),chmode))

			mchtmp = MCdata.split(":")
			if len(mchtmp)==2:
				mchtmp = (mchtmp[0],mchtmp[1],0)
			if len(mchtmp)==3:
				mahost = mchtmp[0]
				maport = mchtmp[1]
				mamode = int(mchtmp[2])
				if mamode>=20:
					mamode -= 20
					masheet = 1
				elif mamode>=10:
					mamode -= 10
					masheet = 0
				else:
					mamode = 1
					masheet = 0

				out.append("""<div class="tab_title">Master server charts</div>""")
				out.append("""<form action="#"><table class="FR" cellspacing="0" cellpadding="0">""")
				out.append("""	<tr>""")
				out.append("""		<th class="knob-cell chart-range">""")
				out.append(html_knob_selector_chart_range('ma_main'))
				out.append("""		</th>""")
				out.append("""		<th class="chart-select">""")
				out.append("""			<div class="select-fl"><select class="chart-select" name="madata" size="1" onchange="ma_change_data(this.selectedIndex,this.options[this.selectedIndex].value)">""")
				if MCdata not in entrystr:
					out.append("""				<option value="" selected="selected"> data type or server</option>""")
				for estr in entrystr:
					if estr==MCdata:
						out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
					else:
						out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
				out.append("""			</select><span class="arrow"></span></div>""")
				out.append("""		</th>""")
				out.append("""	</tr>""")

				if MCdata in entrystr:
					for id,name,desc,fdesc,sdesc in charts[masheet]:
						out.append("""	<tr class="C2 CHART">""")
						out.append("""		<td id="ma_%s_p" colspan="3" style="height:124px;" valign="middle">""" % name)
						out.append("""			<div class="CHARTJSW">""")
						out.append("""				<div id="ma_%s_c" class="CHARTJSC">""" % name)
						out.append("""					<span class="CAPTIONJS">%s</span>""" % fdesc)
						out.append("""				</div>""")
						out.append("""			</div>""")
						out.append("""			<div id="ma_%s_l" class="CHARTJSL"></div>""" % name)
						out.append("""			<div id="ma_%s_r" class="CHARTJSR"></div>""" % name)
						out.append("""		</td>""")
						out.append("""	</tr>""")
				out.append("""</table></form>""")

				if MCdata in entrystr:
					out.append("""<div class="tab_title">Master server charts (comparison)</div>""")
					out.append("""<form action="#"><table class="FR" cellspacing="0" cellpadding="0">""")
					for i in range(2):
						out.append("""	<tr>""")
						if i==0:
							out.append("""		<th class="knob-cell chart-cmp-range">""")
							out.append(html_knob_selector_chart_range('ma_cmp')) 
						else:
							out.append("""		<th style="text-align: right;vertical-align:middle;border:none;">""")
							out.append("""versus:""")
						out.append("""		</th>""")
						out.append("""		<th class="chart-cmp-select">""")
						out.append("""			<div class="select-fl"><select class="chart-select" id="machart%u_select" name="machart%u" size="1" onchange="ma_change_type(%u,this.options[this.selectedIndex].value)">""" % (i,i,i))
						no = 0
						for id,name,desc,fdesc,sdesc in charts[0]:
							out.append("""				<option value="%u">%s</option>""" % (no,desc))
							no += 1
						for id,name,desc,fdesc,sdesc in charts[1]:
							out.append("""				<option value="%u">%s</option>""" % (no,desc))
							no += 1
						out.append("""			</select><span class="arrow"></span></div>""")	
						out.append("""		</th>""")
						out.append("""	</tr>""")
						out.append("""	<tr class="C2 CHART">""")
						out.append("""		<td id="ma_%u_p" colspan="2" style="height: 124px; border-top: none;" valign="middle">""" % i)
						out.append("""			<div class="CHARTJSW">""")
						out.append("""				<div id="ma_%u_c" class="CHARTJSC">""" % i)
						out.append("""					<span id="ma_%u_d" class="CAPTIONJS">%s</span>""" % (i,charts[0][0][3]))
						out.append("""				</div>""")
						out.append("""			</div>""")
						out.append("""			<div id="ma_%u_l" class="CHARTJSL"></div>""" % i)
						out.append("""			<div id="ma_%u_r" class="CHARTJSR"></div>""" % i)
						out.append("""		</td>""")
						out.append("""	</tr>""")
					out.append("""</table></form>""")

					out.append("""<script type="text/javascript">""")
					out.append("""<!--//--><![CDATA[//><!--""")
					out.append("""	var ma_vids = [%s];""" % ",".join(map(repr,[ x[0] for x in charts[masheet] ])))
					out.append("""	var ma_inames = [%s];""" % ",".join(map(repr,[ x[1] for x in charts[masheet] ])))
					out.append("""	var ma_idesc = [%s];""" % ",".join(map(repr,[ x[3] for x in charts[masheet] ])))
					out.append("""	var ma_vids_cmp = [%s];""" % ",".join(map(repr,[ x[0] for x in (charts[0]+charts[1]) ])))
					out.append("""	var ma_idesc_cmp = [%s];""" % ",".join(map(repr,[ x[3] for x in (charts[0]+charts[1]) ])))
					out.append("""	var ma_hosts = [%s];""" % ",".join(map(repr,[ x[0] for x in servers ])))
					out.append("""	var ma_ports = [%s];""" % ",".join(map(repr,[ x[1] for x in servers ])))
					out.append("""	var ma_modes = [%s];""" % ",".join(map(repr,[ x[4]-10 for x in servers ])))
					out.append("""	var ma_host = "%s";""" % mahost)
					out.append("""	var ma_port = "%s";""" % maport)
					out.append("""	var ma_mode = %u;""" % mamode)
					out.append("""	var ma_valid = 1;""")
					out.append("""	var ma_base_href = "%s";""" % createrawlink({"MCdata":""}))
					out.append("""//--><!]]>""")
					out.append("""</script>""")
				else:
					out.append("""<script type="text/javascript">""")
					out.append("""<!--//--><![CDATA[//><!--""")
					out.append("""	var ma_valid = 0;""")
					out.append("""	var ma_base_href = "%s";""" % createrawlink({"MCdata":""}))
					out.append("""//--><!]]>""")
					out.append("""</script>""")
				out.append("""<script type="text/javascript">
<!--//--><![CDATA[//><!--
//#1 - comparison charts
function ma_create_charts(show_loading=true) {
	ma_charttab = [];
	var i;
	if (ma_valid) {
		for (i=0 ; i<ma_inames.length ; i++) {
			ma_charttab.push(new AcidChartWrapper("ma_"+ma_inames[i]+"_","ma_main",ma_host,ma_port,ma_mode,ma_vids[i],show_loading));
		}
		if (ma_chartcmp[0]) document.getElementById('machart0_select').value = ma_vids_cmp.indexOf(ma_chartcmp[0].id);
		if (ma_chartcmp[1]) document.getElementById('machart1_select').value = ma_vids_cmp.indexOf(ma_chartcmp[1].id);
		ma_chartcmp[0] = new AcidChartWrapper("ma_0_","ma_cmp",ma_host,ma_port,ma_mode,(ma_chartcmp[0]) ? ma_chartcmp[0].id : ma_vids_cmp[0],show_loading);
		ma_chartcmp[1] = new AcidChartWrapper("ma_1_","ma_cmp",ma_host,ma_port,ma_mode,(ma_chartcmp[1]) ? ma_chartcmp[1].id : ma_vids_cmp[0],show_loading);
	}
}

// main charts data update
function ma_change_data(indx, mcdata) {
	var sindx,i;
	if (ma_valid) {
		sindx = indx - ma_inames.length;
		if (sindx >= 0 && sindx < ma_modes.length) {
			for (i=0 ; i<ma_vids.length ; i++) {
				ma_charttab[i].set_host_port_id(ma_hosts[sindx],ma_ports[sindx],ma_modes[sindx],ma_vids[i]);
			}
		}
	}
	rotateKnob("ma_main", -135);
	document.location.replace(ma_base_href + "&MCdata=" + mcdata);
}

// compare charts data update
function ma_change_type(chartid, indx) {
	var descel;
	if (ma_valid) {
		ma_chartcmp[chartid].set_id(ma_vids_cmp[indx]);
		descel = document.getElementById("ma_"+chartid+"_d");
		descel.innerHTML = ma_idesc_cmp[indx];
	}
}

// charts initialization
ma_charttab = [];
ma_chartcmp = [];
ma_create_charts();
//--><!]]>
</script>""")

			elif len(mchtmp)==1 and len(MCdata)>0:
				chid = 0
				sdescadd = ''
				for chlist in charts:
					for id,name,desc,fdesc,sdesc in chlist:
						if name==MCdata:
							chid = id
							if sdesc!='':
								sdescadd = ', '+sdesc
				if chid==0:
					try:
						chid = int(MCdata)
					except Exception:
						pass
				if chid<=0 or chid>=1000:
					MCdata = ""

				out.append("""<div class="tab_title">Master server charts</div>""")
				out.append("""<form action="#"><table class="FR" cellspacing="0" cellpadding="0">""")
				out.append("""	<tr>""")
				out.append("""		<th class="knob-cell chart-range">""")
				out.append(html_knob_selector_chart_range('ma_main'))
				out.append("""		</th>""")
				out.append("""		<th class="chart-select">""")
				out.append("""			<div class="select-fl"><select class="chart-select" name="madata" size="1" onchange="ma_change_data(this.selectedIndex,this.options[this.selectedIndex].value)">""")
				if MCdata not in entrystr:
					out.append("""				<option value="" selected="selected"> data type or server</option>""")
				for estr in entrystr:
					if estr==MCdata:
						out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
					else:
						out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
				out.append("""			</select><span class="arrow"></span></div>""")
				if MCdata in entrystr:
					out.append("""		<label class="switch" for="ma_commonscale" id="ma_commonscale-slide" style="margin-left:10px;"><input type="checkbox" id="ma_commonscale"  onchange="AcidChartSetCommonScale('ma_main',this.checked)"/><span class="slider round"></span><span class="text">Use common Y-scale</span></label>""")				
				out.append("""		</th>""")
				out.append("""	</tr>""")
				if MCdata in entrystr:
					for mahost,maport,name,desc,chmode in servers:
						out.append("""	<tr class="C2 CHART">""")
						out.append("""		<td id="ma_%s_p" colspan="3" style="height:124px;" valign="middle">""" % name)
						out.append("""			<div class="CHARTJSW">""")
						out.append("""				<div id="ma_%s_c" class="CHARTJSC">""" % name)
						out.append("""					<span id="ma_%s_d" class="CAPTIONJS">%s%s</span>""" % (name,desc,sdescadd))
						out.append("""				</div>""")
						out.append("""			</div>""")
						out.append("""			<div id="ma_%s_l" class="CHARTJSL"></div>""" % name)
						out.append("""			<div id="ma_%s_r" class="CHARTJSR"></div>""" % name)
						out.append("""		</td>""")
						out.append("""	</tr>""")
				out.append("""</table></form>""")

				if MCdata in entrystr:
					out.append("""<script type="text/javascript">""")
					out.append("""<!--//--><![CDATA[//><!--""")
					out.append("""	var ma_vids = [%s];""" % ",".join(map(repr,[ x[0] for x in (charts[0]+charts[1]) ])))
					out.append("""	var ma_idesc = [%s];""" % ",".join(map(repr,[ x[4] for x in (charts[0]+charts[1]) ])))
					out.append("""	var ma_hosts = [%s];""" % ",".join(map(repr,[ x[0] for x in servers ])))
					out.append("""	var ma_ports = [%s];""" % ",".join(map(repr,[ x[1] for x in servers ])))
					out.append("""	var ma_inames = [%s];""" % ",".join(map(repr,[ x[2] for x in servers ])))
					out.append("""	var ma_sdesc = [%s];""" % ",".join(map(repr,[ x[3] for x in servers ])))
					out.append("""	var ma_modes = [%s];""" % ",".join(map(repr,[ x[4]-10 for x in servers ])))
					out.append("""	var ma_chid = %u;""" % chid)
					out.append("""	var ma_valid = 1;""")
					out.append("""	var ma_base_href = "%s";""" % createrawlink({"MCdata":""}))
					out.append("""//--><!]]>""")
					out.append("""</script>""")
				else:
					out.append("""<script type="text/javascript">""")
					out.append("""<!--//--><![CDATA[//><!--""")
					out.append("""	var ma_valid = 0;""")
					out.append("""	var ma_base_href = "%s";""" % createrawlink({"MCdata":""}))
					out.append("""//--><!]]>""")
					out.append("""</script>""")

				out.append("""<script type="text/javascript">
<!--//--><![CDATA[//><!--
//#2 - no comparison charts
function ma_create_charts(show_loading=true) {
	var i;
	ma_charttab = [];
	if (ma_valid) {
		for (i=0 ; i<ma_inames.length ; i++) {
			ma_charttab.push(new AcidChartWrapper("ma_"+ma_inames[i]+"_","ma_main",ma_hosts[i],ma_ports[i],ma_modes[i],ma_chid,show_loading));
		}
	}
}

// main charts data update
function ma_change_data(indx, mcdata) {
	var i,chartid;
	var descel;
	if (ma_valid && indx >= 0 && indx < ma_vids.length) {
		for (i=0 ; i<ma_inames.length ; i++) {
			chartid = ma_inames[i];
			ma_charttab[i].set_id(ma_vids[indx]);
			descel = document.getElementById("ma_"+chartid+"_d");
			if (ma_idesc[indx]!="") {
				descel.innerHTML = ma_sdesc[i] + " " + ma_idesc[indx];
			} else {
				descel.innerHTML = ma_sdesc[i];
			}
		}
	}
	document.location.replace(ma_base_href + "&MCdata=" + mcdata);
}

// charts initialization
ma_charttab = [];
ma_create_charts();
//--><!]]>
</script>""")
			else:
				out.append("""<div class="tab_title">Master server charts</div>""")
				out.append("""<form action="#"><table class="FR" cellspacing="0" cellpadding="0">""")
				out.append("""	<tr>""")
				out.append("""		<th>""")
				out.append("""			Select: <div class="select-fl"><select class="chart-select" name="madata" size="1" onchange="document.location.href='%s&MCdata='+this.options[this.selectedIndex].value">""" % createrawlink({"MCdata":""}))
				if MCdata not in entrystr:
					out.append("""				<option value="" selected="selected"> data type or server</option>""")
				for estr in entrystr:
					if estr==MCdata:
						out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
					else:
						out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
				out.append("""			</select><span class="arrow"></span></div>""")
				out.append("""		</th>""")
				out.append("""	</tr>""")
				out.append("""</table></form>""")
		elif jsonmode:
			json_mc_array = []
			if masterconn.version_at_least(4,31,0):
				chrange = MCrange
				if (chrange<0 or chrange>3) and chrange!=9:
					chrange = 0
				if MCcount<0 or MCcount>4095:
					MCcount = 4095
				for host,port,no,mode,desc,name,raw in MCchdata:
					json_mc_dict = {}
					if (host==None or port==None):
						json_mc_dict["master"] = "leader"
					else:
						json_mc_dict["master"] = "%s:%s" % (host,port)
					json_mc_dict["name"] = name
					json_mc_dict["description"] = desc
					json_mc_dict["raw_format"] = bool(raw)
					if host==None or port==None:
						perc,base,datadict = get_charts_multi_data(masterconn,no*10+chrange,MCcount)
					else:
						try:
							conn = MFSConn(host,port)
							perc,base,datadict = get_charts_multi_data(conn,no*10+chrange,MCcount)
							del conn
						except Exception:
							perc,base,datadict = None,None,None
					if perc!=None and base!=None and datadict!=None:
						base -= 2
						json_mc_dict["percent"] = bool(perc)
						json_mc_data = {}
						for chrng in datadict:
							ch1data,ch2data,ch3data,ts,mul,div = datadict[chrng]
							if base>0:
								mul = mul*(1000**base)
							if base<0:
								div = div*(1000**(-base))
							json_mc_data_range = {}
							if ch1data!=None:
								json_mc_data_range["data_array_1"] = charts_convert_data(ch1data,mul,div,raw)
								if ch2data!=None:
									json_mc_data_range["data_array_2"] = charts_convert_data(ch2data,mul,div,raw)
									if ch3data!=None:
										json_mc_data_range["data_array_3"] = charts_convert_data(ch3data,mul,div,raw)
							json_mc_data_range["timestamp"] = shiftts(ts)
							json_mc_data_range["shifted_timestamp"] = ts
							timestring = time.strftime("%Y-%m-%d %H:%M",time.gmtime(ts))
							json_mc_data_range["timestamp_str"] = timestring
							json_mc_data_range["time_step"] = [60,360,1800,86400][chrng]
							if raw:
								json_mc_data_range["multiplier"] = mul
								json_mc_data_range["divisor"] = div
							json_mc_data[chrng] = json_mc_data_range
						json_mc_dict["data_ranges"] = json_mc_data
					else:
						json_mc_dict["percent"] = None
						json_mc_dict["data_ranges"] = None
					json_mc_array.append(json_mc_dict)
			else:
				jcollect["errors"].append("Master chart data is not supported in your version of MFS - please upgrade!")
		else:
			if masterconn.version_at_least(2,0,15):
				if ttymode:
					tab = Table("Master chart data",len(MCchdata)+1,"r")
					hdrstr = ["host/port ->"]
					for host,port,no,mode,desc,name,raw in MCchdata:
						if (host==None or port==None):
							hdrstr.append("leader")
						else:
							hdrstr.append("%s:%s" % (host,port))
					tab.header(*hdrstr)
					tab.header(("---","",len(MCchdata)+1))
					hdrstr = ["Time"]
					for host,port,no,mode,desc,name,raw in MCchdata:
						if raw:
							if (no==0 or no==1 or no==100):
								hdrstr.append("%s (+)" % desc)
							else:
								hdrstr.append("%s (raw)" % desc)
						else:
							hdrstr.append(desc)
					tab.header(*hdrstr)
				else:
					tab = Table("Master chart data",len(MCchdata)+1)
				chrange = MCrange
				if chrange<0 or chrange>3:
					chrange = 0
				if MCcount<0 or MCcount>4095:
					MCcount = 4095
				chrangestep = [60,360,1800,86400][chrange]
				series = set()
				for host,port,no,mode,desc,name,raw in MCchdata:
					if no==100:
						series.add((host,port,0))
						series.add((host,port,1))
					else:
						series.add((host,port,no))
				for gpass in (1,2):
					MCresult = {}
					timestamp = 0
					entries = 0
					repeat = 0
					for host,port,x in series:
						if host==None or port==None:
							data,length = masterconn.command(CLTOAN_CHART_DATA,ANTOCL_CHART_DATA,struct.pack(">LL",x*10+chrange,MCcount))
						else:
							try:
								conn = MFSConn(host,port)
								data,length = conn.command(CLTOAN_CHART_DATA,ANTOCL_CHART_DATA,struct.pack(">LL",x*10+chrange,MCcount))
								del conn
							except Exception:
								data,length = None,0
						if length>=8:
							ts,e = struct.unpack(">LL",data[:8])
							if e*8+8==length and (entries==0 or entries==e):
								entries = e
								if timestamp==0 or timestamp==ts or gpass==2:
									timestamp=ts
									MCresult[(host,port,x)] = list(struct.unpack(">"+e*"Q",data[8:]))
								else:
									repeat = 1
									break
							else:
								MCresult[(host,port,x)]=None
						else:
							MCresult[(host,port,x)]=None
					if repeat:
						continue
					else:
						break
				for e in xrange(entries):
					ts = timestamp-chrangestep*e
					timestring = time.strftime("%Y-%m-%d %H:%M",time.gmtime(ts))
					dline = [timestring]
					for host,port,no,mode,desc,name,raw in MCchdata:
						if no==100:
							datalist1 = MCresult[(host,port,0)]
							datalist2 = MCresult[(host,port,1)]
							if (datalist1!=None and datalist2!=None and datalist1[e]<((2**64)-1) and datalist2[e]<((2**64)-1)):
								data = datalist1[e]+datalist2[e]
							else:
								data = None
						else:
							datalist = MCresult[(host,port,no)]
							if datalist!=None and datalist[e]<((2**64)-1):
								data = datalist[e]
							else:
								data = None
						if data==None:
							dline.append("-")
						elif mode==0:
							cpu = (data/(10000.0*chrangestep))
							if raw:
								dline.append("%.8f%%" % (cpu))
							else:
								dline.append("%.2f%%" % (cpu))
						elif mode==1:
							if raw:
								dline.append("%u" % data)
							else:
								data = float(data)/float(chrangestep)
								dline.append("%.3f/s" % data)
						elif mode==2:
							if raw:
								dline.append("%u" % data)
							else:
								dline.append("%s" % humanize_number(data," "))
						elif mode==5:
							if raw:
								dline.append("%u" % data)
							else:
								data = float(data)/float(chrangestep)
								dline.append("%.3fMB/s" % (data/(1024.0*1024.0)))
						elif mode==6:
							dline.append("%u" % data)
						elif mode==7:
							dline.append("%us" % data)
						elif mode==8:
							diff = (data/1000.0)
							if raw:
								dline.append("%.8f%%" % (diff))
							else:
								dline.append("%.2f%%" % (diff))
					tab.append(*dline)
			else:
				tab = Table("Master chart data is not supported in your version of MFS - please upgrade!",1,"r")
		if cgimode:
			print("\n".join(out))
		elif jsonmode:
			jcollect["dataset"]["mastercharts"] = json_mc_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# charts are ajax-updated individually, without resending the page content
if "CC" in sectionset and masterconn!=None and not ajax_request:
	out = []
	try:
		if not cgimode:
			CCchdata_defined = []
			CCchdata_undefined = []
			for host,port,no,mode,desc,name,raw in CCchdata:
				if host==None or port==None:
					CCchdata_undefined.append((no,mode,desc,name,raw))
				else:
					CCchdata_defined.append((host,port,no,mode,desc,name,raw))
			CCchdata = []
			if len(CCchdata_undefined)>0:
				for cs in dataprovider.get_chunkservers():
					for no,mode,desc,name,raw in CCchdata_undefined:
						CCchdata.append((cs.host,cs.port,no,mode,desc,name,raw))
			for host,port,no,mode,desc,name,raw in CCchdata_defined:
				CCchdata.append((host,port,no,mode,desc,name,raw))
		if cgimode:
			hostlist = []
			for cs in dataprovider.get_chunkservers():
				if (cs.flags&1)==0:
					hostlist.append((cs.ip,cs.port,cs.version))
			icharts = (
				(100,'cpu','cpu usage (percent)','<b>cpu usage</b>, sys: BOXGR2A user: BOXGR2B','sys: BOXGR2A user: BOXGR2B'),
				(107,'memory','memory usage (if available) rss + virt','<b>memory usage</b> (if available), rss: BOXGR2A + virt: BOXGR2B','rss: BOXGR2A + virt: BOXGR2B'),
				(109,'space','raw disk space usage (used/total)','<b>raw disk space usage</b>, used: BOXGR2A total: BOXGR2B','used: BOXGR2A total: BOXGR2B'),
				(110,'chunks','number of stored chunks (ec8/ec4/copy)','<b>number of stored chunks</b>, ec8: BOXGR3A ec4: BOXGR3B copy: BOXGR3C','ec8: BOXGR3A ec4: BOXGR3B copy: BOXGR3C'),
				(111,'hddcnt','number of folders (hard drives) with data','<b>number of folders (hard drives) with data</b>, damaged: BOXGR3A marked for removal: BOXGR3B working: BOXGR3C','damaged: BOXGR3A marked for removal: BOXGR3B working: BOXGR3C'),
				(47,'udiff','usage difference between the most and least used disk','',''),
				(28,'load','max number operations waiting in queue','',''),
				(101,'datain','traffic from clients and other chunkservers (bits/s - main server + replicator)','<b>traffic from clients and other chunkservers</b> (bits/s), main server: BOXGR2A + replicator: BOXGR2B','main server: BOXGR2A + replicator: BOXGR2B'),
				(102,'dataout','traffic to clients and other chunkservers (bits/s - main server + replicator)','<b>traffic to clients and other chunkservers</b> (bits/s), main server: BOXGR2A + replicator: BOXGR2B','main server: BOXGR2A + replicator: BOXGR2B'),
				(103,'bytesr','bytes read - data/other (bytes/s)','<b>bytes read</b> (bytes/s), data: BOXGR2A other: BOXGR2B','data: BOXGR2A other: BOXGR2B'),
				(104,'bytesw','bytes written - data/other (bytes/s)','<b>bytes written</b> (bytes/s), data: BOXGR2A other: BOXGR2B','data: BOXGR2A other: BOXGR2B'),
				(2,'masterin','traffic from master (bits/s)','',''),
				(3,'masterout','traffic to master (bits/s)','',''),
				(105,'hddopr','number of low-level read operations (per minute)','<b>number of low-level read operations</b> (per minute), data: BOXGR2A other: BOXGR2B','data: BOXGR2A other: BOXGR2B'),
				(106,'hddopw','number of low-level write operations (per minute)','<b>number of low-level write operations</b> (per minute), data: BOXGR2A other: BOXGR2B','data: BOXGR2A other: BOXGR2B'),
				(16,'hlopr','number of high-level read operations (per minute)','',''),
				(17,'hlopw','number of high-level write operations (per minute)','',''),
				(18,'rtime','time of data read operations','',''),
				(19,'wtime','time of data write operations','',''),
				(20,'repl','number of chunk replications (per minute)','',''),
				(21,'create','number of chunk creations (per minute)','',''),
				(22,'delete','number of chunk deletions (per minute)','',''),
				(33,'change','number of chunk internal operations duplicate, truncate, etc. (per minute)','',''),
				(108,'move','number of chunk internal rebalances per minute (high speed + low speed)','<b>number of chunk internal rebalances</b> (per minute), high speed: BOXGR2A + low speed: BOXGR2B','high speed: BOXGR2A + low speed: BOXGR2B')
			)

			charts = []
			for id,oname,desc,fdesc,sdesc in icharts:
				if fdesc=='':
					fdesc = '<b>' + desc.replace(' (', '</b> (')
					if not '</b>' in fdesc:
						fdesc = fdesc + '</b>'
				fdeschtml = fdesc.replace('BOXGR1A','<span class="CBOX GR1A"></span>').replace('BOXGR2A','<span class="CBOX GR2A"></span>').replace('BOXGR2B','<span class="CBOX GR2B"></span>').replace('BOXGR3A','<span class="CBOX GR3A"></span>').replace('BOXGR3B','<span class="CBOX GR3B"></span>').replace('BOXGR3C','<span class="CBOX GR3C"></span>')
				sdeschtml = sdesc.replace('BOXGR1A','<span class="CBOX GR1A"></span>').replace('BOXGR2A','<span class="CBOX GR2A"></span>').replace('BOXGR2B','<span class="CBOX GR2B"></span>').replace('BOXGR3A','<span class="CBOX GR3A"></span>').replace('BOXGR3B','<span class="CBOX GR3B"></span>').replace('BOXGR3C','<span class="CBOX GR3C"></span>')
				charts.append((id,oname,desc,fdeschtml,sdeschtml))
			servers = []
			entrystr = []
			entrydesc = {}
			if len(hostlist)>0:
				hostlist.sort()
				for id,oname,desc,fdesc,sdesc in charts:
					name = oname.replace(":","")
					entrystr.append(name)
					entrydesc[name] = desc
				for ip,port,version in hostlist:
					if version>=(2,0,15):
						chmode = 10 if version>=(4,31,0) else 12
						strip = "%u.%u.%u.%u" % ip
						name = "%s:%u" % (strip,port)
						namearg = "%s:%u" % (name,chmode)
						hostx = resolve(strip)
						if hostx==UNRESOLVED:
							host = ""
						else:
							host = " / "+hostx
						entrystr.append(namearg)
						entrydesc[namearg] = "Server: %s%s" % (name,host)
						servers.append((strip,port,"cs_"+name.replace(".","_").replace(":","_"),"Server: <b>%s%s</b>" % (name,host),chmode))

			cchtmp = CCdata.split(":")
			if len(cchtmp)==2:
				cchtmp = (cchtmp[0],cchtmp[1],0)
			if len(cchtmp)==3:
				cshost = cchtmp[0]
				csport = cchtmp[1]
				csmode = int(cchtmp[2])
				if csmode<10:
					csmode = 1
				else:
					csmode -= 10

				out.append("""<div class="tab_title">Chunk servers charts</div>""")
				out.append("""<form action="#"><table class="FR" cellspacing="0" cellpadding="0">""")
				out.append("""	<tr>""")
				out.append("""		<th class="knob-cell chart-range">""")
				out.append(html_knob_selector_chart_range('cs_main'))
				out.append("""		</th>""")
				out.append("""		<th class="chart-select">""")
				out.append("""			<div class="select-fl"><select class="chart-select" name="csdata" size="1" onchange="cs_change_data(this.selectedIndex,this.options[this.selectedIndex].value)">""")
				if CCdata not in entrystr:
					out.append("""				<option value="" selected="selected"> data type or server</option>""")
				for estr in entrystr:
					if estr==CCdata:
						out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
					else:
						out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
				out.append("""			</select><span class="arrow"></span></div>""")
				out.append("""		</th>""")
				out.append("""	</tr>""")
				if CCdata in entrystr:
					for id,name,desc,fdesc,sdesc in charts:
						out.append("""	<tr class="C2 CHART">""")
						out.append("""		<td id="cs_%s_p" colspan="3" style="height:124px;" valign="middle">""" % name)
						out.append("""			<div class="CHARTJSW">""")
						out.append("""				<div id="cs_%s_c" class="CHARTJSC">""" % name)
						out.append("""					<span class="CAPTIONJS">%s</span>""" % fdesc)
						out.append("""				</div>""")
						out.append("""			</div>""")
						out.append("""			<div id="cs_%s_l" class="CHARTJSL"></div>""" % name)
						out.append("""			<div id="cs_%s_r" class="CHARTJSR"></div>""" % name)
						out.append("""		</td>""")
						out.append("""	</tr>""")
				out.append("""</table></form>""")

				if CCdata in entrystr:
					out.append("""<div class="tab_title">Chunk server charts (comparison)</div>""")
					out.append("""<form action="#"><table class="FR" cellspacing="0" cellpadding="0">""")
					for i in range(2):
						out.append("""	<tr>""")
						if i==0:
							out.append("""		<th class="knob-cell chart-cmp-range">""")
							out.append(html_knob_selector_chart_range('cs_cmp')) 
						else:
							out.append("""		<th style="text-align: right;vertical-align:middle;border:none;">""")
							out.append("""versus:""")
						out.append("""		</th>""")
						out.append("""		<th class="chart-cmp-select">""")
						out.append("""			<div class="select-fl"><select class="chart-select" id="cschart%u_select" name="cschart%u" size="1" onchange="cs_change_type(%u,this.options[this.selectedIndex].value)">""" % (i,i,i))
						no = 0
						for id,name,desc,fdesc,sdesc in charts:
							out.append("""				<option value="%u">%s</option>""" % (no,desc))
							no += 1
						out.append("""			</select><span class="arrow"></span></div>""")
						out.append("""		</th>""")
						out.append("""	</tr>""")
						out.append("""	<tr class="C2 CHART">""")
						out.append("""		<td id="cs_%u_p" colspan="2" style="height:124px; border-top: none;" valign="middle">""" % i)
						out.append("""			<div class="CHARTJSW">""")
						out.append("""				<div id="cs_%u_c" class="CHARTJSC">""" % i)
						out.append("""					<span id="cs_%u_d" class="CAPTIONJS">%s</span>""" % (i,charts[0][3]))
						out.append("""				</div>""")
						out.append("""			</div>""")
						out.append("""			<div id="cs_%u_l" class="CHARTJSL"></div>""" % i)
						out.append("""			<div id="cs_%u_r" class="CHARTJSR"></div>""" % i)
						out.append("""		</td>""")
						out.append("""	</tr>""")
					out.append("""</table></form>""")


					out.append("""<script type="text/javascript">""")
					out.append("""<!--//--><![CDATA[//><!--""")
					out.append("""	var cs_vids = [%s];""" % ",".join(map(repr,[ x[0] for x in charts ])))
					out.append("""	var cs_inames = [%s];""" % ",".join(map(repr,[ x[1] for x in charts ])))
					out.append("""	var cs_idesc = [%s];""" % ",".join(map(repr,[ x[3] for x in charts ])))
					out.append("""	var cs_hosts = [%s];""" % ",".join(map(repr,[ x[0] for x in servers ])))
					out.append("""	var cs_ports = [%s];""" % ",".join(map(repr,[ x[1] for x in servers ])))
					out.append("""	var cs_modes = [%s];""" % ",".join(map(repr,[ x[4]-10 for x in servers ])))
					out.append("""	var cs_host = "%s";""" % cshost)
					out.append("""	var cs_port = "%s";""" % csport)
					out.append("""	var cs_mode = %u;""" % csmode)
					out.append("""	var cs_valid = 1;""")
					out.append("""	var cs_base_href = "%s";""" % createrawlink({"CCdata":""}))
					out.append("""//--><!]]>""")
					out.append("""</script>""")
				else:
					out.append("""<script type="text/javascript">""")
					out.append("""<!--//--><![CDATA[//><!--""")
					out.append("""	var cs_valid = 0;""")
					out.append("""	var cs_base_href = "%s";""" % createrawlink({"CCdata":""}))
					out.append("""//--><!]]>""")
					out.append("""</script>""")
				out.append("""<script type="text/javascript">
<!--//--><![CDATA[//><!--
function cs_create_charts(show_loading=true) {
	var i;
	cs_charttab = [];
	if (cs_valid) {
		for (i=0 ; i<cs_inames.length ; i++) {
			cs_charttab.push(new AcidChartWrapper("cs_"+cs_inames[i]+"_","cs_main",cs_host,cs_port,cs_mode,cs_vids[i],show_loading));
		}
		if (cs_chartcmp[0]) document.getElementById('cschart0_select').value = cs_vids.indexOf(cs_chartcmp[0].id);
		if (cs_chartcmp[1]) document.getElementById('cschart1_select').value = cs_vids.indexOf(cs_chartcmp[1].id);
		cs_chartcmp[0] = new AcidChartWrapper("cs_0_","cs_cmp",cs_host,cs_port,cs_mode,(cs_chartcmp[0]) ? cs_chartcmp[0].id : cs_vids[0],show_loading);
		cs_chartcmp[1] = new AcidChartWrapper("cs_1_","cs_cmp",cs_host,cs_port,cs_mode,(cs_chartcmp[1]) ? cs_chartcmp[1].id : cs_vids[0],show_loading);
	}
}

// main charts
function cs_change_data(indx, ccdata) {
	var sindx,i;
	if (cs_valid) {
		sindx = indx - cs_inames.length;
		if (sindx >= 0 && sindx < cs_modes.length) {
			for (i=0 ; i<cs_vids.length ; i++) {
				cs_charttab[i].set_host_port_id(cs_hosts[sindx],cs_ports[sindx],cs_modes[sindx],cs_vids[i]);
			}
		}
	}
	document.location.replace(cs_base_href + "&CCdata=" + ccdata);		   
}

// compare charts
function cs_change_type(chartid,indx) {
	var descel;
	if (cs_valid) {
		cs_chartcmp[chartid].set_id(cs_vids[indx]);
		descel = document.getElementById("cs_"+chartid+"_d");
		descel.innerHTML = cs_idesc[indx];
	}
}

cs_charttab = [];
cs_chartcmp = [];
cs_create_charts();
//--><!]]>
</script>""")
			elif len(cchtmp)==1 and len(CCdata)>0:
				chid = 0
				sdescadd = ''
				for id,name,desc,fdesc,sdesc in charts:
					if name==CCdata:
						chid = id
						if sdesc!='':
							sdescadd = ', '+sdesc
				if chid==0:
					try:
						chid = int(CCdata)
					except Exception:
						pass
				if chid<=0 or chid>=1000:
					CCdata = ""

				out.append("""<div class="tab_title">Chunk servers charts</div>""")
				out.append("""<form action="#"><table class="FR" cellspacing="0" cellpadding="0">""")
				out.append("""	<tr>""")
				out.append("""		<th class="knob-cell chart-range">""")
				out.append(html_knob_selector_chart_range('cs_main'))
				out.append("""		</th>""")
				out.append("""		<th class="chart-select">""")
				out.append("""			<div class="select-fl"><select class="chart-select" name="csdata" size="1" onchange="cs_change_data(this.selectedIndex,this.options[this.selectedIndex].value)">""")
				if CCdata not in entrystr:
					out.append("""				<option value="" selected="selected"> data type or server</option>""")
				for estr in entrystr:
					if estr==CCdata:
						out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
					else:
						out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
				out.append("""			</select><span class="arrow"></span></div>""")
				if CCdata in entrystr:
					out.append("""		<label class="switch" for="cs_commonscale" id="cs_commonscale-slide" style="margin-left:10px;"><input type="checkbox" id="cs_commonscale"  onchange="AcidChartSetCommonScale('cs_main',this.checked)"/><span class="slider round"></span><span class="text">Use common Y-scale</span></label>""")				
				out.append("""		</th>""")
				out.append("""	</tr>""")
				if CCdata in entrystr:
					for cshost,csport,name,desc,chmode in servers:
						out.append("""	<tr class="C2 CHART">""")
						out.append("""		<td id="cs_%s_p" colspan="3" style="height:124px;" valign="middle">""" % name)
						out.append("""			<div class="CHARTJSW">""")
						out.append("""				<div id="cs_%s_c" class="CHARTJSC">""" % name)
						out.append("""					<span id="cs_%s_d" class="CAPTIONJS">%s%s</span>""" % (name,desc,sdescadd))
						out.append("""				</div>""")
						out.append("""			</div>""")
						out.append("""			<div id="cs_%s_l" class="CHARTJSL"></div>""" % name)
						out.append("""			<div id="cs_%s_r" class="CHARTJSR"></div>""" % name)
						out.append("""		</td>""")
						out.append("""	</tr>""")
				out.append("""</table></form>""")

				if CCdata in entrystr:
					out.append("""<script type="text/javascript">""")
					out.append("""<!--//--><![CDATA[//><!--""")
					out.append("""	var cs_vids = [%s];""" % ",".join(map(repr,[ x[0] for x in charts ])))
					out.append("""	var cs_idesc = [%s];""" % ",".join(map(repr,[ x[4] for x in charts ])))
					out.append("""	var cs_hosts = [%s];""" % ",".join(map(repr,[ x[0] for x in servers ])))
					out.append("""	var cs_ports = [%s];""" % ",".join(map(repr,[ x[1] for x in servers ])))
					out.append("""	var cs_inames = [%s];""" % ",".join(map(repr,[ x[2] for x in servers ])))
					out.append("""	var cs_sdesc = [%s];""" % ",".join(map(repr,[ x[3] for x in servers ])))
					out.append("""	var cs_modes = [%s];""" % ",".join(map(repr,[ x[4]-10 for x in servers ])))
					out.append("""	var cs_chid = %u;""" % chid)
					out.append("""	var cs_valid = 1;""")
					out.append("""	var cs_base_href = "%s";""" % createrawlink({"CCdata":""}))
					out.append("""//--><!]]>""")
					out.append("""</script>""")
				else:
					out.append("""<script type="text/javascript">""")
					out.append("""<!--//--><![CDATA[//><!--""")
					out.append("""	var cs_valid = 0;""")
					out.append("""	var cs_base_href = "%s";""" % createrawlink({"CCdata":""}))
					out.append("""//--><!]]>""")
					out.append("""</script>""")

				out.append("""<script type="text/javascript">
<!--//--><![CDATA[//><!--
function cs_create_charts(show_loading=true) {
	var i;
	cs_charttab = [];
	if (cs_valid) {
		for (i=0 ; i<cs_inames.length ; i++) {
			cs_charttab.push(new AcidChartWrapper("cs_"+cs_inames[i]+"_","cs_main",cs_hosts[i],cs_ports[i],cs_modes[i],cs_chid,show_loading));
		}
	}
}

// main charts
function cs_change_data(indx, ccdata) {
	var i,chartid;
	var descel;
	if (cs_valid && indx >= 0 && indx < cs_vids.length) {
		for (i=0 ; i<cs_inames.length ; i++) {
			chartid = cs_inames[i];
			cs_charttab[i].set_id(cs_vids[indx]);
			descel = document.getElementById("cs_"+chartid+"_d");
			if (cs_idesc[indx]!="") {
				descel.innerHTML = cs_sdesc[i] + " " + cs_idesc[indx];
			} else {
				descel.innerHTML = cs_sdesc[i];
			}
		}
	}
	document.location.replace(cs_base_href + "&CCdata=" + ccdata);
}

cs_charttab = [];
cs_create_charts();
//--><!]]>
</script>""")
			else:
				out.append("""<div class="tab_title">Chunk servers charts</div>""")
				out.append("""<form action="#"><table class="FR" cellspacing="0" cellpadding="0">""")
				out.append("""	<tr>""")
				out.append("""		<th>""")
				out.append("""			Select: <div class="select-fl"><select class="chart-select" name="csdata" size="1" onchange="document.location.href='%s&CCdata='+this.options[this.selectedIndex].value">""" % createrawlink({"CCdata":""}))
				if CCdata not in entrystr:
					out.append("""				<option value="" selected="selected"> data type or server</option>""")
				for estr in entrystr:
					if estr==CCdata:
						out.append("""<option value="%s" selected="selected">%s</option>""" % (estr,entrydesc[estr]))
					else:
						out.append("""<option value="%s">%s</option>""" % (estr,entrydesc[estr]))
				out.append("""			</select><span class="arrow"></span></div>""")
				out.append("""		</th>""")
				out.append("""	</tr>""")
				out.append("""</table></form>""")
		elif jsonmode:
			json_cc_array = []
			if masterconn.version_at_least(4,31,0):
				chrange = CCrange
				if (chrange<0 or chrange>3) and chrange!=9:
					chrange = 0
				if CCcount<0 or CCcount>4095:
					CCcount = 4095
				for host,port,no,mode,desc,name,raw in CCchdata:
					json_cc_dict = {}
					json_cc_dict["chunkserver"] = "%s:%s" % (host,port)
					json_cc_dict["name"] = name
					json_cc_dict["description"] = desc
					json_cc_dict["raw_format"] = bool(raw)
					try:
						conn = MFSConn(host,port)
						perc,base,datadict = get_charts_multi_data(conn,no*10+chrange,CCcount)
						del conn
					except Exception:
						perc,base,datadict = None,None,None
					if perc!=None and base!=None and datadict!=None:
						base -= 2
						json_cc_dict["percent"] = bool(perc)
						json_cc_data = {}
						for chrng in datadict:
							ch1data,ch2data,ch3data,ts,mul,div = datadict[chrng]
							if base>0:
								mul = mul*(1000**base)
							if base<0:
								div = div*(1000**(-base))
							json_cc_data_range = {}
							if ch1data!=None:
								json_cc_data_range["data_array_1"] = charts_convert_data(ch1data,mul,div,raw)
								if ch2data!=None:
									json_cc_data_range["data_array_2"] = charts_convert_data(ch2data,mul,div,raw)
									if ch3data!=None:
										json_cc_data_range["data_array_3"] = charts_convert_data(ch3data,mul,div,raw)
							json_cc_data_range["timestamp"] = shiftts(ts)
							json_cc_data_range["shifted_timestamp"] = ts
							timestring = time.strftime("%Y-%m-%d %H:%M",time.gmtime(ts))
							json_cc_data_range["timestamp_str"] = timestring
							json_cc_data_range["time_step"] = [60,360,1800,86400][chrng]
							if raw:
								json_cc_data_range["multiplier"] = mul
								json_cc_data_range["divisor"] = div
							json_cc_data[chrng] = json_cc_data_range
						json_cc_dict["data_ranges"] = json_cc_data
					else:
						json_cc_dict["percent"] = None
						json_cc_dict["data_ranges"] = None
					json_cc_array.append(json_cc_dict)
			else:
				jcollect["errors"].append("Chunkserver chart data is not supported in your version of MFS - please upgrade!")
		else:
			if masterconn.version_at_least(2,0,15):
				if ttymode:
					tab = Table("Chunkserver chart data",len(CCchdata)+1,"r")
					hdrstr = ["host/port ->"]
					for host,port,no,mode,desc,name,raw in CCchdata:
						hdrstr.append("%s:%s" % (host,port))
					tab.header(*hdrstr)
					tab.header(("---","",len(CCchdata)+1))
					hdrstr = ["Time"]
					for host,port,no,mode,desc,name,raw in CCchdata:
						if raw:
							if mode==0:
								hdrstr.append("%s (+)" % desc)
							else:
								hdrstr.append("%s (raw)" % desc)
						else:
							hdrstr.append(desc)
					tab.header(*hdrstr)
				else:
					tab = Table("Chunkserver chart data",len(CCchdata)+1)
				chrange = CCrange
				if chrange<0 or chrange>3:
					chrange = 0
				if CCcount<0 or CCcount>4095:
					CCcount = 4095
				chrangestep = [60,360,1800,86400][chrange]
				series = set()
				for host,port,no,mode,desc,name,raw in CCchdata:
					if no==100:
						series.add((host,port,0))
						series.add((host,port,1))
					else:
						series.add((host,port,no))
				for gpass in (1,2):
					CCresult = {}
					timestamp = 0
					entries = 0
					repeat = 0
					for host,port,x in series:
						try:
							conn = MFSConn(host,port)
							data,length = conn.command(CLTOAN_CHART_DATA,ANTOCL_CHART_DATA,struct.pack(">LL",x*10+chrange,CCcount))
							del conn
						except Exception:
							length = 0
						if length>=8:
							ts,e = struct.unpack(">LL",data[:8])
							if e*8+8==length and (entries==0 or entries==e):
								entries = e
								if timestamp==0 or timestamp==ts or gpass==2:
									timestamp=ts
									CCresult[(host,port,x)] = list(struct.unpack(">"+e*"Q",data[8:]))
								else:
									repeat = 1
									break
							else:
								CCresult[(host,port,x)]=None
						else:
							CCresult[(host,port,x)]=None
					if repeat:
						continue
					else:
						break
				for e in xrange(entries):
					ts = timestamp-chrangestep*e
					timestring = time.strftime("%Y-%m-%d %H:%M",time.gmtime(ts))
					dline = [timestring]
					for host,port,no,mode,desc,name,raw in CCchdata:
						if no==100:
							datalist1 = CCresult[(host,port,0)]
							datalist2 = CCresult[(host,port,1)]
							if (datalist1!=None and datalist2!=None):
								data = datalist1[e]+datalist2[e]
							else:
								data = None
						else:
							datalist = CCresult[(host,port,no)]
							if datalist!=None:
								data = datalist[e]
							else:
								data = None
						if data==None:
							dline.append("-")
						elif mode==0:
							cpu = (data/(10000.0*chrangestep))
							if raw:
								dline.append("%.8f%%" % (cpu))
							else:
								dline.append("%.2f%%" % (cpu))
						elif mode==1:
							if raw:
								dline.append("%u" % data)
							else:
								data = float(data)/float(chrangestep)
								dline.append("%.3f/s" % data)
						elif mode==2:
							if raw:
								dline.append("%u" % data)
							else:
								dline.append("%s" % humanize_number(data," "))
						elif mode==3:
							dline.append("%u threads" % data)
						elif mode==4:
							if raw:
								dline.append("%u" % data)
							else:
								data = float(data)/float(chrangestep)
								data /= 10000000.0
								dline.append("%.2f%%" % (data))
						elif mode==5:
							if raw:
								dline.append("%u" % data)
							else:
								data = float(data)/float(chrangestep)
								dline.append("%.3fMB/s" % (data/(1024.0*1024.0)))
						elif mode==6:
							dline.append("%u" % data)
						elif mode==8:
							diff = (data/1000.0)
							if raw:
								dline.append("%.8f%%" % (diff))
							else:
								dline.append("%.2f%%" % (diff))
					tab.append(*dline)
			else:
				tab = Table("Chunkserver chart data are not supported in your version of MFS - please upgrade",1,"r")
		if cgimode:
			print("\n".join(out))
		elif jsonmode:
			jcollect["dataset"]["cscharts"] = json_cc_array
		else:
			print(myunicode(tab))
	except Exception:
		print_exception()

# GUI footer
if cgimode:
	if not ajax_request:
		print("""</div><!-- end of container -->""")
		
		print("""<div id="footer">""")
		if masterconn!=None:
			instancename=""
			if masterconn.has_feature(FEATURE_INSTANCE_NAME):
				data,length = masterconn.command(CLTOMA_INSTANCE_NAME,MATOCL_INSTANCE_NAME)
				if length>0:
					pos = 0
					err = 0
					instancename,pos,err = get_string_from_packet(data,pos,err)
			if instancename=="":
				instancename="(no name)"
		else:
			instancename="(unknown)"	
		print("""<div class="footer_left">""")			
		print("""Instance: <b>%s</b>""" % instancename)
		print("""</div>""")
		print("""<div class="footer_left"  style="padding-left: 14px;">""")
		print("""<label class="switch" for="auto-refresh" id="auto-refresh-slide"><input type="checkbox" id="auto-refresh" /><span class="slider round with-dot"><span class="slider-text"></span></span></label>""")
		print("""</div>""")
		print("""<div class="footer_left" style="padding-left: 3px;">""")
		print("""<span id="refresh-button">Refresh:</span> <span id="refresh-timestamp">%s</span>""" % time_to_str(time.time())  )
		print("""</div>""")
		print("""<div class="footer_right text-icon">""")
		prover=""
		ver_icon=""
		ver_class=""
		if leaderfound or electfound:
			v1,v2,v3 = struct.unpack(">HBB",masterinfo[:4])
			(masterver,_) = version_str_and_sort((v1,v2,v3))
			ver_diff=cmp_ver(masterver, VERSION)
			if (ver_diff==1): # master in newer than GUI
				ver_icon="""<svg height="12px" width="12px"><use xlink:href="#icon-error"/></svg>"""
				ver_class="BADVERSION"
			elif (ver_diff==-1): # GUI in newer than master
				ver_icon="""<svg height="12px" width="12px"><use xlink:href="#icon-warning"/></svg>"""
		print("""GUI&nbsp;%s<span class="%s">v.%s%s</span>, python v.%u.%u""" % (ver_icon, ver_class, VERSION, prover, sys.version_info[0], sys.version_info[1]))
		print("""</div>""")
		print("""<div id="theme-toggle"><svg height="14px" width="14px"><use xlink:href="#icon-light"/></svg></div>""")
		print("""</div>""")
		print("""<script type="text/javascript" src="cgiscripts.js"></script>""")
		print("""</body>""")
		print("""</html>""")

if not cgimode and jsonmode and masterconn!=None:
	print(json.dumps(jcollect))
