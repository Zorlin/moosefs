AM_CPPFLAGS = -I$(top_srcdir)/mfscommon

sbin_PROGRAMS = mfsha

mfsha_SOURCES = \
	mfsha.c \
	init.h \
	raftconsensus.c \
	raftconsensus.h \
	crdtstore.c \
	crdtstore.h \
	shardmgr.c \
	shardmgr.h \
	metasyncer.c \
	metasyncer.h \
	gossip.c \
	gossip.h \
	haconn.c \
	haconn.h \
	../mfscommon/processname.c ../mfscommon/processname.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/mfslog.c ../mfscommon/mfslog.h \
	../mfscommon/clocks.c ../mfscommon/clocks.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/strerr.c ../mfscommon/strerr.h \
	../mfscommon/random.c ../mfscommon/random.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/timeparser.c ../mfscommon/timeparser.h \
	../mfscommon/massert.h \
	../mfscommon/portable.h \
	../mfscommon/datapack.h


EXTRA_DIST = $(man_MANS)

distclean-local:
	-rm -rf $(DEPDIR)