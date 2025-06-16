# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MooseFS is a petabyte-scale, open-source, distributed file system implemented in pure C. It consists of four main components that communicate via a custom binary protocol:

- **Master Server** (`mfsmaster/`): Central metadata server managing the entire cluster
- **Chunk Server** (`mfschunkserver/`): Data storage nodes storing 64MB file chunks
- **Client** (`mfsclient/`): FUSE-based client for mounting the filesystem + management tools
- **Metalogger** (`mfsmetalogger/`): Backup metadata server for high availability

## Build System

### Standard Build Process
```bash
# Configure and build
./configure
make

# Install (requires appropriate permissions)
make install

# Platform-specific builds
./linux_build.sh      # Standard Linux build
./freebsd_build.sh     # FreeBSD build
./macosx_build.sh      # macOS build (requires macFUSE)
./sanitize_build.sh    # Debug build with sanitizers
```

### Build Configuration
- Uses autotools (autoconf/automake) with `configure.ac` and `Makefile.am` files
- Detects system capabilities (FUSE, threading, C11 support)
- Conditional compilation based on detected features
- Each component has its own `Makefile.am` for modular compilation

### Testing
```bash
# Run unit tests
make check

# Individual test binaries are in mfstests/
./mfstests/mfstest_datapack
./mfstests/mfstest_crc32
./mfstests/mfstest_clocks
```

## Code Architecture

### Communication Protocol
- Binary protocol defined in `mfscommon/MFSCommunication.h`
- Packet structure: `type:32 length:32 data:lengthB` (network byte order)
- Files split into chunks (default 64MB, configurable)
- Chunks divided into 64KB blocks

### Core Libraries
- `mfscommon/`: Shared utilities and communication protocols
- `libmfsio.la`: Programmatic access library for MooseFS operations

### Key Source Files by Component

**Master Server (`mfsmaster/`)**:
- `filesystem.c/h`: Core file system operations and metadata
- `chunks.c/h`: Chunk management and distribution
- `exports.c/h`: Access control and export management
- `storageclass.c/h`: Storage class and data tiering policies
- `matoclserv.c/h`: Master-to-client communication
- `matocsserv.c/h`: Master-to-chunkserver communication

**Chunk Server (`mfschunkserver/`)**:
- `hddspacemgr.c/h`: Hard disk space management and I/O
- `masterconn.c/h`: Communication with master server
- `replicator.c/h`: Chunk replication management

**Client (`mfsclient/`)**:
- `mfs_fuse.c/h`: FUSE interface implementation
- `mastercomm.c/h`: Communication with master server
- `readdata.c/h` & `writedata.c/h`: Data read/write operations
- `tools_*.c`: Various management tools (quota, snapshots, etc.)

### Configuration Files
- Template files end with `.cfg.in` and are processed during build
- Configuration files are typically runtime reloadable
- Man pages provide comprehensive configuration documentation

## Development Notes

### Code Style
- Pure C implementation (C99/C11 standards)
- Event-driven architecture with async I/O
- Thread-safe where needed using pthreads
- Extensive error handling with POSIX-style error codes

### Protocol Versioning
- Protocol versions managed for backward compatibility
- Version information in `MFSCommunication.h`
- Current version: 4.57.6 (build 1944)

### Package Structure
The project builds multiple packages:
- `moosefs-master`: Metadata server
- `moosefs-chunkserver`: Data storage server
- `moosefs-client`: Client tools and mount utilities
- `moosefs-metalogger`: Backup metadata server
- `moosefs-cli`: Command-line monitoring tools
- `moosefs-cgi`: Web-based monitoring interface

## Development Rules

### Information Lookup
- **Always use DeepWiki MCP** for `moosefs/moosefs` when curious about how the normal MooseFS codebase looks or unsure of implementation details
- **Ask for clarification** if unsure what to do next instead of inventing something or being overly creative

### Session Initialization
- **Start every session** by checking:
  1. `PROGRESS.md` - Review current project status and recent work
  2. TaskMaster MCP - Check active tasks and project planning

## Metadata Compatibility

- We MUST maintain 100% compatibility with the MooseFS metadata format and normal metaloggers. Users must be able to walk in with an existing cluster's metadata.mfs and load it, and export a copy of metadata.mfs that a normal MooseFS CE cluster would work with just fine.