# MooseFS HA Installation Guide

## Prerequisites

### System Requirements
- **Operating System**: Linux (Ubuntu 20.04+, CentOS 7+, RHEL 7+)
- **Architecture**: x86_64 (amd64)
- **Memory**: 8GB+ RAM per node (16GB+ recommended for production)
- **Storage**: 100GB+ for metadata storage per node
- **Network**: Gigabit Ethernet between nodes (10GbE recommended)

### Software Dependencies
- GCC 7+ or Clang 6+
- autotools (autoconf, automake, libtool)
- pkg-config
- FUSE development libraries
- zlib development libraries

### Network Requirements
- Port 9430 (HA communication) - between all HA nodes
- Port 9419 (legacy master compatibility) - client access
- NTP synchronization recommended for accurate timestamps

## Installation Steps

### 1. Install Build Dependencies

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y build-essential autotools-dev autoconf automake libtool \
    pkg-config libfuse-dev libfuse3-dev zlib1g-dev libc6-dev
```

**CentOS/RHEL:**
```bash
sudo yum groupinstall -y "Development Tools"
sudo yum install -y autoconf automake libtool pkgconfig fuse-devel zlib-devel
```

**Fedora:**
```bash
sudo dnf groupinstall -y "Development Tools"
sudo dnf install -y autoconf automake libtool pkgconfig fuse-devel zlib-devel
```

### 2. Download and Build MooseFS HA

```bash
# Clone or extract MooseFS source with HA extension
cd /usr/src
wget https://github.com/moosefs/moosefs/archive/v4.57.6-ha.tar.gz
tar -xzf v4.57.6-ha.tar.gz
cd moosefs-4.57.6-ha

# Configure build with HA support
./configure --prefix=/usr/local --with-ha-extension --enable-mfsha

# Build
make -j$(nproc)

# Install
sudo make install
```

### 3. Create System User and Directories

```bash
# Create mfs user and group
sudo useradd -r -s /bin/false -d /usr/local/var/mfs mfs

# Create required directories
sudo mkdir -p /usr/local/var/mfs
sudo mkdir -p /var/log/mfs
sudo mkdir -p /etc/mfs

# Set ownership
sudo chown -R mfs:mfs /usr/local/var/mfs
sudo chown -R mfs:mfs /var/log/mfs
sudo chown -R root:mfs /etc/mfs
sudo chmod 750 /etc/mfs
```

### 4. Install Configuration Files

```bash
# Copy configuration template
sudo cp /usr/src/moosefs-4.57.6-ha/mfsha/mfsha.cfg.template /usr/local/etc/mfsha.cfg
sudo chown root:mfs /usr/local/etc/mfsha.cfg
sudo chmod 640 /usr/local/etc/mfsha.cfg

# Copy admin script
sudo cp /usr/src/moosefs-4.57.6-ha/mfsha/mfsha-admin /usr/local/bin/mfsha-admin
sudo chmod 755 /usr/local/bin/mfsha-admin

# Install systemd service (if using systemd)
sudo cp /usr/src/moosefs-4.57.6-ha/mfsha/mfsha.service /etc/systemd/system/
sudo systemctl daemon-reload
```

### 5. Configure Logging

```bash
# Create logrotate configuration
sudo tee /etc/logrotate.d/mfsha > /dev/null << 'EOF'
/var/log/mfsha.log {
    daily
    missingok
    rotate 52
    compress
    delaycompress
    notifempty
    create 644 mfs mfs
    postrotate
        /bin/kill -HUP `cat /var/run/mfsha.pid 2>/dev/null` 2>/dev/null || true
    endscript
}
EOF
```

## Configuration

### 1. Basic Single-Node Setup (Development)

Edit `/usr/local/etc/mfsha.cfg`:
```ini
# Basic development configuration
MFSHA_NODE_ID = 1
MFSHA_PORT = 9430
MFSHA_PEERS = "localhost:9430"
HA_SHARD_COUNT = 8
HA_CRDT_MAX_MEMORY = 2147483648  # 2GB
DATA_PATH = /usr/local/var/mfs
```

### 2. Production 3-Node Cluster

**Node 1** (`/usr/local/etc/mfsha.cfg`):
```ini
MFSHA_NODE_ID = 1
MFSHA_PORT = 9430
MFSHA_PEERS = "node1.example.com:9430,node2.example.com:9430,node3.example.com:9430"
HA_SHARD_COUNT = 16
HA_CRDT_MAX_MEMORY = 17179869184  # 16GB
HA_RAFT_ELECTION_TIMEOUT = 5000
HA_RAFT_HEARTBEAT_TIMEOUT = 1000
DATA_PATH = /usr/local/var/mfs
```

**Node 2** (`/usr/local/etc/mfsha.cfg`):
```ini
MFSHA_NODE_ID = 2
MFSHA_PORT = 9430
MFSHA_PEERS = "node1.example.com:9430,node2.example.com:9430,node3.example.com:9430"
HA_SHARD_COUNT = 16
HA_CRDT_MAX_MEMORY = 17179869184  # 16GB
HA_RAFT_ELECTION_TIMEOUT = 5000
HA_RAFT_HEARTBEAT_TIMEOUT = 1000
DATA_PATH = /usr/local/var/mfs
```

**Node 3** (`/usr/local/etc/mfsha.cfg`):
```ini
MFSHA_NODE_ID = 3
MFSHA_PORT = 9430
MFSHA_PEERS = "node1.example.com:9430,node2.example.com:9430,node3.example.com:9430"
HA_SHARD_COUNT = 16
HA_CRDT_MAX_MEMORY = 17179869184  # 16GB
HA_RAFT_ELECTION_TIMEOUT = 5000
HA_RAFT_HEARTBEAT_TIMEOUT = 1000
DATA_PATH = /usr/local/var/mfs
```

## First Startup

### 1. Initialize First Node

```bash
# Start first node
sudo systemctl start mfsha

# Check status
sudo mfsha-admin status

# Watch logs
sudo mfsha-admin logs --follow
```

### 2. Add Additional Nodes

```bash
# Start remaining nodes (they will auto-join the cluster)
# Node 2:
sudo systemctl start mfsha

# Node 3:
sudo systemctl start mfsha

# Verify cluster formation
sudo mfsha-admin nodes
```

### 3. Enable Auto-Start

```bash
# Enable service on all nodes
sudo systemctl enable mfsha

# Verify auto-start works
sudo systemctl reboot
# After reboot:
sudo mfsha-admin status
```

## Integration with Existing MooseFS

### 1. Migrate from Single Master

If you have an existing MooseFS installation:

```bash
# 1. Stop existing master
sudo systemctl stop mfsmaster

# 2. Backup existing metadata
sudo cp /usr/local/var/mfs/metadata.mfs /usr/local/var/mfs/metadata.mfs.backup

# 3. Import metadata into HA cluster (on first node)
sudo -u mfs mfsha --import-metadata /usr/local/var/mfs/metadata.mfs

# 4. Start HA cluster
sudo systemctl start mfsha

# 5. Update client configurations to use HA cluster
# Clients can now connect directly to any HA node
# (No additional mfsmaster configuration needed)
```

### 2. Configure Clients

Clients connect directly to the HA cluster:

```bash
# Connect directly to HA cluster (clients auto-discover all nodes)
mfsmount /mnt/mfs -H node1.example.com,node2.example.com,node3.example.com
```

## Verification

### 1. Health Checks

```bash
# Basic health check
sudo mfsha-admin health --verbose

# Check cluster status
sudo mfsha-admin status

# Monitor logs
sudo mfsha-admin logs --lines=100
```

### 2. Failover Testing

```bash
# Test node failure (on node 2)
sudo systemctl stop mfsha

# Verify cluster still operates (check from node 1)
sudo mfsha-admin status

# Restart failed node
sudo systemctl start mfsha

# Verify recovery
sudo mfsha-admin status
```

### 3. Performance Testing

```bash
# Create test filesystem
mfsmount /tmp/mfs-test -H node1.example.com

# Run basic I/O test
dd if=/dev/zero of=/tmp/mfs-test/testfile bs=1M count=100
rm /tmp/mfs-test/testfile

# Unmount
fusermount -u /tmp/mfs-test
```

## Troubleshooting

### Common Issues

**Issue: "Configuration file not found"**
```bash
# Check file exists and permissions
ls -la /usr/local/etc/mfsha.cfg
sudo chown root:mfs /usr/local/etc/mfsha.cfg
sudo chmod 640 /usr/local/etc/mfsha.cfg
```

**Issue: "Cannot bind to port 9430"**
```bash
# Check if port is in use
sudo netstat -tlnp | grep :9430

# Check firewall
sudo ufw allow 9430/tcp  # Ubuntu
sudo firewall-cmd --add-port=9430/tcp --permanent  # CentOS/RHEL
```

**Issue: "Nodes cannot connect to each other"**
```bash
# Test connectivity
telnet node2.example.com 9430

# Check DNS resolution
nslookup node2.example.com

# Verify firewall rules
sudo iptables -L | grep 9430
```

**Issue: "High memory usage"**
```bash
# Check current memory usage
sudo mfsha-admin status

# Reduce cache size if needed
sudo sed -i 's/HA_CRDT_MAX_MEMORY.*/HA_CRDT_MAX_MEMORY = 4294967296/' /usr/local/etc/mfsha.cfg
sudo systemctl restart mfsha
```

### Log Analysis

```bash
# Check for errors
sudo grep -i error /var/log/mfsha.log

# Check cluster formation
sudo grep -i "cluster\|election\|leader" /var/log/mfsha.log

# Monitor real-time activity
sudo tail -f /var/log/mfsha.log | grep -E "(ERROR|WARN|INFO)"
```

## Performance Tuning

### Memory Optimization

```bash
# For high-memory systems (64GB+)
sudo sed -i 's/HA_CRDT_MAX_MEMORY.*/HA_CRDT_MAX_MEMORY = 34359738368/' /usr/local/etc/mfsha.cfg  # 32GB

# Increase shard count for better parallelism
sudo sed -i 's/HA_SHARD_COUNT.*/HA_SHARD_COUNT = 64/' /usr/local/etc/mfsha.cfg
```

### Network Optimization

```bash
# Tune for low-latency networks
sudo sed -i 's/HA_RAFT_HEARTBEAT_TIMEOUT.*/HA_RAFT_HEARTBEAT_TIMEOUT = 500/' /usr/local/etc/mfsha.cfg
sudo sed -i 's/HA_GOSSIP_INTERVAL.*/HA_GOSSIP_INTERVAL = 2.0/' /usr/local/etc/mfsha.cfg
```

### Storage Optimization

```bash
# Use SSD for metadata storage
sudo mkdir -p /mnt/ssd/mfs
sudo chown mfs:mfs /mnt/ssd/mfs
sudo sed -i 's|DATA_PATH.*|DATA_PATH = /mnt/ssd/mfs|' /usr/local/etc/mfsha.cfg
```

## Backup and Recovery

### Automated Backup

```bash
# Create backup script
sudo tee /usr/local/bin/mfsha-backup > /dev/null << 'EOF'
#!/bin/bash
BACKUP_DIR="/backup/mfsha/$(date +%Y%m%d)"
mkdir -p "$BACKUP_DIR"
sudo -u mfs mfsha --export-metadata "$BACKUP_DIR/metadata-$(hostname)-$(date +%H%M%S).mfs"
find /backup/mfsha -name "*.mfs" -mtime +30 -delete
EOF

sudo chmod +x /usr/local/bin/mfsha-backup

# Add to crontab
echo "0 2 * * * /usr/local/bin/mfsha-backup" | sudo tee -a /etc/crontab
```

### Disaster Recovery

```bash
# Restore from backup (new cluster)
sudo -u mfs mfsha --init-cluster --from-backup /backup/mfsha/20250614/metadata-node1-020000.mfs

# Verify restoration
sudo mfsha-admin health --verbose
```

This installation guide provides everything needed to successfully deploy MooseFS HA in production environments.