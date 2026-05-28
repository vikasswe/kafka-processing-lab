# Kafka message Processing Lab

Production-grade Apache Kafka learning repository covering:

- Kafka KRaft setup
- Producer / Consumer internals
- Retention behavior
- Offsets
- Lag
- Backpressure
- Disk crash scenarios
- Monitoring
- Kafka failure debugging
- message processing architecture
- Production reliability concepts

---

# Architecture Goal

This repository simulates a real-world message processing system:

User places message
↓
message Service
↓
Kafka
↓
Satellite Microservices
↓
External Systems

The goal is to understand:

- what happens under heavy load
- why Kafka crashes
- retention behavior
- lag behavior
- consumer recovery
- offset mismatch
- disk exhaustion
- production-safe architecture

---

# Important Reality About Kafka

Kafka is NOT:

- permanent database
- long-term storage
- guaranteed infinite replay system

Kafka IS:

- distributed event stream
- high-throughput message pipeline
- temporary event retention system

Best production pattern:

Database = source of truth
Kafka = event transport

---

# CREATE IT MANUALLY

```bash
sudo mkdir -p /var/lib/kafka-logs
sudo chown -R $USER:$USER /var/lib/kafka-logs
sudo chown -R kafka:kafka /var/lib/kafka-logs
```

# Final Kafka Production-Style Configuration

File:

```bash
config/server.properties
```

```bash
##############################################
########### KAFKA KRaft CONFIG ###############
##############################################

process.roles=broker,controller
node.id=1

controller.quorum.voters=1@192.168.29.13:9093

listeners=PLAINTEXT://192.168.29.13:9092,CONTROLLER://192.168.29.13:9093
advertised.listeners=PLAINTEXT://192.168.29.13:9092

inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER

listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

##############################################
# STORAGE
##############################################

log.dirs=/var/lib/kafka-logs

num.partitions=3
num.recovery.threads.per.data.dir=2

##############################################
# INTERNAL TOPICS
##############################################

offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

##############################################
# RETENTION
##############################################

# delete data older than 3 hours
log.retention.hours=3

# segment size = 256 MB
log.segment.bytes=268435456

# check retention every 5 minutes
log.retention.check.interval.ms=300000

##############################################
# SAFETY
##############################################

auto.create.topics.enable=false

# avoid excessive disk flush pressure
log.flush.interval.ms=5000
log.flush.interval.messages=10000

##############################################
# ADDED FOR PRODUCTION STABILITY
##############################################

# prevents corrupted data from being elected
unclean.leader.election.enable=false

# ensures better durability even in single broker setups
min.insync.replicas=1
```

# Some commands to execute

## Topic Creation

```bash
bin/kafka-topics.sh \
--create \
--topic stress-topic \
--partitions 3 \
--replication-factor 1 \
--bootstrap-server 192.168.29.13:9092
```

## Describe topic

```bash
bin/kafka-topics.sh \
--describe \
--topic stress-topic \
--bootstrap-server 192.168.29.13:9092
```

## Delete topic

```bash
bin/kafka-topics.sh \
--delete \
--topic stress-topic \
--bootstrap-server 192.168.29.13:9092
```

## Consumer Group Commands

```bash
# Show lag
bin/kafka-consumer-groups.sh \
--describe \
--all-groups \
--bootstrap-server 192.168.29.13:9092

# Delete group
bin/kafka-consumer-groups.sh \
--delete \
--group stress-group \
--bootstrap-server 192.168.29.13:9092
```

## Monitoring Commands

```bash
# Disk usage
df -h

# Kafka log directory size
du -sh /var/lib/kafka-logs

# File count
find /var/lib/kafka-logs -type f | wc -l

# Watch memory continuously
watch -n 2 free -h

# Kafka JVM GC monitoring
PID=$(jps | grep Kafka | awk '{print $1}')

jstat -gc $PID 1000 5

# Open file descriptors
lsof -p $PID | wc -l

# Memory/OOM check
dmesg -T | grep -i -E 'killed process|out of memory|oom'

# Open file limits
ulimit -n

# Kafka JVM memory
ps -ef | grep Xmx

# Check if Swap Exists
swapon --show

# Check Linux Memory
free -h

# Broker logs (MOST IMPORTANT)
grep -i "error" logs/server.log | tail -100
grep -i "exception" logs/server.log | tail -100
grep -i "fatal" logs/server.log | tail -100

# Kafka logs
grep -i "log dir" logs/server.log

# Broker shutdown reason
grep -i "shutdown" logs/server.log

# Consumer lag before crash
bin/kafka-consumer-groups.sh --describe --all-groups --bootstrap-server 192.168.29.13:9092

# Retention cleanup
grep -i "Deleting segment" logs/server.log

# Lag Growth Rate
watch -n 5 '
/usr/local/kafka-4.1.0-src/bin/kafka-consumer-groups.sh \
--bootstrap-server 192.168.29.13:9092 \
--describe --all-groups
'

# Watch memory continuously
watch -n 2 free -h

```

# Kafka UI (BEST)

```bash
docker run -d \
--name kafka-ui \
-p 8080:8080 \
-e KAFKA_CLUSTERS_0_NAME=local \
-e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=192.168.29.13:9092 \
provectuslabs/kafka-ui
```

# Use of terms

## 📌 WHY THESE SETTINGS MATTER

- 🔵 log.dirs=/var/lib/kafka-logs
  - where Kafka stores real data
  - if disk fills → broker crashes (you already saw this)
- 🔵 log.retention.hours=3
  - Kafka deletes messages older than 3 hours
  - not based on consumption
  - based on TIME only
- 🔵 partitions=3
  - parallelism
  - multiple consumers can read faster
- 🔵 flush settings
  - controls disk write behavior
  - too aggressive = slow + unstable
  - too relaxed = slight risk in crash
- 🔵 OFFSET
  - Offset = message number inside partition
  - `Example: 0, 1, 2, 3, 4...`
  - 👉 Kafka NEVER deletes offsets
  - 👉 Even if data is deleted, offsets continue increasing
- 🔵 FIRST OFFSET
  - Oldest available message in Kafka
- 🔵 NEXT OFFSET
  - Where next message will be written
- 🔵 LAG
  - lag = produced - consumed
- 🔵 RETENTION
  - Kafka deletes based on:
  - `✔ time (log.retention.hours)`
  - `✔ size (log.segment.bytes)`
  - NOT based on consumption
- 🔵 HEARTBEAT
  - Consumer says: `I am alive`
  - If heartbeat stops → Kafka removes consumer from group
- 🔵 POLL
  - Poll does:-
  - `✔ fetch messages`
  - `✔ send heartbeat`
  - If you don’t poll → consumer is considered dead

---

# Pro/Con via terminal

```sh
############################################################
# CHECK CONSUMER LAG
############################################################

watch -n 5 '
/usr/local/kafka-4.1.0-src/bin/kafka-consumer-groups.sh \
--bootstrap-server 192.168.29.13:9092 \
--describe \
--all-groups
'

############################################################
# CONSOLE PRODUCER
############################################################

bin/kafka-console-producer.sh \
--bootstrap-server 192.168.29.13:9092 \
--topic stress-topic

############################################################
# CONSOLE CONSUMER
############################################################

bin/kafka-console-consumer.sh \
--bootstrap-server 192.168.29.13:9092 \
--topic stress-topic \
--from-beginning
```

# Installation Documentation

```sh
# ============================================================================
# Kafka 4.0.0 Setup Script (KRaft Mode - No ZooKeeper)
# ============================================================================
# This script includes full documentation, setup instructions, and a working
# configuration for running Apache Kafka 4.0.0 in KRaft mode.
# It explains control flow, UUID usage, restart instructions, and includes
# producer/consumer command examples.
# ============================================================================

# ---------------------------------------------
# 📌 Kafka Without ZooKeeper: The Transition to KRaft
# ---------------------------------------------
# In this script and documentation:
# - Overview of ZooKeeper removal
# - KRaft architecture
# - Comparison with old system
# - Step-by-step setup
# - Explanation of how Kafka works internally
# - Restart instructions, UUID role
# - Producer/Consumer usage

# ----------------------------------------------------------------------------
# 🛠 Why Did Kafka Remove ZooKeeper?
# ----------------------------------------------------------------------------
# - ZooKeeper was a separate coordination system used for metadata & leader election.
# - Problems with ZooKeeper:
#   1. External dependency
#   2. Slow leader elections
#   3. Poor scalability with large clusters
#   4. Operational overhead
#
# KRaft (Kafka Raft) replaces ZooKeeper with an internal Raft-based consensus protocol.
# Benefits of KRaft:
# ✅ Simpler deployment
# ✅ Faster leader election (milliseconds)
# ✅ Higher scalability
# ✅ Lower maintenance

# ----------------------------------------------------------------------------
# 🏗 Kafka’s Old vs. New Architecture
# ----------------------------------------------------------------------------
# OLD (ZooKeeper):
# - Metadata stored externally
# - Slower failover, more latency
#
# NEW (KRaft):
# - Kafka brokers manage metadata internally
# - Uses Raft log for consensus
# - Brokers are both data handlers and metadata controllers

# ----------------------------------------------------------------------------
# ⚖️ ZooKeeper vs KRaft Comparison
# ----------------------------------------------------------------------------
# | Feature               | ZooKeeper        | KRaft            |
# |----------------------|------------------|------------------|
# | Metadata Management  | External (ZK)     | Internal (Kafka) |
# | Leader Election      | Seconds           | Milliseconds     |
# | Scalability          | Limited           | High             |
# | Setup Complexity     | High              | Low              |
# | Failure Recovery     | Slower            | Faster           |
```

## STEP 1: Download & Build Kafka 4.0.0

```sh
wget https://www.apache.org/dyn/closer.lua/kafka/4.2.0/kafka_2.13-4.2.0.tgz

tar -xvzf kafka_2.13-4.2.0.tgz
cd kafka_2.13-4.2.0

```

## STEP 2: Configure Environment Variables

```bash
sudo nano ~/.bashrc
```

```bash
export KAFKA_HOME=/usr/local/kafka_2.13-4.2.0
export PATH=$PATH:$KAFKA_HOME/bin
```

**Apply:**

```bash
source ~/.bashrc

# Then verify:
echo $KAFKA_HOME

```

## STEP 3: Update config

```bash
sudo nano /usr/local/kafka_2.13-4.2.0/config/server.properties
```

> Add config that given above line (84 to 139)

## STEP 4: Generate Cluster UUID & Format Storage

```bash
echo "Generating Cluster UUID..."
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
echo "Generated Cluster ID: $CLUSTER_ID"
bin/kafka-storage.sh format -t $CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties
# 🔁 AFTER REBOOT / SYSTEM RESTART
# ✅ You do NOT need to generate UUID again.
# ✅ You do NOT need to format again.
# - Kafka Broker now also acts as the metadata controller.
# - Uses Raft consensus to elect a controller among quorum voters.
# - Topics are created and stored in replicated logs.
# - Metadata is persisted internally, removing need for ZooKeeper.
# - Fast failover: if controller fails, another broker takes over quickly.
```

# Clean kafka

```bash
# ==============================
# STOP KAFKA
# ==============================

pkill -f kafka.Kafka
pkill -f kafka-server-start

# ==============================
# REMOVE KAFKA INSTALLATION
# ==============================

sudo rm -rf /usr/local/kafka_2.13-4.2.0

# if symlink exists
sudo rm -rf /usr/local/kafka

# ==============================
# REMOVE KAFKA LOG DATA
# ==============================

sudo rm -rf /var/lib/kafka-logs

# old tmp logs (if previously used)
sudo rm -rf /tmp/kraft-combined-logs

# ==============================
# REMOVE LOCAL LOG FILES
# ==============================

rm -f ~/kafka.log
rm -f ~/nohup.out

# ==============================
# REMOVE ENV VARIABLES
# ==============================

sed -i '/KAFKA_HOME/d' ~/.bashrc
sed -i '/kafka_2.13-4.2.0\/bin/d' ~/.bashrc

source ~/.bashrc

# ==============================
# VERIFY CLEANUP
# ==============================

echo "==== CHECK ===="

which kafka-server-start.sh
ls -la /usr/local | grep kafka
ls -la /var/lib | grep kafka

echo "Kafka cleanup completed."
```

# Security in kafka (SASL SECURITY)

**THIS LEARN:**

- Next Kafka security topics:
  - ACLs
  - TLS
  - SASL SCRAM
  - Topic permissions
  - Producer quotas
  - Schema registry
  - Secret management
  - mTLS

## Since you now want username/password authentication, remove the old PLAINTEXT block completely for SASL

