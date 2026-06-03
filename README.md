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

```bash
config/server.properties
```

```bash
##############################################
########### KAFKA KRaft CONFIG ###############
##############################################

process.roles=broker,controller
node.id=1

controller.quorum.voters=1@xxx.xxx.xx.xx:9093

listeners=PLAINTEXT://xxx.xxx.xx.xx:9092,CONTROLLER://xxx.xxx.xx.xx:9093
advertised.listeners=PLAINTEXT://xxx.xxx.xx.xx:9092

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
```

# Some commands to execute

## Topic Creation

```bash
bin/kafka-topics.sh \
--create \
--topic stress-topic \
--partitions 3 \
--replication-factor 1 \
--bootstrap-server xxx.xxx.xx.xx:9092
```

## Describe topic

```bash
bin/kafka-topics.sh \
--describe \
--topic stress-topic \
--bootstrap-server xxx.xxx.xx.xx:9092
```

## Delete topic

```bash
bin/kafka-topics.sh \
--delete \
--topic stress-topic \
--bootstrap-server xxx.xxx.xx.xx:9092
```

## Consumer Group Commands

```bash
# Show lag
bin/kafka-consumer-groups.sh \
--describe \
--all-groups \
--bootstrap-server xxx.xxx.xx.xx:9092

# Delete group
bin/kafka-consumer-groups.sh \
--delete \
--group stress-group \
--bootstrap-server xxx.xxx.xx.xx:9092
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
bin/kafka-consumer-groups.sh --describe --all-groups --bootstrap-server xxx.xxx.xx.xx:9092

# Retention cleanup
grep -i "Deleting segment" logs/server.log

# Lag Growth Rate
watch -n 5 '
/usr/local/kafka-4.1.0-src/bin/kafka-consumer-groups.sh \
--bootstrap-server xxx.xxx.xx.xx:9092 \
--describe --all-groups
'

# Watch memory continuously
watch -n 2 free -h

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
--bootstrap-server xxx.xxx.xx.xx:9092 \
--describe \
--all-groups
'

############################################################
# CONSOLE PRODUCER
############################################################

bin/kafka-console-producer.sh \
--bootstrap-server xxx.xxx.xx.xx:9092 \
--topic stress-topic

############################################################
# CONSOLE CONSUMER
############################################################

bin/kafka-console-consumer.sh \
--bootstrap-server xxx.xxx.xx.xx:9092 \
--topic stress-topic \
--from-beginning
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

```bash
config/server.properties
```

```sh
##############################################
########### KAFKA KRaft CONFIG ###############
##############################################

process.roles=broker,controller
node.id=1

controller.quorum.voters=1@xxx.xxx.xx.xx:9093

##############################################
# LISTENERS
##############################################

listeners=SASL_PLAINTEXT://xxx.xxx.xx.xx:9092,CONTROLLER://xxx.xxx.xx.xx:9093

advertised.listeners=SASL_PLAINTEXT://xxx.xxx.xx.xx:9092

inter.broker.listener.name=SASL_PLAINTEXT

controller.listener.names=CONTROLLER

listener.security.protocol.map=CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT

##############################################
# SASL AUTH
##############################################

sasl.enabled.mechanisms=PLAIN

sasl.mechanism.inter.broker.protocol=PLAIN

security.inter.broker.protocol=SASL_PLAINTEXT

sasl.mechanism.controller.protocol=PLAIN

listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
username="spade" \
password="Vikas@123" \
user_spade="Vikas@123";

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

log.retention.hours=3

log.segment.bytes=268435456

log.retention.check.interval.ms=300000

##############################################
# SAFETY
##############################################

auto.create.topics.enable=false

log.flush.interval.ms=5000

log.flush.interval.messages=10000
```

## Create

```bash
sudo nano /usr/local/kafka-4.1.0-src/client.properties
```

```bash
security.protocol=SASL_PLAINTEXT

sasl.mechanism=PLAIN

sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
username="spade" \
password="Vikas@123";
```

> WHAT EACH SECURITY LINE MEANS

```table
| Config                        | Meaning                            |
| ----------------------------- | ---------------------------------- |
| `SASL_PLAINTEXT`              | username/password auth             |
| `PLAIN`                       | simple username/password mechanism |
| `listener.name...jaas.config` | defines allowed users              |
| `user_spade="Vikas@123"`      | actual allowed client              |
| `inter.broker.listener.name`  | broker internal communication      |
| `client.properties`           | CLI/client authentication          |

```

## Delete ALL Kafka data completely

- ⚠️ This removes:
  - topics
  - consumer groups
  - offsets
  - retained messages
  - metadata

```bash
sudo rm -rf /var/lib/kafka-logs/*
rm -rf logs/*
```

## Create topic for security

```bash
# List topics
bin/kafka-topics.sh \
--list \
--bootstrap-server xxx.xxx.xx.xx:9092 \
--command-config client.properties

# Create
bin/kafka-topics.sh \
--create \
--topic stress-topic \
--partitions 3 \
--replication-factor 1 \
--bootstrap-server xxx.xxx.xx.xx:9092 \
--command-config client.properties
```

## Verify clean state

```bash
bin/kafka-topics.sh \
--list \
--bootstrap-server xxx.xxx.xx.xx:9092 \
--command-config client.properties
```

## Verify groups

```bash
bin/kafka-consumer-groups.sh \
--list \
--bootstrap-server xxx.xxx.xx.xx:9092 \
--command-config client.properties
```

> ⚠️ BUT in SASL_PLAINTEXT:

*password is sent over network during authentication handshake.*

```table
| Layer    | Security                   |
| -------- | -------------------------- |
| Payload  | Safe                       |
| Network  | NOT encrypted              |
| Password | Visible to packet sniffing |
```

- So what does SASL_PLAINTEXT mean?
  - Authentication = YES
  - Encryption = NO

- So password is protected from users at app level
- BUT NOT protected from network sniffing tools like Wireshark.

- What is SASL_SSL?
  - This is enterprise-level Kafka security.
  - `Authentication + Encryption`

```table
| Mode           | Auth | Encryption | Safe in production |
| -------------- | ---- | ---------- | ------------------ |
| PLAINTEXT      | ❌    | ❌          | ❌                  |
| SASL_PLAINTEXT | ✅    | ❌          | ⚠️ (dev only)      |
| SASL_SSL       | ✅    | ✅          | ✅ production       |
```

- What changes in SASL_SSL?
  - Instead of: `listeners=SASL_PLAINTEXT://host:9092`
  - You use: `listeners=SASL_SSL://host:9093`

- And you add SSL certs:
  - keystore (server identity)
  - truststore (client trust)
  - certificates

- What it gives you
  - Encrypted traffic (TLS)
  - Password hidden in network
  - Secure microservice communication
  - Production-grade security

- Production stage
  - You MUST move to: `SASL_SSL + ACLs`
