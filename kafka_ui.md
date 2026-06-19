# 📊 Kafka Monitoring UIs (Docker Setup Guide)

This document provides Docker-based setups for popular Kafka UI tools with and without security enabled.

---

## 🚀 1. Kafka UI (Provectus)

👉 Apache Kafka web UI for managing topics, messages, and consumer groups.

### 🔓 Kafka UI Without Security

```bash
docker run -d \
  --name kafka-ui \
  -p 8080:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=xxx.xxx.xx.xx:9092 \
  provectuslabs/kafka-ui
```

### 🔐 Kafka UI With SASL Security

```bash
docker run -d \
  --name kafka-ui \
  -p 8080:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=xxx.xxx.xx.xx:9092 \
  -e KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL=SASL_PLAINTEXT \
  -e KAFKA_CLUSTERS_0_PROPERTIES_SASL_MECHANISM=PLAIN \
  -e KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="spade" password="Vikas@123";' \
  provectuslabs/kafka-ui
```

---

## 🥇 2. AKHQ (Best for Production)

👉 A powerful open-source Kafka UI for production monitoring.

**✨ Features:**

- Multi-cluster support
- Topic & message browsing
- Consumer group monitoring
- Schema Registry support
- Kafka Connect integration
- LDAP / OAuth / OIDC authentication

### 🔓 AKHQ Without Security

```bash
docker run -d \
  --name akhq \
  -p 8080:8080 \
  -e AKHQ_CONFIGURATION='
akhq:
  connections:
    my-kafka:
      properties:
        bootstrap.servers: "xxx.xxx.xx.xx:9092"
' \
  tchiotludo/akhq:latest
```

### 🔐 AKHQ With SASL Security

```bash
docker run -d \
  --name akhq \
  -p 8080:8080 \
  -e AKHQ_CONFIGURATION='
akhq:
  connections:
    my-kafka:
      properties:
        bootstrap.servers: "xxx.xxx.xx.xx:9092"
        security.protocol: SASL_PLAINTEXT
        sasl.mechanism: PLAIN
        sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="YOUR_USERNAME" password="YOUR_PASSWORD";
' \
  tchiotludo/akhq:latest
```

### You can write separate configuration also

*Create one file `akhq.yml`*

```bash
micronaut:
  server:
    context-path: /kafka-server

akhq:
  security:
    enabled: false

  connections:
    my-kafka:
      properties:
        bootstrap.servers: "172.31.2.175:9092"
```

**Need to run via this command:**

```bash
sudo docker run -d \
  --name akhq \
  -p 8080:8080 \
  -v $(pwd)/akhq.yml:/app/application.yml \
  -e MICRONAUT_CONFIG_FILES=/app/application.yml \
  tchiotludo/akhq:latest
```
