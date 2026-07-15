# Kafka: The Definitive Guide — 2nd Edition

> **Real-Time Data and Stream Processing at Scale**
>
> Authors: Gwen Shapira, Todd Palino, Rajini Sivaram & Krit Petty
>
> Published by O'Reilly Media, November 2021
>
> ISBN: 978-1-492-04308-9

---

## Table of Contents

- [Foreword to the Second Edition](#foreword-to-the-second-edition)
- [Foreword to the First Edition](#foreword-to-the-first-edition)
- [Preface](#preface)
- [1. Meet Kafka](#1-meet-kafka)
- [2. Installing Kafka](#2-installing-kafka)
- [3. Kafka Producers: Writing Messages to Kafka](#3-kafka-producers-writing-messages-to-kafka)
- [4. Kafka Consumers: Reading Data from Kafka](#4-kafka-consumers-reading-data-from-kafka)
- [5. Managing Apache Kafka Programmatically](#5-managing-apache-kafka-programmatically)
- [6. Kafka Internals](#6-kafka-internals)
- [7. Reliable Data Delivery](#7-reliable-data-delivery)
- [8. Exactly-Once Semantics](#8-exactly-once-semantics)
- [9. Building Data Pipelines](#9-building-data-pipelines)
- [10. Cross-Cluster Data Mirroring](#10-cross-cluster-data-mirroring)
- [11. Securing Kafka](#11-securing-kafka)
- [12. Administering Kafka](#12-administering-kafka)
- [13. Monitoring Kafka](#13-monitoring-kafka)
- [14. Stream Processing](#14-stream-processing)
- [Appendix A: Installing Kafka on Other Operating Systems](#appendix-a-installing-kafka-on-other-operating-systems)
- [Appendix B: Additional Kafka Tools](#appendix-b-additional-kafka-tools)

---

## Foreword to the Second Edition

By Jay Kreps, Cofounder and CEO at Confluent

Apache Kafka is now used in over 70% of Fortune 500 companies. It has become the de facto foundation for managing data in motion. The community has evolved existing APIs, configuration options, metrics, and tools, while also introducing:

- A new programmatic administration API (AdminClient)
- Next-generation global replication and DR with MirrorMaker 2.0
- A new Raft-based consensus protocol (KRaft) allowing Kafka to run without ZooKeeper
- Tiered storage support for true elasticity
- Advanced security options—authentication, authorization, and encryption

Most Kafka installations are now in the cloud, with use cases ranging from ETL and messaging to event-driven microservices, real-time stream processing, IoT, and machine learning pipelines.

---

## Foreword to the First Edition

By Jay Kreps

Kafka was created as an internal infrastructure system at LinkedIn. The idea was simple: instead of focusing on holding piles of data like traditional databases, Kafka focuses on treating data as a continually evolving and ever-growing stream.

Kafka is a **streaming platform**: a system that lets you publish and subscribe to streams of data, store them, and process them. Key differences from traditional messaging systems:

1. **Distributed system** — runs as a cluster and scales elastically
2. **True storage system** — data is replicated, persistent, and retained as long as needed
3. **Stream processing** — compute derived streams and datasets dynamically

---

## Preface

This book covers everything developers and SREs need to use Kafka to its full potential, from basic APIs and configuration to the latest capabilities.

### Who Should Read This Book

- **Software engineers** developing applications that use Kafka's APIs
- **Production engineers (SREs, DevOps, sysadmins)** installing, configuring, tuning, and monitoring Kafka
- **Data architects and data engineers** designing data infrastructure

Chapters 3, 4, and 14 are geared toward Java developers. Chapters 2, 10, 12, and 13 assume Linux experience.

---

## 1. Meet Kafka

### Publish/Subscribe Messaging

Publish/subscribe (pub/sub) messaging is characterized by the sender (publisher) not specifically directing a message to a receiver. Instead, the publisher classifies the message, and the receiver (subscriber) subscribes to receive certain classes of messages. Pub/sub systems often have a **broker** as a central point.

### Messages and Batches

The **message** is the unit of data within Kafka—an array of bytes with no specific format. A message can have an optional **key** (also a byte array) used for partition assignment.

For efficiency, messages are written in **batches**—collections of messages produced to the same topic and partition. This reduces network overhead at a latency trade-off. Batches are typically compressed.

### Schemas

Messages are opaque byte arrays to Kafka, but imposing a **schema** is recommended. Options include:

- **Apache Avro** — compact serialization, strong typing, schema evolution (backward and forward compatibility)
- **JSON/XML** — simple and human-readable but lack robust type handling

### Topics and Partitions

Messages are categorized into **topics** (analogous to database tables). Topics are broken down into **partitions**—single append-only logs. Message ordering is guaranteed within a partition, not across a topic. Partitions provide redundancy and scalability.

### Producers and Consumers

- **Producers** create new messages, typically balancing messages across partitions. Keys and a partitioner control partition assignment.
- **Consumers** read messages as part of a **consumer group**, where each partition is consumed by only one member of the group. Consumers track their position using **offsets**.

### Brokers and Clusters

A single Kafka server is a **broker**. Brokers operate as part of a **cluster**, with one broker functioning as the **controller** (elected automatically). A partition has one **leader** and possibly multiple **followers** for replication.

**Retention** is the durable storage of messages for a configurable period. Topics can also be **log compacted**, retaining only the last message with a specific key.

### Multiple Clusters

As deployments grow, multiple clusters become useful for:
- Segregation of data types
- Security isolation
- Multiple datacenters (disaster recovery)

**MirrorMaker** is used for replicating data between clusters.

### Why Kafka?

- **Multiple Producers** — seamless handling of many producers
- **Multiple Consumers** — designed for multiple consumers reading without interference
- **Disk-Based Retention** — durable retention means consumers don't need to work in real time
- **Scalable** — flexible scalability from single broker to hundreds
- **High Performance** — subsecond latency under high load
- **Platform Features** — Kafka Connect for data integration, Kafka Streams for stream processing

### Use Cases

- **Activity Tracking** — the original use case at LinkedIn
- **Messaging** — applications sending notifications
- **Metrics and Logging** — collecting application and system metrics
- **Commit Log** — database change data capture
- **Stream Processing** — real-time data processing

### Kafka's Origin

Created at LinkedIn by Jay Kreps, Neha Narkhede, and Jun Rao to address the data pipeline problem. Released as open source in 2010, accepted as an Apache incubator project in July 2011, and graduated in October 2012.

---

## 2. Installing Kafka

### Environment Setup

- **Operating System** — Linux is recommended
- **Java** — OpenJDK-based Java (Java 8 or Java 11)
- **ZooKeeper** — Used by Kafka for storing cluster metadata (tested with ZooKeeper 3.5.x)

### Installing ZooKeeper

**Standalone server**:
```bash
tar -zxf apache-zookeeper-3.5.9-bin.tar.gz
mv apache-zookeeper-3.5.9-bin /usr/local/zookeeper
mkdir -p /var/lib/zookeeper
```

**Ensemble** (cluster): ZooKeeper works as an ensemble with an odd number of servers (3, 5, or 7). Configuration includes `initLimit`, `syncLimit`, and server definitions with peer and leader ports.

### Installing a Kafka Broker

```bash
tar -zxf kafka_2.13-2.7.0.tgz
mv kafka_2.13-2.7.0 /usr/local/kafka
mkdir /tmp/kafka-logs
/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties
```

### Configuring the Broker

Key broker configuration parameters:

- **broker.id** — Unique integer identifier for each broker
- **listeners** — Comma-separated list of URIs (e.g., `PLAINTEXT://localhost:9092`)
- **zookeeper.connect** — ZooKeeper connection string (chroot path recommended)
- **log.dirs** — Comma-separated list of paths for storing log segments
- **num.recovery.threads.per.data.dir** — Threads for handling log segments during startup/shutdown
- **auto.create.topics.enable** — Whether to auto-create topics (default: true)
- **delete.topic.enable** — Whether to allow topic deletion

**Topic Defaults**:

- **num.partitions** — Default partition count for new topics (default: 1)
- **default.replication.factor** — Default replication factor
- **log.retention.ms** — How long to retain messages (default: 168 hours / 1 week)
- **log.retention.bytes** — Retention by total bytes per partition
- **log.segment.bytes** — Size of log segments (default: 1 GB)
- **min.insync.replicas** — Minimum number of in-sync replicas for durability
- **message.max.bytes** — Maximum message size (default: 1 MB)

### Selecting Hardware

- **Disk Throughput** — Faster disk writes = lower produce latency. SSDs provide best performance.
- **Disk Capacity** — Determined by retention requirements (e.g., 1 TB/day × 7 days = 7 TB minimum)
- **Memory** — More memory for page cache improves consumer performance. Kafka itself uses modest heap (e.g., 5 GB).
- **Networking** — 10 Gb NICs recommended
- **CPU** — Less critical but matters for compression at scale

### Kafka in the Cloud

- **Microsoft Azure** — Standard D16s v3 for smaller clusters, D64s v4 for larger. Use Azure Managed Disks.
- **Amazon Web Services** — m4 or r3 instance types common

### Configuring Kafka Clusters

**How many brokers?** Determined by disk capacity, replica capacity per broker, CPU capacity, and network capacity.

**Broker Configuration** — All brokers must have the same `zookeeper.connect` and unique `broker.id`.

### OS Tuning

- **Virtual Memory** — Set `vm.swappiness=1`, tune `vm.dirty_background_ratio` and `vm.dirty_ratio`
- **Disk** — Use XFS filesystem with `noatime` mount option
- **Networking** — Increase socket buffer sizes, enable TCP window scaling

### Production Concerns

- **Garbage Collector** — Use G1GC with `MaxGCPauseMillis=20` and `InitiatingHeapOccupancyPercent=35`
- **Datacenter Layout** — Use rack-aware replica placement (`broker.rack` configuration)
- **Colocating Applications on ZooKeeper** — Share ZooKeeper ensemble across multiple Kafka clusters using chroot paths

---

## 3. Kafka Producers: Writing Messages to Kafka

### Producer Overview

The Kafka producer uses the following flow:
1. Create a `ProducerRecord` (topic, value, optional key, partition, timestamp, headers)
2. Serialize the key and value objects to byte arrays
3. Send to a partitioner (if no partition specified)
4. Add to a batch of records for the same topic and partition
5. A separate thread sends batches to Kafka brokers
6. Broker responds with `RecordMetadata` (topic, partition, offset) or an error

### Constructing a Kafka Producer

Three mandatory properties:
- `bootstrap.servers` — List of `host:port` pairs for initial connection
- `key.serializer` — Class to serialize message keys
- `value.serializer` — Class to serialize message values

```java
Properties kafkaProps = new Properties();
kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");
kafkaProps.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer");
kafkaProps.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer");
producer = new KafkaProducer<String, String>(kafkaProps);
```

### Sending a Message

Three primary methods:
- **Fire-and-forget** — No concern about success
- **Synchronous send** — Using `Future.get()` to wait for response
- **Asynchronous send** — Using a `Callback` function

### Configuring Producers

Key configuration parameters:
- **client.id** — Logical identifier for the client
- **acks** — Controls durability: `0` (no wait), `1` (leader only), `all` (all in-sync replicas)
- **max.block.ms** — How long `send()` blocks when buffer is full
- **delivery.timeout.ms** — Total time until send succeeds or fails
- **request.timeout.ms** — Time to wait for each produce request response
- **retries** — Number of retry attempts
- **linger.ms** — Time to wait for additional messages before sending batch
- **buffer.memory** — Memory for buffering messages
- **compression.type** — `snappy`, `gzip`, `lz4`, or `zstd`
- **batch.size** — Maximum bytes per batch
- **max.in.flight.requests.per.connection** — Concurrent in-flight requests
- **max.request.size** — Maximum request size
- **enable.idempotence** — Exactly-once semantics (requires `acks=all`, `retries>0`, `max.in.flight.requests<=5`)

### Serializers

- **Custom Serializers** — Possible but fragile; not recommended
- **Apache Avro** — Recommended for its schema evolution, compact format, and compatibility

Using Avro with Schema Registry:
```java
props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("schema.registry.url", schemaUrl);
```

### Partitions

- **Default partitioner** — Hashes the key to determine partition (consistent mapping)
- **Null keys** — Round-robin distribution (sticky partitioning since Kafka 2.4)
- **Custom partitioner** — Implement `Partitioner` interface for custom logic

### Headers

Record headers provide metadata about the record. Implemented as an ordered collection of key/value pairs.

### Interceptors

`ProducerInterceptor` allows modifying producer behavior without changing client code:
- `onSend()` — Called before the record is serialized and sent
- `onAcknowledgement()` — Called when Kafka responds with an ack

### Quotas and Throttling

Three quota types:
- **Produce** — Limits write rate (bytes/second)
- **Consume** — Limits read rate (bytes/second)
- **Request** — Limits percentage of time spent processing requests

---

## 4. Kafka Consumers: Reading Data from Kafka

### Consumers and Consumer Groups

Consumers are typically part of a **consumer group**. Each consumer in a group receives messages from a different subset of partitions. To scale consumption, add more consumers (up to the number of partitions in the topic).

**Key rule**: If you need an application to get all messages, give it its own consumer group.

### Consumer Groups and Partition Rebalance

Two types of rebalances:

1. **Eager rebalances** — All consumers stop, give up ownership, rejoin, and get new assignments
2. **Cooperative rebalances** (incremental) — Only a small subset of partitions are reassigned; consumers continue processing unaffected partitions

**Static Group Membership** — Using `group.instance.id` allows consumers to leave and rejoin without triggering rebalances.

### Creating a Kafka Consumer

Mandatory properties: `bootstrap.servers`, `key.deserializer`, `value.deserializer`. Common property: `group.id`.

```java
Properties props = new Properties();
props.put("bootstrap.servers", "broker1:9092,broker2:9092");
props.put("group.id", "CountryCounter");
props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer");
KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
```

### Subscribing to Topics

```java
consumer.subscribe(Collections.singletonList("customerCountries"));
// Or with regex:
consumer.subscribe(Pattern.compile("test.*"));
```

### The Poll Loop

```java
Duration timeout = Duration.ofMillis(100);
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(timeout);
    for (ConsumerRecord<String, String> record : records) {
        // process record
    }
}
```

### Configuring Consumers

- **fetch.min.bytes** — Minimum data to return per fetch (default: 1 byte)
- **fetch.max.wait.ms** — Maximum time to wait for min bytes (default: 500 ms)
- **fetch.max.bytes** — Maximum bytes returned per poll (default: 50 MB)
- **max.poll.records** — Maximum records returned per poll
- **max.partition.fetch.bytes** — Maximum bytes per partition (default: 1 MB)
- **session.timeout.ms** — Consumer heartbeat timeout (default: 10 seconds)
- **heartbeat.interval.ms** — Heartbeat frequency (typically 1/3 of session timeout)
- **max.poll.interval.ms** — Maximum time between polls before considered dead (default: 5 minutes)
- **auto.offset.reset** — Behavior when no committed offset: `latest`, `earliest`, or `none`
- **enable.auto.commit** — Whether to auto-commit offsets (default: true)
- **partition.assignment.strategy** — `Range`, `RoundRobin`, `Sticky`, or `CooperativeSticky`

### Commits and Offsets

**Automatic Commit** — Simplest but limited control over duplicates

**Commit Current Offset** (`commitSync()`):
```java
try {
    consumer.commitSync();
} catch (CommitFailedException e) {
    log.error("commit failed", e);
}
```

**Asynchronous Commit** (`commitAsync()`):
```java
consumer.commitAsync();
```

**Combined approach** — Use `commitAsync()` during normal operation and `commitSync()` before shutdown

**Committing Specified Offsets** — Pass a map of partitions and offsets to `commitSync()` or `commitAsync()`

### Rebalance Listeners

Implement `ConsumerRebalanceListener` with:
- `onPartitionsAssigned()` — Called after partitions are assigned
- `onPartitionsRevoked()` — Called before partitions are taken away
- `onPartitionsLost()` — Called when partitions are lost without revocation (cooperative rebalancing)

### Consuming Records with Specific Offsets

```java
consumer.seekToBeginning(Collection<TopicPartition>);
consumer.seekToEnd(Collection<TopicPartition>);
consumer.seek(topicPartition, offset);
```

### Deserializers

- **Custom Deserializers** — Possible but not recommended
- **Avro Deserialization** — Use `KafkaAvroDeserializer` with Schema Registry

### Standalone Consumer

```java
consumer.assign(partitions);
```

---

## 5. Managing Apache Kafka Programmatically

### AdminClient Overview

Kafka's `AdminClient` provides a programmatic API for administrative operations:
- Topic management (create, list, describe, delete)
- Configuration management
- Consumer group management
- Advanced operations (partition reassignment, leader election)

### Key Design Principles

- **Asynchronous and Eventually Consistent** — Each method returns Future objects; metadata propagation is eventually consistent
- **Options** — Each method accepts an Options object (e.g., `ListTopicsOptions`)
- **Flat Hierarchy** — All operations are implemented directly in `AdminClient`

### Creating, Configuring, and Closing AdminClient

```java
Properties props = new Properties();
props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
AdminClient admin = AdminClient.create(props);
// ... use admin ...
admin.close(Duration.ofSeconds(30));
```

### Essential Topic Management

**Listing topics**:
```java
ListTopicsResult topics = admin.listTopics();
topics.names().get().forEach(System.out::println);
```

**Describing and creating topics**:
```java
DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST);
CreateTopicsResult newTopic = admin.createTopics(
    Collections.singletonList(new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR)));
```

**Deleting topics**:
```java
admin.deleteTopics(TOPIC_LIST).all().get();
```

### Configuration Management

```java
ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
DescribeConfigsResult configsResult = admin.describeConfigs(Collections.singleton(configResource));
```

### Consumer Group Management

**Exploring groups**:
```java
admin.listConsumerGroups().valid().get().forEach(System.out::println);
```

**Describing groups**:
```java
ConsumerGroupDescription groupDescription = admin.describeConsumerGroups(CONSUMER_GRP_LIST)
    .describedGroups().get(CONSUMER_GROUP).get();
```

**Modifying offsets**:
```java
admin.alterConsumerGroupOffsets(CONSUMER_GROUP, resetOffsets).all().get();
```

### Cluster Metadata

```java
DescribeClusterResult cluster = admin.describeCluster();
System.out.println("Connected to cluster " + cluster.clusterId().get());
```

### Advanced Operations

- **Adding Partitions** — `admin.createPartitions()`
- **Deleting Records** — `admin.deleteRecords()`
- **Leader Election** — `admin.electLeaders()`
- **Reassigning Replicas** — `admin.alterPartitionReassignments()`

### Testing

Use `MockAdminClient` for unit testing without running a Kafka cluster.

---

## 6. Kafka Internals

### Cluster Membership

Kafka uses ZooKeeper to maintain the list of active brokers. Each broker creates an **ephemeral node** in ZooKeeper when starting. When a broker loses ZooKeeper connectivity, its node is removed.

### The Controller

The controller is a broker responsible for electing partition leaders. Key characteristics:
- The first broker to start becomes the controller by creating an ephemeral node in ZooKeeper
- Uses **controller epoch** numbers to prevent split-brain scenarios (zombie fencing)
- When a broker leaves the cluster, the controller determines new leaders for affected partitions
- Sends `LeaderAndISR` requests to brokers with new leadership information

### KRaft: Kafka's New Raft-Based Controller

Starting in Kafka 2.8 (preview), KRaft replaces the ZooKeeper-based controller. Benefits:
- Eliminates ZooKeeper dependency
- Uses a Raft quorum of controller nodes managing a metadata event log
- Faster controller failover (no lengthy reloading)
- Brokers fetch metadata updates via new `MetadataFetch` API
- Designed to handle millions of partitions

### Replication

**Two types of replicas:**
- **Leader replica** — Handles all produce requests to guarantee consistency
- **Follower replicas** — Replicate messages from the leader; stay up-to-date

**Read from Follower** (KIP-392) — Allows consumers to read from the nearest in-sync replica using `client.rack` and `RackAwareReplicaSelector`.

**In-Sync Replicas (ISR)** — Replicas that are consistently asking for the latest messages. Controlled by `replica.lag.time.max.ms`.

### Request Processing

Kafka uses a binary protocol over TCP. Each broker runs:
- **Acceptor thread** — Creates connections
- **Processor threads** (network threads) — Handle client connections
- **I/O threads** (request handler threads) — Process requests from the request queue

**Key request types:**
- **Produce requests** — Write messages to partition leaders
- **Fetch requests** — Read messages (from consumers or follower replicas)
- **Metadata requests** — Discover cluster topology
- **Admin requests** — Administrative operations

**Produce request flow**: Validate → Write to local disk → Check acks config → Respond immediately or wait for replication

**Fetch request flow**: Validate offset → Read from partition (zero-copy) → Return data to client

### Physical Storage

**Tiered Storage** (planned for 3.0) — Two tiers:
- **Local tier** — Local disks on Kafka brokers (fast, short retention)
- **Remote tier** — HDFS or S3 (slower, longer retention)

**Partition Allocation** — Partitions are distributed across `log.dirs` directories using a "least-used" approach

**File Management** — Log segments are files managed with retention policies. Active segment is written to; closed segments are eligible for expiration.

**File Format** — Each log segment contains messages with: offset, message size, checksum, magic byte, attributes, timestamp, key, and value.

**Indexes** — Two indexes per segment: offset-to-position index and timestamp-to-offset index.

**Compaction** — Retains only the last message with a specific key. Useful for changelog-type data. Configurable via `cleanup.policy=compact`.

---

## 7. Reliable Data Delivery

### Reliability Guarantees

Kafka's reliability guarantees are based on:
- **Replication** — Data is copied across multiple brokers
- **Acknowledgment configuration** — `acks` setting controls durability
- **Retry logic** — Automatic retries for transient errors

### Replication

- **Replication Factor** — Number of copies of each partition. Higher = better durability but more storage.
- **Unclean Leader Election** — Leader election when no in-sync replica is available (risk of data loss)
- **Minimum In-Sync Replicas** (`min.insync.replicas`) — Minimum ISR count for successful writes

### Broker Configuration for Reliability

- `replication.factor` — Should be at least 3
- `min.insync.replicas` — Set to replication factor minus 1
- `unclean.leader.election.enable` — Set to `false` to prevent data loss

### Producers in a Reliable System

- **Send Acknowledgments** — Use `acks=all`
- **Configure Producer Retries** — Use `delivery.timeout.ms` instead of `retries`
- **Additional Error Handling** — Handle non-retriable errors

### Consumers in a Reliable System

- **Important Consumer Configurations** — `enable.auto.commit=false` for explicit control
- **Explicitly Committing Offsets** — Use `commitSync()` after processing

### Validating System Reliability

- Validate broker configuration
- Validate application behavior
- Monitor reliability in production

---

## 8. Exactly-Once Semantics

### Idempotent Producer

Enabled via `enable.idempotence=true`. The producer attaches sequence numbers to each record. If the broker receives duplicate sequence numbers, it rejects duplicates.

**Limitations**: Only prevents duplicates within a single producer session; does not handle read-process-write cycles.

### Transactions

**Use Cases**: Exactly-once processing in read-process-write cycles (e.g., consume, process, produce results).

**How it works**: Uses a transactional coordinator and transaction logs. Producers write to a transaction log. Consumers only read committed data when `isolation.level=read_committed`.

**Key Configuration**:
- `transactional.id` — Unique ID for the producer
- `isolation.level` — `read_committed` for consumers to avoid uncommitted data

### Performance Considerations

Transactions add overhead due to:
- Additional round trips for transaction coordination
- Write to the transaction log topic
- Fencing of zombie producers

---

## 9. Building Data Pipelines

### Considerations

- **Timeliness** — Real-time vs. batch
- **Reliability** — Exactly-once, at-least-once
- **Throughput** — High and varying
- **Data Formats** — Schema management
- **Transformations** — ETL vs. ELT
- **Security** — Authentication, authorization, encryption
- **Failure Handling** — Dead letter queues, retries
- **Coupling and Agility** — Decoupling producers and consumers

### Kafka Connect

**Running Kafka Connect**: Distributed or standalone mode.

**Connector Examples**:
- File Source and File Sink
- MySQL to Elasticsearch (using JDBC Source + Elasticsearch Sink)

**Single Message Transformations (SMTs)**: Lightweight transformations applied within Kafka Connect.

### Alternatives to Kafka Connect

- Ingest frameworks for other datastores
- GUI-based ETL tools
- Stream processing frameworks

---

## 10. Cross-Cluster Data Mirroring

### Use Cases

- Regional data aggregation
- Disaster recovery
- Cloud migration

### Multicluster Architectures

- **Hub-and-Spoke** — Single central cluster receives data from multiple edge clusters
- **Active-Active** — Multiple clusters all accept reads/writes
- **Active-Standby** — One cluster is primary, another serves as backup
- **Stretch Clusters** — Single cluster spanning multiple datacenters

### MirrorMaker

- **Configuring MirrorMaker** — Consumer reads from source, producer writes to target
- **Multicluster Replication Topology** — Chain, star, or mesh
- **Securing MirrorMaker** — TLS/SSL, SASL authentication
- **Deploying in Production** — Considerations for scaling and monitoring
- **Tuning** — Consumer and producer configuration for replication throughput

### Other Solutions

- **Uber uReplicator** — Built on top of MirrorMaker
- **LinkedIn Brooklin** — General-purpose data replication
- **Confluent Cross-Datacenter Mirroring** — Enterprise replication features

---

## 11. Securing Kafka

### Locking Down Kafka

Multi-layered security approach:
- **Network level** — Firewalls, private networks
- **Authentication** — Verifying client identity
- **Authorization** — Controlling access to resources
- **Encryption** — Protecting data in transit and at rest

### Security Protocols

- **SSL/TLS** — Encryption and authentication via certificates
- **SASL** — Authentication mechanisms: PLAIN, SCRAM, GSSAPI (Kerberos), OAUTHBEARER

### Authentication

- **SSL** — Client and server certificates
- **SASL** — Various mechanisms for different environments
- **Reauthentication** — Periodic re-verification of credentials

### Encryption

- **In Transit** — TLS encryption between clients and brokers
- **End-to-End** — Application-level encryption (client encrypts before producing)

### Authorization

- **AclAuthorizer** — Default Kafka authorization mechanism
- **Customizing Authorization** — Implement custom `Authorizer`

### Security Considerations

- **Auditing** — Tracking access and operations
- **Securing ZooKeeper** — SASL and SSL for ZooKeeper
- **Securing the Platform** — Password protection, secure configuration

---

## 12. Administering Kafka

### Topic Operations

**Creating a New Topic**:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic \
    --replication-factor 2 --partitions 8
```

**Listing Topics**:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Describing Topics**:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic my-topic
```

**Useful filters**: `--under-replicated-partitions`, `--at-min-isr-partitions`, `--under-min-isr-partitions`, `--unavailable-partitions`

**Adding Partitions**:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic my-topic --partitions 16
```

**Deleting a Topic**:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic my-topic
```

### Consumer Groups

**Listing and Describing Groups**:
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group
```

**Deleting Groups**: Remove an empty consumer group to clean up committed offsets.

### Offset Management

**Resetting offsets**:
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group my-group --topic my-topic --reset-offsets --to-earliest --execute
```

### Dynamic Configuration Changes

- **Overriding Topic Configuration** — Using `kafka-configs.sh`
- **Overriding Client and User Configuration** — Per-client and per-user quotas
- **Overriding Broker Configuration** — Dynamic broker configuration

### Producing and Consuming

**Console Producer**:
```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic my-topic
```

**Console Consumer**:
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --from-beginning
```

### Partition Management

- **Preferred Replica Election** — Rebalance leadership
- **Changing Partition Replicas** — Moving replicas between brokers
- **Dumping Log Segments** — Inspecting log segment contents using `kafka-dump-log.sh`
- **Replica Verification** — Checking replica consistency

### Other Tools

- **Unsafe Operations** — Moving the cluster controller, manually deleting topics

---

## 13. Monitoring Kafka

### Metric Basics

Metrics are exposed via JMX MBeans. Key metric categories include rates (per second measurements) and percentiles.

### Kafka Broker Metrics

**Key Metrics**:
- **Bytes in/out per second** — Overall cluster throughput
- **Messages in per second** — Individual message count
- **Partition count** — Number of partitions per broker
- **Leader count** — Number of partitions the broker leads
- **Offline partitions** — Partitions without a leader (critical!)
- **Under-replicated partitions** — Partitions with replicas not in sync
- **Request metrics** — Per-request-type timing (total, queue, local, remote, throttle, response queue, response send time)

**Request metrics provided for**: Produce, Fetch, Metadata, OffsetCommit, JoinGroup, and many more (60+ request types)

### JVM Monitoring

- GC pause times and frequency
- Heap memory usage
- Thread states

### OS Monitoring

- Disk I/O and utilization
- Network throughput and errors
- CPU utilization
- Memory usage (page cache efficiency)

### Client Monitoring

- **Producer Metrics** — Send rate, error rate, compression ratio, buffer availability
- **Consumer Metrics** — Fetch rate, poll rate, commit rate, lag
- **Quotas** — Throttle time metrics

### Lag Monitoring

Consumer lag is the difference between the latest offset in a partition and the consumer's committed offset. Monitoring lag is critical for ensuring consumers are keeping up.

### End-to-End Monitoring

Produce messages with known timestamps and verify consumption within acceptable time windows.

---

## 14. Stream Processing

### What Is Stream Processing?

Stream processing continuously processes data in real time, as opposed to batch processing (Hadoop-style periodic processing of bulk data).

### Key Concepts

- **Topology** — The directed graph of processing nodes
- **Time** — Event time vs. processing time
- **State** — Local state stores for aggregation
- **Stream-Table Duality** — Streams can be viewed as tables and vice versa
- **Time Windows** — Tumbling, hopping, sliding, and session windows
- **Processing Guarantees** — Exactly-once, at-least-once

### Design Patterns

- **Single-Event Processing** — Stateless per-record operations
- **Processing with Local State** — Stateful operations (e.g., aggregations)
- **Multiphase Processing/Repartitioning** — GroupBy operations
- **Stream-Table Join** — Enriching streams with external data
- **Table-Table Join** — Joining two changelog streams
- **Streaming Join** — Joining two data streams
- **Out-of-Sequence Events** — Handling late-arriving data
- **Reprocessing** — Replaying from earlier offsets
- **Interactive Queries** — Querying state stores directly

### Kafka Streams by Example

- **Word Count** — Classic word count topology
- **Stock Market Statistics** — Time-windowed aggregations
- **ClickStream Enrichment** — Stream-table joins

### Architecture Overview

- **Building a Topology** — Using DSL (Processor API)
- **Optimizing a Topology** — Kafka Streams optimizer for combining operations
- **Testing a Topology** — Using `TopologyTestDriver`
- **Scaling a Topology** — Adding more application instances
- **Surviving Failures** — Exactly-once semantics and state store replication

### Stream Processing Frameworks Comparison

- **Kafka Streams** — Embedded library, lightweight, no cluster required
- **Apache Samza** — Similar model, YARN-based
- **Apache Flink** — True streaming with event-time semantics
- **Apache Spark Streaming** — Micro-batch processing

---

## Appendix A: Installing Kafka on Other Operating Systems

### Windows

- Download the Kafka binary distribution
- Ensure Java is installed and `JAVA_HOME` is set
- Start ZooKeeper using `zookeeper-server-start.bat`
- Start Kafka using `kafka-server-start.bat`

### macOS

- Use Homebrew: `brew install kafka`
- Or download and extract the binary distribution
- Start ZooKeeper: `zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties`
- Start Kafka: `kafka-server-start /usr/local/etc/kafka/server.properties`

---

## Appendix B: Additional Kafka Tools

### Ecosystem Tools

- **Cruise Control** (LinkedIn) — Automated cluster rebalancing
- **Kafka Monitor** (LinkedIn) — End-to-end monitoring framework
- **Burrow** (LinkedIn) — Consumer lag monitoring
- **ksqlDB** (Confluent) — Streaming SQL engine for Kafka
- **Schema Registry** (Confluent) — Schema management for Avro, Protobuf, JSON Schema
- **REST Proxy** (Confluent) — HTTP-based Kafka access
- **Kafka Connect** — Built-in data integration framework
- **Kafka Streams** — Built-in stream processing library

### Monitoring and Management

- **JMX Exporter** — Exporting JMX metrics to Prometheus
- **Grafana Dashboards** — Visualizing Kafka metrics
- **Confluent Control Center** — Enterprise monitoring and management

---

> *This document was compiled from the extracted text of Kafka: The Definitive Guide, 2nd Edition (O'Reilly, 2021).*

