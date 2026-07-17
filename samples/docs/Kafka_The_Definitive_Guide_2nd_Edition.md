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

This book covers everything developers and SREs need to use Kafka to its full potential, from basic APIs and configuration to the latest capabilities—including what *not* to do and antipatterns to avoid.

### Who Should Read This Book

- **Software engineers** developing applications that use Kafka's APIs
- **Production engineers (SREs, DevOps, sysadmins)** installing, configuring, tuning, and monitoring Kafka
- **Data architects and data engineers** designing an organization's entire data infrastructure
- **Managers and architects** who work with Kafka teams and need to understand Kafka's guarantees and trade-offs

Chapters 3, 4, and 14 are geared toward Java developers. Chapters 2, 10, 12, and 13 assume Linux experience. The rest of the book discusses Kafka and software architectures in general terms.

---

## 1. Meet Kafka

### Publish/Subscribe Messaging

Publish/subscribe (pub/sub) messaging is characterized by the sender (publisher) not specifically directing a message to a receiver. Instead, the publisher classifies the message, and the receiver (subscriber) subscribes to receive certain classes of messages. Pub/sub systems often have a **broker** as a central point.

- **How It Starts** — Most pub/sub systems start as a simple message queue or direct connection (e.g., an application pushing metrics to a dashboard). As more publishers and subscribers are added, direct connections become tangled technical debt, and a single centralized application is built to receive and route all messages.
- **Individual Queue Systems** — Organizations often end up with multiple separate pub/sub systems (e.g., one for metrics, one for logs, one for user tracking), each with its own quirks and maintenance burden.
- **Enter Kafka** — Kafka was built to solve this: a single, centralized pub/sub system designed for scale that can handle many types of data and many applications.

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

### The Data Ecosystem

Many applications participate in the environments we build for data processing: inputs (applications that create data), outputs (metrics, reports, and other data products), and loops (components that read, transform, and reintroduce data).

Apache Kafka provides the **circulatory system** for the data ecosystem. It carries messages between the various members of the infrastructure, providing a consistent interface for all clients. When coupled with a system to provide message schemas, producers and consumers no longer require tight coupling or direct connections of any sort. Components can be added and removed as business cases are created and dissolved.

### Use Cases

- **Activity Tracking** — the original use case at LinkedIn; user actions (page views, clicks, profile changes) are published to topics and consumed by backend applications for reports, machine learning, and search updates
- **Messaging** — applications sending notifications (e.g., emails) without concern for formatting or delivery; a single application reads and handles all messages consistently
- **Metrics and Logging** — collecting application and system metrics and logs; destination systems can change without altering frontend applications
- **Commit Log** — database changes published as a changelog stream for live updates, replication, or consolidation; log-compacted topics can retain a single change per key
- **Stream Processing** — real-time operations on data as quickly as messages are produced (covered in Chapter 14)

### Kafka's Origin

Created at LinkedIn to address the data pipeline problem—a high-performance messaging system that can handle many types of data and provide clean, structured data about user activity and system metrics in real time.

- **LinkedIn's Problem** — LinkedIn had a polling-based metrics system and a separate XML/HTTP-based user-activity tracking system; both were fragile, inconsistent, and incompatible. Off-the-shelf solutions (e.g., ActiveMQ) could not handle the scale, so a custom infrastructure was built.
- **The Birth of Kafka** — The team was led by Jay Kreps, with Neha Narkhede and, later, Jun Rao. Goals: decouple producers and consumers with a push-pull model, persist message data to allow multiple consumers, optimize for high throughput, and allow horizontal scaling. The result was a pub/sub messaging system with a storage layer like a log-aggregation system, using Apache Avro for serialization. LinkedIn's usage grew past seven trillion messages per day (as of February 2020).
- **Open Source** — Released as open source on GitHub in late 2010, accepted as an Apache incubator project in July 2011, and graduated in October 2012. A healthy ecosystem grew around it, including LinkedIn's Cruise Control, Kafka Monitor, and Burrow, and Confluent's ksqlDB, schema registry, and REST proxy (see Appendix B).
- **Commercial Engagement** — In the fall of 2014, Kreps, Narkhede, and Rao left LinkedIn to found Confluent, providing development, enterprise support, training, and cloud services for Kafka, and organizing the Kafka Summit conference series (started 2016).
- **The Name** — Jay Kreps: since Kafka was a system optimized for writing, a writer's name made sense; he liked Franz Kafka, and the name sounded cool for an open source project.

### Getting Started with Kafka

The next chapter covers installing and configuring Kafka, selecting the right hardware, and things to keep in mind when moving to production operations.

---

## 2. Installing Kafka

### Environment Setup

- **Choosing an Operating System** — Linux is recommended; Kafka is a Java application that runs on any system with a JRE, but it is optimized for Linux (see Appendix A for Windows and macOS)
- **Installing Java** — OpenJDK-based Java; the version of Kafka covered works with Java 8 and Java 11
- **Installing ZooKeeper** — Used by Kafka for storing broker metadata (tested with ZooKeeper 3.5.x)

**Standalone ZooKeeper server**:
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

**General Broker Parameters**:

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

- **How Many Brokers?** — Determined by disk capacity, replica capacity per broker, CPU capacity, and network capacity.
- **Broker Configuration** — All brokers must have the same `zookeeper.connect` and a unique `broker.id`.

### OS Tuning

- **Virtual Memory** — Set `vm.swappiness=1`, tune `vm.dirty_background_ratio` and `vm.dirty_ratio`
- **Disk** — Use XFS filesystem with `noatime` mount option
- **Networking** — Increase socket buffer sizes, enable TCP window scaling

### Production Concerns

- **Garbage Collector Options** — Use G1GC with `MaxGCPauseMillis=20` and `InitiatingHeapOccupancyPercent=35`
- **Datacenter Layout** — Use rack-aware replica placement (`broker.rack` configuration)
- **Colocating Applications on ZooKeeper** — Share a ZooKeeper ensemble across multiple Kafka clusters using chroot paths

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

### Sending a Message to Kafka

Three primary methods:
- **Fire-and-forget** — Send a message with no concern about whether it arrives successfully
- **Sending a Message Synchronously** — Using `Future.get()` to wait for the response
- **Sending a Message Asynchronously** — Using a `Callback` function invoked on success or failure

### Configuring Producers

Key configuration parameters:
- **client.id** — Logical identifier for the client
- **acks** — Controls durability: `0` (no wait), `1` (leader only), `all` (all in-sync replicas)
- **Message Delivery Time** — Governed by `max.block.ms` (how long `send()` blocks when the buffer is full), `delivery.timeout.ms` (total time until send succeeds or fails), `request.timeout.ms` (time to wait for each produce request response), and `retries`/`retry.backoff.ms`
- **linger.ms** — Time to wait for additional messages before sending batch
- **buffer.memory** — Memory for buffering messages
- **compression.type** — `snappy`, `gzip`, `lz4`, or `zstd`
- **batch.size** — Maximum bytes per batch
- **max.in.flight.requests.per.connection** — Concurrent in-flight requests
- **max.request.size** — Maximum request size
- **receive.buffer.bytes and send.buffer.bytes** — TCP socket buffer sizes
- **enable.idempotence** — Exactly-once semantics (requires `acks=all`, `retries>0`, `max.in.flight.requests<=5`)

### Serializers

- **Custom Serializers** — Possible but fragile; not recommended
- **Serializing Using Apache Avro** — Recommended for its schema evolution, compact format, and compatibility
- **Using Avro Records with Kafka** — Register schemas with a Schema Registry so producers and consumers stay compatible

Using Avro with Schema Registry:
```java
props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("schema.registry.url", schemaUrl);
```

### Partitions

- **Default partitioner** — Hashes the key to determine partition (consistent mapping)
- **Null keys** — Round-robin distribution (sticky partitioning since Kafka 2.4)
- **Custom partitioner** — Implement the `Partitioner` interface for custom logic

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

### Kafka Consumer Concepts

**Consumers and Consumer Groups** — Consumers are typically part of a **consumer group**. Each consumer in a group receives messages from a different subset of partitions. To scale consumption, add more consumers (up to the number of partitions in the topic).

**Key rule**: If you need an application to get all messages, give it its own consumer group.

**Consumer Groups and Partition Rebalance** — Two types of rebalances:

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

**Thread Safety** — Only one thread per consumer may call `poll()`. To interrupt a poll loop from another thread (e.g., for shutdown), use `consumer.wakeup()`.

### Configuring Consumers

- **fetch.min.bytes** — Minimum data to return per fetch (default: 1 byte)
- **fetch.max.wait.ms** — Maximum time to wait for min bytes (default: 500 ms)
- **fetch.max.bytes** — Maximum bytes returned per poll (default: 50 MB)
- **max.poll.records** — Maximum records returned per poll
- **max.partition.fetch.bytes** — Maximum bytes per partition (default: 1 MB)
- **session.timeout.ms and heartbeat.interval.ms** — Consumer heartbeat timeout (default: 10 seconds) and heartbeat frequency (typically 1/3 of session timeout)
- **max.poll.interval.ms** — Maximum time between polls before considered dead (default: 5 minutes)
- **default.api.timeout.ms** — Timeout for nearly all consumer API calls (default: 1 minute)
- **request.timeout.ms** — Time the consumer waits for a response to each request
- **auto.offset.reset** — Behavior when no committed offset: `latest`, `earliest`, or `none`
- **enable.auto.commit** — Whether to auto-commit offsets (default: true)
- **partition.assignment.strategy** — `Range`, `RoundRobin`, `Sticky`, or `CooperativeSticky`
- **client.id** — Logical identifier for the client
- **client.rack** — Rack identifier, used to fetch from the closest replica
- **group.instance.id** — Unique instance ID enabling static group membership
- **receive.buffer.bytes and send.buffer.bytes** — TCP socket buffer sizes
- **offsets.retention.minutes** — How long committed offsets are retained after the group becomes empty

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

**Combining Synchronous and Asynchronous Commits** — Use `commitAsync()` during normal operation and `commitSync()` before shutdown

**Committing a Specified Offset** — Pass a map of partitions and offsets to `commitSync()` or `commitAsync()`

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

**But How Do We Exit?** — To cleanly exit a poll loop, another thread calls `consumer.wakeup()`, causing `poll()` to throw a `WakeupException`; catch it, then call `consumer.close()`.

### Deserializers

- **Custom Deserializers** — Possible but not recommended
- **Using Avro Deserialization with Kafka Consumer** — Use `KafkaAvroDeserializer` with Schema Registry

### Standalone Consumer: Why and How to Use a Consumer Without a Group

When a single consumer should read specific partitions (no group coordination or rebalances needed), assign partitions directly:

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

Key design principles:

- **Asynchronous and Eventually Consistent API** — Each method returns Future objects; metadata propagation is eventually consistent
- **Options** — Each method accepts an Options object (e.g., `ListTopicsOptions`)
- **Flat Hierarchy** — All operations are implemented directly in `AdminClient`
- **Additional Notes** — AdminClient is intended for application developers, complementing (not replacing) the command-line tools used by operators

### AdminClient Lifecycle: Creating, Configuring, and Closing

```java
Properties props = new Properties();
props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
AdminClient admin = AdminClient.create(props);
// ... use admin ...
admin.close(Duration.ofSeconds(30));
```

Notable configurations:
- **client.dns.lookup** — Controls how broker hostnames are resolved (e.g., `use_all_dns_ips`)
- **request.timeout.ms** — How long the client waits for a response before retrying or failing

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

**Exploring Consumer Groups**:
```java
admin.listConsumerGroups().valid().get().forEach(System.out::println);

ConsumerGroupDescription groupDescription = admin.describeConsumerGroups(CONSUMER_GRP_LIST)
    .describedGroups().get(CONSUMER_GROUP).get();
```

**Modifying Consumer Groups** — e.g., resetting offsets:
```java
admin.alterConsumerGroupOffsets(CONSUMER_GROUP, resetOffsets).all().get();
```

### Cluster Metadata

```java
DescribeClusterResult cluster = admin.describeCluster();
System.out.println("Connected to cluster " + cluster.clusterId().get());
```

### Advanced Admin Operations

- **Adding Partitions to a Topic** — `admin.createPartitions()`
- **Deleting Records from a Topic** — `admin.deleteRecords()`
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

**Produce Requests** — Validate → write to local disk → check `acks` config → respond immediately or wait for replication.

**Fetch Requests** — Validate offset → read from partition (zero-copy) → return data to client. Clients can specify a maximum wait time and minimum bytes so brokers batch responses efficiently.

**Other Requests** — Metadata requests (discover cluster topology) and admin requests (administrative operations).

### Physical Storage

**Tiered Storage** (planned for 3.0) — Two tiers:
- **Local tier** — Local disks on Kafka brokers (fast, short retention)
- **Remote tier** — HDFS or S3 (slower, longer retention)

**Partition Allocation** — Partitions are distributed across `log.dirs` directories using a "least-used" approach.

**File Management** — Log segments are files managed with retention policies. The active segment is written to; closed segments are eligible for expiration.

**File Format** — Each log segment contains messages with: offset, message size, checksum, magic byte, attributes, timestamp, key, and value.

**Indexes** — Two indexes per segment: offset-to-position index and timestamp-to-offset index.

**Compaction** — Retains only the last message with a specific key. Useful for changelog-type data. Configurable via `cleanup.policy=compact`.
- **How Compaction Works** — A cleaner thread rewrites closed segments, keeping only the most recent value per key
- **Deleted Events** — A null value (tombstone) marks a key for eventual removal
- **When Are Topics Compacted?** — Only inactive segments are compacted, governed by `min.cleanable.dirty.ratio`

---

## 7. Reliable Data Delivery

### Reliability Guarantees

Kafka's reliability guarantees are based on:
- **Replication** — Data is copied across multiple brokers
- **Acknowledgment configuration** — `acks` setting controls durability
- **Retry logic** — Automatic retries for transient errors

Kafka offers **at-least-once** delivery by default; exactly-once requires additional mechanisms (Chapter 8).

### Replication

Replication keeps multiple copies of each partition on different brokers, so data survives broker failure. All produce requests go to the leader replica; followers replicate and stay in sync.

### Broker Configuration

- **Replication Factor** — Number of copies of each partition. Higher = better durability but more storage. Should be at least 3.
- **Unclean Leader Election** — Leader election when no in-sync replica is available (risk of data loss). Set `unclean.leader.election.enable=false` to prevent data loss.
- **Minimum In-Sync Replicas** (`min.insync.replicas`) — Minimum ISR count for successful writes; set to replication factor minus 1.
- **Keeping Replicas In Sync** — Controlled by `replica.lag.time.max.ms` and `num.replica.fetchers`.
- **Persisting to Disk** — Kafka relies on the OS page cache; tune `flush` behavior carefully and rely on replication, not fsync, for durability.

### Using Producers in a Reliable System

- **Send Acknowledgments** — Use `acks=all`
- **Configuring Producer Retries** — Use `delivery.timeout.ms` instead of `retries`
- **Additional Error Handling** — Handle non-retriable errors

### Using Consumers in a Reliable System

- **Important Consumer Configuration Properties for Reliable Processing** — `enable.auto.commit=false` for explicit control
- **Explicitly Committing Offsets in Consumers** — Use `commitSync()` after processing

### Validating System Reliability

- **Validating Configuration** — Verify broker and topic configuration matches durability requirements
- **Validating Applications** — Test producer/consumer behavior under failure scenarios
- **Monitoring Reliability in Production** — Track under-replicated partitions, unclean elections, and related metrics

---

## 8. Exactly-Once Semantics

### Idempotent Producer

**How Does the Idempotent Producer Work?** — Enabled via `enable.idempotence=true`. The producer attaches a producer ID (PID) and sequence numbers to each record batch. If the broker receives duplicate sequence numbers, it rejects the duplicates, so retries no longer introduce duplicates.

**Limitations of the Idempotent Producer** — Only prevents duplicates within a single producer session (a producer restart gets a new PID); does not handle read-process-write cycles or deduplication across producers.

**How Do I Use the Kafka Idempotent Producer?** — Set `enable.idempotence=true`; this implies `acks=all` and safe retry/in-flight limits. Upgrading existing applications is transparent.

### Transactions

**Transactions Use Cases** — Exactly-once processing in read-process-write cycles (e.g., consume from one topic, process, produce results to another).

**What Problems Do Transactions Solve?** — Atomic writes across multiple partitions: either all messages of a transaction are visible to consumers, or none are. Also fences zombie producers.

**How Do Transactions Guarantee Exactly-Once?** — A transactional coordinator manages a transaction log topic. Records are written to partitions but marked uncommitted; consumers with `isolation.level=read_committed` only see committed data. Offsets consumed in the cycle are written to the offsets topic within the same transaction.

**What Problems Aren't Solved by Transactions?** — Side effects outside Kafka (e.g., sending email), external systems without transactional integration, and exactly-once across non-Kafka stores.

**How Do I Use Transactions?** — Configure `transactional.id` on the producer, call `initTransactions()`, then `beginTransaction()`, `sendOffsetsToTransaction()`, and `commitTransaction()`/`abortTransaction()` around the consume-transform-produce loop.

**Transactional IDs and Fencing** — A unique, stable `transactional.id` per producer instance lets the coordinator fence off zombie producers with the same ID via an epoch.

**How Transactions Work** — The coordinator writes prepare/commit markers to the transaction log and to data partitions (control batches); brokers return data according to the consumer's isolation level.

**Performance of Transactions** — Transactions add overhead: additional round trips for coordination, writes to the transaction log topic, and fencing of zombie producers. Keep transactions reasonably short and batch where possible.

---

## 9. Building Data Pipelines

### Considerations When Building Data Pipelines

- **Timeliness** — Real-time vs. batch
- **Reliability** — Exactly-once, at-least-once
- **High and Varying Throughput** — Systems must handle peaks
- **Data Formats** — Schema management
- **Transformations** — ETL vs. ELT
- **Security** — Authentication, authorization, encryption
- **Failure Handling** — Dead letter queues, retries
- **Coupling and Agility** — Decoupling producers and consumers

### When to Use Kafka Connect Versus Producer and Consumer

- Use **Kafka clients** (producer/consumer) when you can modify the application code and want to push data into or pull data from Kafka.
- Use **Kafka Connect** to connect Kafka to datastores you did not write and cannot modify. With an existing connector, you only write configuration files.
- If no connector exists, writing one with the Connect API is recommended over a bespoke app: Connect provides configuration management, offset storage, parallelization, error handling, support for different data types, and standard management REST APIs out of the box.

### Kafka Connect

**Running Kafka Connect** — Distributed mode (`connect-distributed.sh`) for production; standalone mode (`connect-standalone.sh`) for single-machine cases. Key worker configs: `bootstrap.servers`, `group.id`, `plugin.path`, `key.converter`/`value.converter`, and REST host/port. Connectors are configured and monitored through the REST API.

**Connector Example: File Source and File Sink** — The file connectors and JSON converter ship with Apache Kafka; a connector is created by POSTing JSON configuration to the Connect REST API.

**Connector Example: MySQL to Elasticsearch** — JDBC source connector reads from MySQL; Elasticsearch sink connector writes to Elasticsearch, demonstrating end-to-end pipeline configuration.

**Single Message Transformations (SMTs)** — Lightweight transformations applied within Kafka Connect (e.g., masking fields, routing, format changes) without stream processing code.

**A Deeper Look at Kafka Connect** — Three basic concepts:
- **Connectors and tasks** — Connectors decide how many tasks run and split the work; tasks actually move data in and out of Kafka
- **Workers** — The container processes executing connectors and tasks; they handle the REST API, configuration storage, reliability, high availability, scaling, and load balancing
- **Converters and Connect's data model** — Source connectors produce Schema/Value pairs; converters (JSON, Avro, Protobuf, etc.) serialize them to Kafka, and vice versa for sinks. Workers also manage offsets for connectors (pluggable storage, usually Kafka topics).

### Alternatives to Kafka Connect

- **Ingest Frameworks for Other Datastores** — Flume (Hadoop), Logstash/Fluentd (Elasticsearch) make sense when those systems, not Kafka, are the center of the architecture
- **GUI-Based ETL Tools** — Informatica, Talend, Pentaho, Apache NiFi, StreamSets; useful if already in use, but heavy for simple Kafka in/out
- **Stream Processing Frameworks** — Can double as data integration when the destination is supported, at the cost of harder troubleshooting

---

## 10. Cross-Cluster Data Mirroring

### Use Cases of Cross-Cluster Mirroring

- Regional and central clusters
- High availability (HA) and disaster recovery (DR)
- Regulatory compliance
- Cloud migrations
- Aggregation of data from edge clusters

### Multicluster Architectures

- **Some Realities of Cross-Datacenter Communication** — Higher latencies and limited bandwidth make cross-datacenter setups fundamentally different from single-datacenter clusters
- **Hub-and-Spoke Architecture** — Single central cluster receives data from multiple edge clusters
- **Active-Active Architecture** — Multiple clusters all accept reads/writes
- **Active-Standby Architecture** — One cluster is primary, another serves as backup
- **Stretch Clusters** — Single cluster spanning multiple datacenters

### Apache Kafka's MirrorMaker

- **Configuring MirrorMaker** — Consumer reads from source, producer writes to target
- **Multicluster Replication Topology** — Chain, star, or mesh
- **Securing MirrorMaker** — TLS/SSL, SASL authentication
- **Deploying MirrorMaker in Production** — Considerations for scaling and monitoring
- **Tuning MirrorMaker** — Consumer and producer configuration for replication throughput

### Other Cross-Cluster Mirroring Solutions

- **Uber uReplicator** — Uber's MirrorMaker clone using Apache Helix as a central controller to manage topics and partition assignment, avoiding consumer rebalances
- **LinkedIn Brooklin** — General-purpose data replication
- **Confluent Cross-Datacenter Mirroring Solutions** — Enterprise replication features

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
- **Security Updates Without Downtime** — Rolling upgrades to change security protocols or rotate credentials without cluster outages

### Encryption

- **In Transit** — TLS encryption between clients and brokers
- **End-to-End Encryption** — Application-level encryption (client encrypts before producing), so data is never plaintext inside Kafka

### Authorization

- **AclAuthorizer** — Default Kafka authorization mechanism
- **Customizing Authorization** — Implement a custom `Authorizer`

### Security Considerations

- **Auditing** — Tracking access and operations
- **Securing ZooKeeper** — SASL, SSL, and authorization for ZooKeeper
- **Securing the Platform** — Password protection, secure configuration

---

## 12. Administering Kafka

### Topic Operations

**Creating a New Topic**:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic \
    --replication-factor 2 --partitions 8
```

**Listing All Topics in a Cluster**:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Describing Topic Details**:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic my-topic
```

**Useful filters**: `--under-replicated-partitions`, `--at-min-isr-partitions`, `--under-min-isr-partitions`, `--unavailable-partitions`

**Adding Partitions**:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic my-topic --partitions 16
```

**Reducing Partitions** — Not supported; delete and recreate the topic instead.

**Deleting a Topic**:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic my-topic
```

### Consumer Groups

**List and Describe Groups**:
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group
```

**Delete Group** — Remove an empty consumer group to clean up committed offsets.

**Offset Management** — Resetting offsets:
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group my-group --topic my-topic --reset-offsets --to-earliest --execute
```

### Dynamic Configuration Changes

- **Overriding Topic Configuration Defaults** — Using `kafka-configs.sh`
- **Overriding Client and User Configuration Defaults** — Per-client and per-user quotas
- **Overriding Broker Configuration Defaults** — Dynamic broker configuration
- **Describing Configuration Overrides** — List current overrides
- **Removing Configuration Overrides** — Revert to defaults

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
- **Changing a Partition's Replicas** — Moving replicas between brokers
- **Dumping Log Segments** — Inspecting log segment contents using `kafka-dump-log.sh`
- **Replica Verification** — Checking replica consistency

### Other Tools

Additional tooling for cluster operations beyond the core command-line utilities.

### Unsafe Operations

- **Moving the Cluster Controller** — Manually shutting down the controller to force re-election
- **Removing Topics to Be Deleted** — Clearing stuck topic deletions
- **Deleting Topics Manually** — Removing topic data directly from ZooKeeper when normal deletion fails

---

## 13. Monitoring Kafka

### Metric Basics

- **Where Are the Metrics?** — Metrics are exposed via JMX MBeans
- **What Metrics Do I Need?** — Key categories include rates (per-second measurements) and percentiles
- **Application Health Checks** — Simple external checks that the broker responds

### Service-Level Objectives

- **Service-Level Definitions** — An **SLI** (indicator) is a metric describing one aspect of reliability, best expressed as a ratio of good events to total events; an **SLO** (objective) combines an SLI with a target value and time frame (e.g., 99.9% over 7 days); an **SLA** (agreement) is a contract with a client including SLOs and penalties
- **What Metrics Make Good SLIs?** — Gathered externally to the brokers; client-side metrics are usually best. Common SLI types: availability, latency, quality, security, throughput
- **Using SLOs in Alerting** — SLOs should inform primary alerts; alert on the SLO **burn rate** rather than the SLO directly to catch problems before breaching the objective

### Kafka Broker Metrics

- **Diagnosing Cluster Problems** — Three major categories: single-broker problems, overloaded clusters, and controller problems. Run a preferred replica election first; use external tools like Cruise Control to keep the cluster balanced.
- **The Art of Under-Replicated Partitions** — Under-replicated partitions are the single most important health indicator; investigate causes from broker-level issues to cluster-wide overload.
- **Broker Metrics** — Bytes in/out per second, messages in per second, partition count, leader count, offline partitions (critical!), under-replicated partitions, and per-request-type timing (total, queue, local, remote, throttle, response queue, response send time) for 60+ request types (Produce, Fetch, Metadata, OffsetCommit, JoinGroup, etc.)
- **Topic and Partition Metrics** — Per-topic and per-partition rates and sizes
- **JVM Monitoring** — GC pause times and frequency, heap memory usage, thread states
- **OS Monitoring** — Disk I/O and utilization, network throughput and errors, CPU utilization, memory usage (page cache efficiency)
- **Logging** — Broker and controller logs provide essential context for diagnosis

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

Stream processing continuously processes data in real time, as opposed to batch processing (Hadoop-style periodic processing of bulk data) and request-response transactions.

### Stream Processing Concepts

- **Topology** — The directed graph of processing nodes
- **Time** — Event time vs. processing time
- **State** — Local state stores for aggregation
- **Stream-Table Duality** — Streams can be viewed as tables and vice versa
- **Time Windows** — Hopping, tumbling, and session windows, plus grace periods and alignment
- **Processing Guarantees** — Exactly-once, at-least-once

### Stream Processing Design Patterns

- **Single-Event Processing** — Stateless per-record operations
- **Processing with Local State** — Stateful operations (e.g., aggregations)
- **Multiphase Processing/Repartitioning** — GroupBy operations
- **Processing with External Lookup: Stream-Table Join** — Enriching streams with external data
- **Table-Table Join** — Joining two changelog streams
- **Streaming Join** — Joining two data streams
- **Out-of-Sequence Events** — Handling late-arriving data
- **Reprocessing** — Replaying from earlier offsets
- **Interactive Queries** — Querying state stores directly

### Kafka Streams by Example

- **Word Count** — Classic word count topology
- **Stock Market Statistics** — Time-windowed aggregations
- **ClickStream Enrichment** — Stream-table joins

### Kafka Streams: Architecture Overview

- **Building a Topology** — Using the DSL (or the lower-level Processor API)
- **Optimizing a Topology** — Kafka Streams optimizer for combining operations
- **Testing a Topology** — Using `TopologyTestDriver`
- **Scaling a Topology** — Adding more application instances
- **Surviving Failures** — Exactly-once semantics and state store replication

### Stream Processing Use Cases

- **Customer Service** — Every system gets updates seconds after an event (e.g., hotel reservations visible immediately to the service desk, website, and confirmation emails)
- **Internet of Things** — Processing device events at scale to predict preventive maintenance (manufacturing, telecom, cable TV)
- **Fraud Detection** — Anomaly detection in near real time (credit card fraud, trading fraud, game cheating, cybersecurity beaconing)

### How to Choose a Stream Processing Framework

Match the framework to the application type:
- **Ingest** — Consider Kafka Connect instead; otherwise ensure good connector support
- **Low milliseconds actions** — Request-response may be better; otherwise choose event-by-event low-latency over microbatch
- **Asynchronous microservices** — Need good message-bus integration, change capture, and local store support
- **Near real-time data analytics** — Need advanced aggregations, windows, and join types

Global considerations: operability, usability of APIs and ease of debugging, making hard things easy, and community.

Frameworks compared in the chapter include **Kafka Streams** (embedded library, lightweight, no cluster required), **Apache Samza** (similar model, YARN-based), **Apache Flink** (true streaming with event-time semantics), and **Apache Spark Streaming** (micro-batch processing).

---

## Appendix A: Installing Kafka on Other Operating Systems

Kafka is primarily a Java application and runs anywhere a JRE is available, but it is optimized for Linux. For development or test on a desktop OS, consider a virtual machine matching your production environment.

### Installing on Windows

Two options on Windows 10: the Windows Subsystem for Linux (preferred) or a native Java installation.

**Using Windows Subsystem for Linux (WSL)** — Install WSL with Ubuntu, then install a JDK and follow the Linux instructions from Chapter 2:
```bash
sudo apt install openjdk-16-jre-headless
```

**Using Native Java** — Install the latest Oracle Java JDK (avoid paths with spaces, e.g., `C:\Java\jdk-16.0.1`), set `JAVA_HOME` and add `%JAVA_HOME%\bin` to `Path`, then extract Kafka (e.g., to `C:\kafka_2.13-2.8.0`). Use the Windows batch files, each in its own shell:
```powershell
bin\windows\zookeeper-server-start.bat C:\kafka_2.13-2.8.0\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat C:\kafka_2.13-2.8.0\config\server.properties
```

### Installing on macOS

**Using Homebrew** — One-step install (also installs Java and ZooKeeper):
```bash
brew install kafka
/usr/local/bin/zkServer start
/usr/local/bin/kafka-server-start /usr/local/etc/kafka/server.properties
```
Homebrew links binaries into `/usr/local/bin`, configurations into `/usr/local/etc/kafka` and `/usr/local/etc/zookeeper`, and sets `log.dirs` to `/usr/local/var/lib/kafka-logs`.

**Installing Manually** — Install a JDK, extract Kafka (e.g., to `/usr/local/kafka_2.13-2.8.0`), set `JAVA_HOME`, and start the services as on Linux:
```bash
export JAVA_HOME=`/usr/libexec/java_home -v 16.0.1`
/usr/local/kafka_2.13-2.8.0/bin/zookeeper-server-start.sh -daemon /usr/local/kafka_2.13-2.8.0/config/zookeeper.properties
/usr/local/kafka_2.13-2.8.0/bin/kafka-server-start.sh /usr/local/kafka_2.13-2.8.0/config/server.properties
```

---

## Appendix B: Additional Kafka Tools

The Apache Kafka community has created a robust ecosystem of tools and platforms. This is not an exhaustive list—do your own research on suitability.

### Comprehensive Platforms

Fully integrated, managed platforms for working with Kafka:

- **Confluent Cloud** (confluent.io/confluent-cloud) — Managed Kafka from the company founded by Kafka's original developers; includes schema management, clients, a RESTful interface, and monitoring across AWS, Azure, and GCP
- **Aiven** (aiven.io) — Managed solutions for many data platforms including Kafka; provides Karapace, an Apache 2.0-licensed schema registry and REST proxy; also supports DigitalOcean and UpCloud
- **CloudKarafka** (cloudkarafka.com) — Managed Kafka with integrations for infrastructure services (DataDog, Splunk) on AWS and GCP
- **Amazon MSK** (aws.amazon.com/msk) — AWS's managed Kafka; schema support via AWS Glue; promotes community tools (Cruise Control, Burrow) without directly supporting them
- **Azure HDInsight** (azure.microsoft.com/services/hdinsight) — Microsoft's managed platform supporting Kafka alongside Hadoop and Spark; focuses on the core cluster
- **Cloudera** (cloudera.com) — Managed Kafka as the stream data component of its Customer Data Platform (CDP), in public cloud and private options

### Cluster Deployment and Management

- **Strimzi** (strimzi.io) — Kubernetes operators for deploying Kafka clusters; includes the Strimzi Kafka Bridge REST proxy (Apache 2.0)
- **AKHQ** (akhq.io) — GUI for managing and interacting with clusters: configuration, users, ACLs, Schema Registry, Connect, and data browsing
- **JulieOps** (github.com/kafka-ops/julie) — Automated, GitOps-style management of topics, schemas, and ACLs (formerly Kafka Topology Builder)
- **Cruise Control** (github.com/linkedin/cruise-control) — LinkedIn's tool for automated rebalancing, anomaly detection, and administrative operations at scale; a must-have beyond testing clusters
- **Conduktor** (conduktor.io) — Popular desktop tool (not open source) for managing clusters and working with data; free license for development use

### Monitoring and Data Exploration

- **Xinfra Monitor** (github.com/linkedin/kafka-monitor) — LinkedIn's availability monitor (formerly Kafka Monitor); generates synthetic traffic and measures latency, availability, and completeness
- **Burrow** (github.com/linkedin/burrow) — Holistic consumer-lag monitoring without directly interacting with consumers
- **Kafka Dashboard** (datadoghq.com/dashboards/kafka-dashboard) — DataDog's single-pane Kafka dashboard
- **Streams Explorer** (github.com/bakdata/streams-explorer) — Visualizes data flow through Kafka Streams/Faust applications and connectors in Kubernetes
- **kcat** (github.com/edenhill/kafkacat) — Fast, JVM-free (C) alternative to the console producer/consumer (formerly kafkacat), with cluster metadata views

### Client Libraries

- **librdkafka** (github.com/edenhill/librdkafka) — High-performance C client library; basis for Confluent's Go, Python, and .NET clients (two-clause BSD license)
- **Sarama** (github.com/Shopify/sarama) — Shopify's native Golang client (MIT license)
- **kafka-python** (github.com/dpkp/kafka-python) — Native Python client (Apache 2.0 license)

### Stream Processing

- **Samza** (samza.apache.org) — Stream processing framework designed for Kafka; runs on YARN as a full application framework
- **Spark** (spark.apache.org) — Batch-oriented processing with fast microbatch streaming; wide community support
- **Flink** (flink.apache.org) — True low-latency stream processing; supports YARN, Mesos, Kubernetes, or standalone; Python and R APIs
- **Beam** (beam.apache.org) — Unified batch/stream programming model using Samza, Spark, or Flink as runners

---

> *This document was compiled from the extracted text of Kafka: The Definitive Guide, 2nd Edition (O'Reilly, 2021).*
