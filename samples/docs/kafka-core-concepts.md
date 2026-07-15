# Kafka Core Concepts

> Based on **Kafka: The Definitive Guide, 2nd Edition** by Gwen Shapira, Todd Palino, Rajini Sivaram & Krit Petty (O'Reilly, 2021)

---

## 1. What is Apache Kafka?

Apache Kafka is a **distributed streaming platform** that lets you publish and subscribe to streams of data, store them durably, and process them in real time. It acts as a **centralized publish/subscribe messaging system** designed to handle the continuous flow of data across an organization.

### Key Characteristics

- **Distributed commit log**: Data is stored durably, in order, and can be read deterministically
- **High throughput**: Handles millions of messages per second with sub-second latency
- **Scalable**: Horizontally scalable from single broker to hundreds of brokers
- **Durable**: Messages are persisted to disk with configurable retention
- **Fault-tolerant**: Replication provides availability and durability when nodes fail

---

## 2. Core Concepts

### 2.1 Messages

The **unit of data** within Kafka is called a **message** (comparable to a row or record in a database). A message is simply an array of bytes with no specific format or meaning to Kafka itself.

A message can have optional **metadata**:
- **Key**: Also a byte array, used for partition assignment (messages with the same key go to the same partition)
- **Timestamp**: Indicates when the event was produced
- **Headers**: Optional metadata key-value pairs

### 2.2 Batches

For efficiency, messages are written into Kafka in **batches** — collections of messages being produced to the same topic and partition. This reduces network overhead at the cost of a latency trade-off:
- Larger batches → higher throughput, higher latency per message
- Smaller batches → lower latency, more network round trips

Batches are typically **compressed** for efficient data transfer and storage.

### 2.3 Schemas

While messages are opaque byte arrays, imposing a **schema** on message content is recommended. Common options:

- **Apache Avro**: Compact serialization, schemas separate from payloads, strong typing, schema evolution with backward/forward compatibility — the preferred choice for Kafka
- **JSON/XML**: Simple and human-readable but lack robust type handling and schema evolution

### 2.4 Topics and Partitions

**Topics** are the primary category for messages (analogous to a database table or filesystem folder).

**Partitions** break topics into multiple logs:
- A partition is a single, ordered, append-only commit log
- Messages within a partition are read in order from beginning to end
- **No ordering guarantee across partitions** — only within a single partition
- Partitions provide **redundancy** and **scalability**:
  - Each partition can be hosted on a different server (horizontal scaling)
  - Partitions can be replicated across servers for fault tolerance

The term **stream** typically refers to a single topic of data, regardless of partition count.

### 2.5 Producers

**Producers** create new messages and write them to specific topics. Key behaviors:

- By default, producers **balance messages evenly** across all partitions of a topic
- Messages can be directed to specific partitions using:
  - **Message key**: A hash of the key determines the partition (ensures same-key messages go to the same partition)
  - **Custom partitioner**: Implements custom business rules for partition assignment

**Send methods**:
1. **Fire-and-forget**: Send and don't check success
2. **Synchronous send**: Use `Future.get()` to wait for acknowledgment
3. **Asynchronous send**: Use a callback to handle success/failure

**Important producer configurations**:
- `acks` (0, 1, all): Controls durability guarantees
- `batch.size`: Maximum bytes per batch
- `linger.ms`: Time to wait for additional messages before sending a batch
- `compression.type`: snappy, gzip, lz4, or zstd
- `max.in.flight.requests.per.connection`: Controls ordering guarantees
- `enable.idempotence`: Enables exactly-once semantics for producers
- `delivery.timeout.ms`: Maximum time to wait for message delivery

### 2.6 Consumers

**Consumers** read messages from topics. Key concepts:

- Consumers track their progress using **offsets** — an integer that continually increases, unique per message within a partition
- Multiple consumers work together as a **consumer group**
- Each partition is consumed by **only one consumer** within a group at a time
- Consumers can **horizontally scale** — adding more consumers increases throughput
- The **poll loop** is central to consumer operation; consumers must poll continuously to remain alive

**Consumer Groups and Partition Rebalance**:
- A **rebalance** moves partition ownership between consumers when members join/leave
- **Eager rebalance**: All consumers stop, give up partitions, then get reassigned
- **Cooperative rebalance** (incremental): Only a subset of partitions are reassigned, others continue processing
- Static group membership (`group.instance.id`) allows consumers to keep their assignments across restarts

**Important consumer configurations**:
- `group.id`: Consumer group identifier
- `auto.offset.reset` (earliest/latest): Behavior when no committed offset exists
- `enable.auto.commit`: Automatic vs. manual offset commits
- `session.timeout.ms` / `heartbeat.interval.ms`: Consumer liveness detection

### 2.7 Brokers and Clusters

**Broker**: A single Kafka server that:
- Receives messages from producers
- Assigns offsets to messages
- Writes messages to disk storage
- Serves fetch requests from consumers

**Cluster**: A group of brokers working together:
- **Controller**: One broker elected automatically to handle administrative operations (partition assignment, broker failure monitoring)
- **Leader**: The broker that owns a partition and handles all produce/fetch requests
- **Followers**: Replicas that replicate messages from the leader to provide redundancy

### 2.8 Retention

Kafka provides **durable storage** of messages for configurable periods:

- **Time-based retention** (default: 7 days): Messages expire after a set time
- **Size-based retention**: Messages expire when partition reaches a size limit
- **Log compaction**: Retains only the last message with a specific key (useful for changelog data)
- Retention operates on **log segments**, not individual messages

---

## 3. Kafka Internals

### 3.1 Cluster Membership

- Kafka uses **Apache ZooKeeper** to maintain the list of live brokers
- Each broker registers itself by creating an **ephemeral node** in ZooKeeper
- When a broker loses connectivity to ZooKeeper, its ephemeral node is removed
- Components watching the `/brokers/ids` path get notified of membership changes

### 3.2 The Controller

- One broker serves as the **controller**, responsible for **electing partition leaders**
- The first broker to start creates the `/controller` ephemeral node in ZooKeeper
- Controller uses an **epoch number** (monotonically increasing) to prevent **split-brain** scenarios (zombie fencing)
- When a broker leaves the cluster, the controller determines new leaders for affected partitions
- The new controller state is persisted to ZooKeeper and communicated to all brokers

### 3.3 KRaft (Kafka's New Raft-Based Controller)

- Starting in Apache Kafka 2.8, KRaft replaces the ZooKeeper-based controller
- Controller nodes form a **Raft quorum** managing a log of metadata events
- Eliminates ZooKeeper dependency — Kafka runs as a single executable
- Addresses scalability bottlenecks and metadata consistency issues

### 3.4 Replication

Replication is the heart of Kafka's availability and durability guarantees:

- **Leader replica**: Handles all produce requests (guarantees consistency); consumers can read from leader or followers
- **Follower replicas**: Replicate messages from the leader; promoted to leader if the leader crashes
- **In-Sync Replicas (ISR)**: Replicas that are fully caught up with the leader
- Only ISR replicas are eligible to become leaders
- `replica.lag.time.max.ms`: Controls how long a follower can be behind before being removed from ISR
- **Preferred leader**: The original leader when topic was created; used for load balancing

### 3.5 Request Processing

Kafka uses a **binary protocol over TCP**:

- **Acceptor thread**: Accepts connections and hands them to processor threads
- **Processor (network) threads**: Place requests in a request queue, pick up responses from response queue
- **I/O (request handler) threads**: Process requests from the queue
- Main request types: **Produce requests** (from producers), **Fetch requests** (from consumers/followers), **Metadata requests** (for cluster topology)

### 3.6 Physical Storage

- Messages are written to **log segments** on disk
- When a log segment reaches a size limit (default 1 GB), it's closed and a new one opens
- Closed segments can be considered for **expiration**
- Kafka uses **indexes** for efficient offset lookups
- **Tiered Storage** (newer feature): Allows moving older data to cheaper storage tiers
- **Compaction**: Retains only the latest value per key for log-compacted topics

---

## 4. Reliability and Data Delivery

### 4.1 Reliability Guarantees

Kafka provides **at-least-once** delivery by default and supports **exactly-once** semantics:

- **At-least-once**: Messages are guaranteed to be delivered but may have duplicates
- **Exactly-once**: Messages are delivered once and only once (via idempotent producers + transactions)

### 4.2 Broker Configuration for Reliability

- **Replication factor** (default: 1, recommended: 3): Number of copies of each partition
- **unclean.leader.election.enable** (default: false): Whether to allow out-of-sync replicas to become leaders (risks data loss)
- **min.insync.replicas** (recommended: 2): Minimum number of ISR replicas for a successful write

### 4.3 Producer Reliability

- **acks=all**: Waits for all in-sync replicas to acknowledge
- **retries** / **delivery.timeout.ms**: Controls retry behavior
- **enable.idempotence=true**: Prevents duplicates from retries

### 4.4 Consumer Reliability

- Commit offsets **only after messages are fully processed**
- Manual offset commits for complex processing patterns
- Handle **rebalances** properly (commit offsets before partition revocation)
- Monitor **consumer lag** — how far the consumer is behind the latest message

---

## 5. Exactly-Once Semantics

### 5.1 Idempotent Producer

- Each message includes a unique **Producer ID (PID)** and **sequence number**
- Brokers detect and reject duplicate messages
- Requires `max.inflight.requests <= 5`, `retries > 0`, `acks=all`

### 5.2 Transactions

- Enable **atomic writes** across multiple partitions and topics
- Combine offset commits with result writes in a single transaction
- **Transactional ID** and **fencing** prevent zombie producers from creating inconsistencies
- Use cases: Exactly-once stream processing, atomic writes to Kafka and external systems

---

## 6. Kafka Connect (Data Pipelines)

Kafka Connect is a framework for **streaming data between Kafka and other systems**:

- **Source connectors**: Import data from external systems into Kafka
- **Sink connectors**: Export data from Kafka to external systems
- **Single Message Transformations (SMTs)**: Lightweight transformations within Connect
- Runs in **standalone** or **distributed** mode

---

## 7. Cross-Cluster Data Mirroring

### 7.1 Architectures

- **Hub-and-Spoke**: Multiple local clusters feed into a central aggregate cluster
- **Active-Active**: Data is replicated bidirectionally between clusters
- **Active-Standby**: One cluster is primary, another is for disaster recovery

### 7.2 MirrorMaker 2.0

- Built on Kafka Connect framework
- Supports topic configuration and ACL migration
- Automatic detection of new topics
- **Offset translation** for seamless failover
- Monitored via replication latency metrics and heartbeats

---

## 8. Kafka Security

### 8.1 Authentication

- **SSL/TLS**: Certificate-based authentication with mutual authentication option
- **SASL/GSSAPI**: Kerberos integration
- **SASL/PLAIN**: Username/password (must use with TLS)
- **SASL/SCRAM**: Salted Challenge Response (SHA-256/SHA-512)
- **SASL/OAUTHBEARER**: OAuth 2.0 bearer token integration
- **Delegation tokens**: Lightweight shared secrets for authentication

### 8.2 Authorization

- **ACLs** (Access Control Lists) control which users can perform which operations
- Custom authorizers can integrate with external authorization systems

### 8.3 Encryption

- **SSL/TLS encryption**: Protects data in transit between clients and brokers, and between brokers
- **End-to-end encryption**: Application-level encryption for sensitive data

### 8.4 Auditing

- Access logs capture authentication successes and failures
- Brokers track client connections and request activity

---

## 9. Stream Processing (Kafka Streams)

### 9.1 What is Stream Processing?

Stream processing operates on data in **real time** as messages are produced, as opposed to batch processing (like Hadoop) that operates on bulk data at a later time.

### 9.2 Core Concepts

- **Topology**: The directed graph of stream processors that defines the computation
- **Time**: Event time, processing time, ingestion time
- **State**: Local state stores for stateful operations (joins, aggregations, windowing)
- **Stream-Table Duality**: Streams and tables are dual — a stream can be viewed as a table (latest value per key), and a table can be viewed as a stream (changelog)
- **Time Windows**: Tumbling, hopping, sliding, and session windows for time-based aggregations

### 9.3 Processing Guarantees

- Exactly-once processing semantics when combined with Kafka transactions
- Automatic fault tolerance via state store replication

### 9.4 Design Patterns

- **Single-event processing**: Process each event independently
- **Processing with local state**: Maintain state for aggregations and joins
- **Multiphase processing**: Repartition data for efficient processing
- **Stream-table joins**: Enrich streams with reference data
- **Interactive queries**: Query state stores directly from applications

---

## 10. Monitoring Kafka

### 10.1 Key Metrics

- **Broker metrics**: Under-replicated partitions, request rates, error rates
- **Producer metrics**: Error rate, retry rate, batch sizes
- **Consumer metrics**: Consumer lag, fetch rates, commit rates
- **JVM monitoring**: Garbage collection, heap usage
- **OS monitoring**: Disk I/O, network throughput, CPU usage

### 10.2 Consumer Lag

The most critical consumer metric — indicates how far a consumer is behind the latest message. Burrow (LinkedIn's lag checker) provides sophisticated lag monitoring.

### 10.3 End-to-End Monitoring

Track message production and consumption rates to ensure no data loss. Compare events per second from producers and consumers, and measure the time between produce and consume timestamps.

---

## 11. AdminClient API

Kafka's `AdminClient` provides programmatic access to cluster management:

- **Topic management**: Create, describe, list, delete topics; add partitions
- **Configuration management**: Describe and update broker, topic, client, and user configurations
- **Consumer group management**: List, describe, and modify consumer groups and offsets
- **Cluster management**: Elect leaders, reassign replicas, delete records
- **Testing**: `MockAdminClient` for unit testing without a real cluster

---

## 12. Use Cases

| Use Case | Description |
|----------|-------------|
| **Activity Tracking** | The original Kafka use case — tracking user actions on websites/applications |
| **Messaging** | Decoupled notification delivery (e.g., email) with consistent formatting |
| **Metrics & Logging** | Collecting application metrics and logs for monitoring and analysis |
| **Commit Log** | Database change data capture (CDC) and changelog streams |
| **Stream Processing** | Real-time analytics, aggregations, joins, and transformations |

---

*This document summarizes key concepts from "Kafka: The Definitive Guide, 2nd Edition" (O'Reilly, 2021) by Gwen Shapira, Todd Palino, Rajini Sivaram, and Krit Petty.*

