# How to Design and Configure a Kafka Cluster

> Sources: *Kafka: The Definitive Guide, 2nd Edition* (O'Reilly, 2021) — see [Kafka_The_Definitive_Guide_2nd_Edition.md](Kafka_The_Definitive_Guide_2nd_Edition.md) and [kafka-core-concepts.md](kafka-core-concepts.md) — plus current industry best practices (Apache Kafka documentation, AWS MSK best practices, Confluent recommendations).

---

## 1. Start with Requirements

Before touching any configuration, quantify four things:

| Requirement | Questions to Answer |
|-------------|--------------------|
| **Throughput** | Peak and average MB/s in and out? Messages/second and average message size? |
| **Retention** | How long must data be kept? (drives disk capacity) |
| **Availability** | How many broker/AZ failures must the cluster survive? RPO/RTO targets? |
| **Latency** | Produce/consume latency budget? (drives disk and network choices) |

**Disk capacity rule of thumb** (from the book): daily ingest × retention days, plus headroom.
Example: 1 TB/day × 7 days retention = **7 TB minimum** across the cluster — then multiply by the replication factor and add ~10% for overhead.

---

## 2. Architecture Decisions

### 2.1 KRaft vs. ZooKeeper

- **The book (Kafka 2.7/2.8 era)**: ZooKeeper stores broker metadata; run an ensemble of an **odd number of servers (3, 5, or 7)**. KRaft was a preview in 2.8, early-access in 3.0–3.2.
- **Today**: **KRaft is the only mode — ZooKeeper support was completely removed in Apache Kafka 4.0.** New clusters should always use KRaft. KRaft became production-ready in **3.3** (not 3.0 — earlier versions were early-access only).

**KRaft controller deployment:**
- Use **3 controllers** for production (tolerates 1 failure); 5 for very large clusters. Never an even number.
- **Combined mode** (`process.roles=broker,controller`) — acceptable for development and small clusters (3–5 nodes).
- **Dedicated controllers** (`process.roles=controller` on separate nodes) — recommended for production and large clusters, isolating metadata traffic from data traffic.

### 2.2 How Many Brokers?

Determined by (per the book):
1. **Disk capacity** — total retention requirement ÷ usable disk per broker
2. **Replica capacity per broker** — stay within partition-count limits (see §3.2)
3. **CPU capacity** — compression and request handling
4. **Network capacity** — replication multiplies ingress traffic (RF=3 roughly doubles write traffic)

Keep broker CPU utilization (user + system) **under 60%** so the cluster retains headroom for rolling upgrades, patching, and broker failures ([AWS MSK guidance](https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html)).

### 2.3 Single Cluster vs. Multiple Clusters

Use multiple clusters for: regional aggregation, HA/disaster recovery, regulatory compliance, cloud migration, and edge aggregation. Common topologies (book Ch10):

- **Hub-and-spoke** — edge clusters feed a central aggregate cluster
- **Active-active** — all clusters serve reads/writes (beware conflict handling)
- **Active-standby** — primary + DR backup
- **Stretch cluster** — one cluster spanning datacenters (only with excellent inter-DC networking)

Mirror data with **MirrorMaker 2** (built on Kafka Connect; supports offset translation, topic config and ACL migration, automatic new-topic detection).

### 2.4 Rack / Availability-Zone Awareness

- Spread brokers across **at least 3 AZs** (or racks).
- Set `broker.rack` (or the cloud equivalent) so Kafka places replicas of each partition in different failure domains.
- Ensure client connection strings include **at least one broker per AZ** for failover.

---

## 3. Hardware Selection

### 3.1 What Matters (book Ch2)

| Resource | Guidance |
|----------|----------|
| **Disk throughput** | The biggest latency factor. Prefer SSD/NVMe; faster writes = lower produce latency |
| **Disk capacity** | Driven by retention (see §1) |
| **Memory** | Kafka uses a modest heap (~5 GB); the rest becomes **OS page cache**, which serves most consumer reads — more RAM = better consumer performance |
| **Network** | 10 GbE minimum; replication and consumers multiply egress |
| **CPU** | Less critical, but matters at scale when compression is enabled |

### 3.2 Cloud Instances

- **Book (2021)**: Azure Standard D16s v3 (small) / D64s v4 (large) with managed disks; AWS m4 or r3 families.
- **AWS MSK today** — recommended partitions per broker (leader + follower replicas):

| Broker size | Recommended partitions/broker |
|-------------|-------------------------------|
| kafka.t3.small | 300 |
| kafka.m5.large / m5.xlarge, m7g.large / m7g.xlarge | 1,000 |
| kafka.m5.2xlarge, m7g.2xlarge | 2,000 |
| kafka.m5.4xlarge and larger, m7g.4xlarge and larger | 4,000 |

- **General ceiling**: ~4,000 partitions per broker and ~200,000 partitions per cluster (Apache Kafka/Confluent guidance). High partition counts also inflate metrics and offset-tracking overhead — delete unused consumer groups.

### 3.3 Disk Layout: JBOD vs. RAID

- **JBOD** (multiple `log.dirs` paths, one per disk) is the common recommendation: more capacity and throughput; Kafka's own replication provides durability, so RAID redundancy is largely wasted.
- **RAID 10** simplifies failure handling (a disk loss doesn't take a log dir offline) but costs capacity and some throughput.
- **Caveat**: Tiered Storage requires a single mount point and does not support JBOD (Confluent Platform note).

> **Tiered Storage** (Kafka 3.6+, Confluent Platform): offloads older log segments to cheap remote storage (S3, GCS, etc.), decoupling retention from local disk capacity. If you plan to use it, factor a single-mount layout into your disk design. See [KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage) for details.

---

## 4. OS and JVM Tuning

### 4.1 Operating System (book Ch2)

- **OS**: Linux (Kafka is optimized for it)
- **Virtual memory**: `vm.swappiness=1`; tune `vm.dirty_background_ratio` and `vm.dirty_ratio`
- **Filesystem**: XFS with the `noatime` mount option
- **Networking**: increase socket buffer sizes; enable TCP window scaling

### 4.2 JVM and Garbage Collection

- **Heap**: keep modest — **5–6 GB** is typical; Kafka's performance comes from page cache, not heap. Alarm if heap-used-after-GC exceeds **60%**.
- **GC**: G1GC with `MaxGCPauseMillis=20` and `InitiatingHeapOccupancyPercent=35` (book recommendation, still the standard starting point).
- **Java version**: the book covers Java 8/11; current Kafka releases run on Java 17+ (Kafka 3.7+ supports Java 21). Check your Kafka version's support matrix.

---

## 5. Broker Configuration

### 5.1 General Parameters

```properties
# Identity — unique integer per broker (KRaft uses node.id; broker.id is for legacy ZK mode)
node.id=1

# Client endpoint
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://broker1.example.com:9092

# Storage — one path per disk for JBOD
log.dirs=/data1/kafka-logs,/data2/kafka-logs

# Faster log recovery after unclean shutdown (set to # of CPU cores)
num.recovery.threads.per.data.dir=16

# Disable auto topic creation in production
auto.create.topics.enable=false

# Rack/AZ awareness
broker.rack=us-east-1a
```

### 5.2 Topic Defaults

```properties
num.partitions=8                    # default for new topics (book default: 1 — raise deliberately)
                                    # ⚠ Partition count can only be increased, never decreased (without recreating the topic)
default.replication.factor=3
log.retention.hours=168             # 1 week (book default)
log.retention.bytes=-1              # or cap per-partition size
log.segment.bytes=1073741824        # 1 GB (book default)
message.max.bytes=1000000           # ~1 MB (book default)
min.insync.replicas=2
```

### 5.3 Thread Tuning (larger brokers)

Scale request-handling threads with CPU cores (AWS MSK guidance):

| Instance size | `num.io.threads` | `num.network.threads` |
|---------------|------------------|-----------------------|
| 16 cores (m5.4xl / m7g.4xl) | 16 | 8 |
| 32 cores (m5.8xl / m7g.8xl) | 32 | 16 |
| 48 cores (m5.12xl / m7g.12xl) | 48 | 24 |
| 64 cores (m5.16xl / m7g.16xl) | 64 | 32 |

Increase `num.io.threads` **before** `num.network.threads` to avoid queue congestion.

### 5.4 KRaft-Specific Configuration

```properties
process.roles=broker                # or controller, or broker,controller (combined)
node.id=1
controller.quorum.voters=1@ctrl1:9093,2@ctrl2:9093,3@ctrl3:9093
# Note: controller.quorum.voters is deprecated since Kafka 3.4+; use controller.quorum.bootstrap.servers instead
controller.listener.names=CONTROLLER
listener.security.protocol.map=BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT
listeners=BROKER://0.0.0.0:9092
```

New clusters: generate a cluster UUID and format storage (`kafka-storage.sh`) before first start.

---

## 6. Reliability Configuration (book Ch7)

The durability baseline — apply at broker/topic level **and** client level:

| Setting | Value | Why |
|---------|-------|-----|
| `replication.factor` | **3** | Survive broker loss; RF=1 risks offline partitions, RF=2 risks data loss during rolling ops |
| `min.insync.replicas` | **RF − 1** (=2) | Writes succeed with one replica offline; minISR = RF would block writes during maintenance |
| `unclean.leader.election.enable` | **false** (default) | Prevents out-of-sync replicas becoming leader (data loss) |
| Producer `acks` | **all** | Wait for all in-sync replicas |
| Producer `enable.idempotence` | **true** | No duplicates from retries (requires `acks=all`, `retries>0`; since Kafka 3.0+ the client enforces `max.in.flight.requests.per.connection<=5` automatically) |
| Consumer `enable.auto.commit` | **false** | Commit offsets only after messages are fully processed |

Also: rely on **replication, not fsync**, for durability — Kafka flushes via the OS page cache; tune `flush.*` settings with care.

---

## 7. Security Baseline (book Ch11)

- **Encryption in transit**: TLS between clients and brokers, and between brokers (enable everywhere in production)
- **Authentication**: SSL client certificates or SASL (SCRAM-SHA-512 preferred over PLAIN; OAUTHBEARER for OAuth 2.0; GSSAPI for Kerberos)
- **Authorization**: `AclAuthorizer` with least-privilege ACLs
- **ZooKeeper (legacy clusters only)**: secure with SASL/SSL and ACLs
- **Auditing**: broker logs capture authentication success/failure

---

## 8. Monitoring and Operations (book Ch13 + industry)

### 8.1 Metrics to Watch

| Metric | Threshold / Action |
|--------|--------------------|
| **Under-replicated partitions** | > 0 = investigate immediately |
| **Active controller count** | Must be exactly 1 per cluster (in KRaft, `ActiveControllerCount` reports 1 only on the current leader; other controllers report 0) |
| **Offline partitions** | > 0 = data unavailable |
| Broker CPU (user + system) | Keep **< 60%**; scale up/out above that |
| Disk usage (`KafkaDataLogsDiskUsed` or equivalent) | Alarm at **85%** — expand storage, shorten retention, or delete unused topics |
| Heap memory after GC | Alarm above **60%** |
| Consumer lag | Track per group (Burrow or equivalent) |
| Request handler idle ratio | Low idle = thread saturation |

Define **SLIs/SLOs** (e.g., produce latency, availability) and alert on burn rate rather than raw metric spikes.

### 8.2 Operational Practices

- **Rebalancing**: use **Cruise Control** for continuous load distribution; reassign partitions in small batches (≤ ~10 partitions per `kafka-reassign-partitions` call as a conservative starting point — larger batches are feasible with monitoring) and avoid reassignment when CPU > 70% (replication adds load).
- **Retention management**: topic-level `retention.ms` / `retention.bytes` override cluster defaults — use them to free disk selectively.
- **Scaling**: prefer scaling **up** (bigger brokers) for CPU headroom, or **out** (more brokers + new partitions) for granular growth; verify new partitions land on new brokers with `kafka-topics.sh --describe`.
- **Rolling changes**: one broker at a time; with RF=3 and minISR=2 the cluster stays fully available.
- **Log recovery**: after an unclean shutdown, recovery uses one thread per log dir by default — set `num.recovery.threads.per.data.dir` to the core count to avoid hours-long restarts.
- **Prometheus scraping**: use a ≥ 60s scrape interval to avoid CPU overhead.

---

## 9. Client-Side Considerations

Cluster design fails if clients can't tolerate broker loss:

- `bootstrap.servers` should list **multiple brokers across AZs** (not a single broker or LB pointing at one).
- Producers: `acks=all`, idempotence enabled, `delivery.timeout.ms` tuned to the latency budget.
- Consumers: manual offset commits after processing; use static group membership (`group.instance.id`) to reduce rebalances during rolling restarts.
- Run performance tests to verify client configs meet objectives before production.

---

## 10. Reference Checklist

**Design**
- [ ] Throughput, retention, availability, latency requirements documented
- [ ] KRaft mode with 3 dedicated controllers (production)
- [ ] Brokers across 3 AZs with `broker.rack` set
- [ ] Partition plan: ≤ 4,000 replicas/broker, ≤ 200K/cluster

**Configuration**
- [ ] `default.replication.factor=3`, `min.insync.replicas=2`
- [ ] `unclean.leader.election.enable=false`
- [ ] `auto.create.topics.enable=false`
- [ ] Retention set per topic; `log.dirs` on dedicated disks (JBOD) or RAID 10
- [ ] `vm.swappiness=1`, XFS + `noatime`, G1GC with 5–6 GB heap
- [ ] TLS + SASL/SCRAM (or mTLS) + ACLs enabled

**Operations**
- [ ] Alerts: under-replicated partitions, offline partitions, CPU > 60%, disk > 85%, heap-after-GC > 60%
- [ ] Cruise Control (or equivalent) for rebalancing
- [ ] Consumer lag monitoring in place
- [ ] Rolling upgrade/runbook tested

---

*Book-derived facts reference "Kafka: The Definitive Guide, 2nd Edition" (Shapira, Palino, Sivaram, Petty). Industry guidance: Apache Kafka documentation (KRaft), AWS MSK Best Practices for Standard brokers, Confluent Platform deployment docs.*

---

## Appendix A: Review Fix Summary

The following corrections were applied after a technical review to improve accuracy and recency:

| # | Fix | Section |
|---|-----|--------|
| 1 | Changed `broker.id=1` → `node.id=1`; updated comment to clarify KRaft uses `node.id`, `broker.id` is for legacy ZK mode | §5.1 |
| 2 | Added missing `listener.security.protocol.map=BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT` so the KRaft config block is self-contained | §5.4 |
| 3 | Added deprecation note: `controller.quorum.voters` is deprecated since Kafka 3.4+; use `controller.quorum.bootstrap.servers` instead | §5.4 |
| 4 | Added warning that partition count can only be **increased**, never decreased (without recreating the topic) | §5.2 |
| 5 | Updated idempotence note: since Kafka 3.0+ the client enforces `max.in.flight.requests.per.connection<=5` automatically — no manual tuning needed | §6 |
| 6 | Corrected KRaft timeline: was a preview in 2.8, **early-access in 3.0–3.2**, production-ready in **3.3** (not 3.0) | §2.1 |
| 7 | Clarified `ActiveControllerCount` behavior in KRaft: reports 1 only on the current leader; other controllers report 0 | §8.1 |
| 8 | Added Tiered Storage callout block with KIP-405 link (offloads older segments to S3/GCS, requires single-mount layout) | §3.3 |
| 9 | Added Java 21 support note (Kafka 3.7+), AWS MSK guidance hyperlink, and nuance on partition reassignment batch sizes | §4.2, §2.2, §8.2 |

---

## Appendix A: Review Fix Summary

The following corrections were applied after a review pass to improve accuracy and recency:

| # | Fix | Section |
|---|-----|--------|
| 1 | Changed `broker.id=1` → `node.id=1`; updated comment to clarify KRaft uses `node.id`, `broker.id` is for legacy ZK mode | §5.1 |
| 2 | Added missing `listener.security.protocol.map=BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT` so the KRaft config block is self-contained | §5.4 |
| 3 | Added deprecation note: `controller.quorum.voters` is deprecated since Kafka 3.4+; use `controller.quorum.bootstrap.servers` instead | §5.4 |
| 4 | Added warning that partition count can only be **increased**, never decreased (without recreating the topic) | §5.2 |
| 5 | Updated idempotence note: since Kafka 3.0+ the client enforces `max.in.flight.requests.per.connection<=5` automatically — no manual tuning needed | §6 |
| 6 | Corrected KRaft timeline: was a preview in 2.8, **early-access in 3.0–3.2**, production-ready in **3.3** (not 3.0) | §2.1 |
| 7 | Clarified `ActiveControllerCount` behavior in KRaft: reports 1 only on the current leader; other controllers report 0 | §8.1 |
| 8 | Added Tiered Storage callout block with KIP-405 link (offloads older segments to S3/GCS, requires single-mount layout) | §3.3 |
| 9 | Added Java 21 support note (Kafka 3.7+), AWS MSK guidance hyperlink, and nuance on partition reassignment batch sizes | §4.2, §2.2, §8.2 |
