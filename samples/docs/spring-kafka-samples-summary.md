# Spring for Apache Kafka — Samples Summary

This document provides an overview of the Kafka concepts demonstrated across all sample projects (`sample-01` through `sample-08`). Each sample focuses on one or more specific Spring Kafka features, ranging from basic messaging patterns to advanced topics like transactions, stream processing, tracing, and the next-generation consumer rebalance protocol.

---

## Table of Contents

- [Sample 01 — JSON Serialization, Error Handling & Dead Letter Topic](#sample-01--json-serialization-error-handling--dead-letter-topic)
- [Sample 02 — Multi-Method `@KafkaListener` with Type-Based Dispatching](#sample-02--multi-method-kafkalistener-with-type-based-dispatching)
- [Sample 03 — Kafka Transactions & Batch Listeners](#sample-03--kafka-transactions--batch-listeners)
- [Sample 04 — `@RetryableTopic` Non-Blocking Retry & `@DltHandler`](#sample-04--retryabletopic-non-blocking-retry--dlthandler)
- [Sample 05 — Global Embedded Kafka for Testing](#sample-05--global-embedded-kafka-for-testing)
- [Sample 06 — Kafka Streams with Stateful Aggregation & Suppression](#sample-06--kafka-streams-with-stateful-aggregation--suppression)
- [Sample 07 — KIP-848 Next Generation Consumer Rebalance Protocol](#sample-07--kip-848-next-generation-consumer-rebalance-protocol)
- [Sample 08 — Micrometer Observation Propagation Across Producer & Consumer](#sample-08--micrometer-observation-propagation-across-producer--consumer)

---

## Sample 01 — JSON Serialization, Error Handling & Dead Letter Topic

| Concept | Details |
|---|---|
| **JSON Serialization/Deserialization** | Producer uses `JsonSerializer` to send POJOs (`Foo1`) as JSON; consumer uses `ByteArrayDeserializer` with a `JsonMessageConverter` that deserializes to `Foo2` based on the target method parameter type. |
| **Dead Letter Topic (DLT)** | A `DefaultErrorHandler` with `DeadLetterPublishingRecoverer` sends failed messages to `topic1-dlt` after retries are exhausted. A separate `@KafkaListener` consumes from the DLT for inspection. |
| **Error Handler Retry with `FixedBackOff`** | `FixedBackOff(1000L, 2)` configures a 1-second delay with 2 retry attempts before the message is sent to the DLT. |
| **Programmatic Topic Creation (`NewTopic`)** | `NewTopic` beans declare `topic1` and `topic1-dlt` with 1 partition and 1 replica; Spring Kafka's `KafkaAdmin` creates them on the broker. |
| **REST-to-Kafka Bridge** | A `@RestController` receives HTTP POST requests and sends messages to `topic1` via `KafkaTemplate`. |
| **Static Consumer Group IDs** | `@KafkaListener(id = "fooGroup", topics = "topic1")` and a DLT listener with `id = "dltGroup"`. |

**Key Files:**
- `src/main/java/com/example/Application.java` — `@KafkaListener`, `DefaultErrorHandler`, `DeadLetterPublishingRecoverer`, `NewTopic` beans, `JsonMessageConverter`
- `src/main/java/com/example/Controller.java` — REST producer
- `src/main/java/com/common/Foo1.java`, `Foo2.java` — Producer & consumer POJOs (different classes, same JSON structure)

---

## Sample 02 — Multi-Method `@KafkaListener` with Type-Based Dispatching

| Concept | Details |
|---|---|
| **Multi-Method `@KafkaListener`** | A class-level `@KafkaListener` subscribing to both `"foos"` and `"bars"` topics dispatches messages to individual `@KafkaHandler`-annotated methods based on payload type, plus a default fallback handler. |
| **Polymorphic JSON Type Mapping** | Producer embeds type identifiers (`foo`, `bar`) via `spring.json.type.mapping`. Consumer uses `DefaultJackson2JavaTypeMapper` with `setIdClassMapping` to map type IDs to consumer-specific POJO classes (`Foo2`, `Bar2`). |
| **Decoupled Producer/Consumer Types** | Producer sends `Foo1`/`Bar1`; consumer receives `Foo2`/`Bar2` — independent class hierarchies connected by a shared type identifier. |
| **REST-to-Kafka Bridge** | Three HTTP endpoints demonstrate sending typed `Foo1`, `Bar1`, and plain `String` messages. The `String` case triggers the default handler. |
| **Error Handling with DLT** | `DefaultErrorHandler` with `DeadLetterPublishingRecoverer` and `FixedBackOff`. |
| **Topic Creation** | `NewTopic` beans for `"foos"` and `"bars"` topics. |

**Key Files:**
- `src/main/java/com/example/MultiMethods.java` — `@KafkaListener` with `@KafkaHandler` dispatch (including `isDefault = true`)
- `src/main/java/com/example/Application.java` — Error handler, record message converter with type mapping, `NewTopic` beans
- `src/main/java/com/example/Controller.java` — REST endpoints
- `src/main/resources/application.yml` — Producer-side type mapping config

---

## Sample 03 — Kafka Transactions & Batch Listeners

| Concept | Details |
|---|---|
| **Kafka Transactions** | Producer configures `transaction-id-prefix: tx.` enabling transactional `KafkaTemplate`. Consumer operates in container-managed transactional mode — sends and offset commits are enrolled in the same transaction, enabling exactly-once semantics. |
| **Exactly-Once Semantics & Isolation Level** | Consumer sets `isolation.level: read_committed` to read only committed transactional messages. |
| **Batch Listeners** | `listener.type: batch` configures the container to poll batches of records; the listener method receives `List<Foo2>`. A `BatchMessagingMessageConverter` handles deserialization. |
| **Programmatic Transaction via `executeInTransaction`** | The REST controller wraps multiple `KafkaTemplate.send()` calls inside `executeInTransaction()` for atomic producer-side transactions. |
| **Chained Data Flow (Multi-Stage Pipeline)** | 1) HTTP → `topic2` (transactional write), 2) batch listener reads `topic2`, transforms data, writes to `topic3` in same transaction, 3) second listener reads `topic3` and logs results. |
| **Manual Offset Control via `System.in.read()`** | The batch listener blocks on `System.in.read()` before committing, simulating a transaction boundary controlled by external input. |

**Key Files:**
- `src/main/resources/application.yml` — `transaction-id-prefix`, `isolation.level: read_committed`, batch listener config, JSON serialization
- `src/main/java/com/example/Application.java` — `@KafkaListener` with batch receive, transactional sends, `NewTopic` beans, `BatchMessagingMessageConverter`
- `src/main/java/com/example/Controller.java` — `KafkaTemplate.executeInTransaction()` for producer transactions

---

## Sample 04 — `@RetryableTopic` Non-Blocking Retry & `@DltHandler`

| Concept | Details |
|---|---|
| **`@RetryableTopic` Annotation** | Automatically creates a chain of retry topics with configurable backoff. Configured with `attempts = "5"` and exponential backoff (`@BackOff(delay = 2000, maxDelay = 10000, multiplier = 2)`), creating retry topics at 2s, 4s, 8s, and 10s delays. |
| **Non-Blocking Topic-Based Retry** | Unlike blocking retry (which holds the partition), failed messages are forwarded to retry topics, allowing the consumer to continue processing other messages. |
| **`@DltHandler` Annotation** | Designates a method to handle messages that have exhausted all retry attempts, consuming from the dead letter topic (`topic4-dlt`). |
| **Header Injection** | Listener extracts `@Header(KafkaHeaders.RECEIVED_TOPIC)` and `@Header(KafkaHeaders.OFFSET)` to observe retry flow across topics. |
| **Conditional Failure Logic** | Messages starting with `"fail"` throw a `RuntimeException`, triggering the retry mechanism; other messages process normally. |

**Key Files:**
- `src/main/java/com/example/Application.java` — `@RetryableTopic`, `@KafkaListener` with header injection, `@DltHandler`, failure logic
- `src/main/java/com/example/Controller.java` — REST producer via `KafkaTemplate`

---

## Sample 05 — Global Embedded Kafka for Testing

| Concept | Details |
|---|---|
| **Global Embedded Kafka Broker** | Uses `GlobalEmbeddedKafkaTestExecutionListener` (JUnit Platform extension) to start a single embedded Kafka broker once per test plan, shared across all test classes. |
| **Custom Broker Properties** | `kafka-broker.properties` sets `auto.create.topics.enable=false`, proving broker properties are loaded from a file. |
| **Pre-Defined Topics via `junit-platform.properties`** | `spring.kafka.embedded.topics=topic1,topic2` auto-creates topics on the embedded broker. |
| **Test 1 — Successful Send/Receive** | Sends a message to `topic1` via `KafkaTemplate`, uses Awaitility to asynchronously wait for the `@KafkaListener` to receive it, verifies via `OutputCaptureExtension`. |
| **Test 2 — Topic Non-Existence** | Sends to `nonExistingTopic` (with `auto.create.topics.enable=false`), verifies a `TimeoutException` with "Topic not present in metadata" is thrown. |
| **IDE vs Maven Execution** | Tests are designed to fail in an IDE because `spring.kafka.global.embedded.enabled` is only configured in the Maven Surefire plugin, not in test resources. |

**Key Files:**
- `pom.xml` — Maven Surefire plugin with `spring.kafka.global.embedded.enabled` and `kafka-broker.properties` location
- `src/test/resources/junit-platform.properties` — Pre-defined embedded topics
- `src/test/resources/kafka-broker.properties` — `auto.create.topics.enable=false`
- `src/test/java/com/example/Sample05Application1Tests.java` — Successful send/receive test
- `src/test/java/com/example/Sample05Application2Tests.java` — Topic non-existence error test

---

## Sample 06 — Kafka Streams with Stateful Aggregation & Suppression

| Concept | Details |
|---|---|
| **Kafka Streams API** | Defines a stream processing topology using `StreamsBuilder` with `@EnableKafkaStreams`. |
| **Stateful Aggregation (`groupByKey().count()`)** | Groups records by key and maintains a running count per key in an internal state store. |
| **Suppression (`suppress()`)** | Delays output emission with `Suppressed.untilTimeLimit(Duration.ofMillis(5))` to reduce update frequency, using `BufferConfig.unbounded()`. |
| **Table-to-Stream Conversion (`toStream()`)** | Converts the `KTable` result back to a `KStream` so it can be written to an output topic via `to()`. |
| **Custom Serdes** | Explicitly specifies `Serdes.Integer()` for keys and `Serdes.String()` for values on input; uses `Serdes.Long()` for output deserialization. |
| **`TopologyTestDriver`** | Unit tests the streaming topology in-memory without a real Kafka cluster, using `TestInputTopic` and `TestOutputTopic`. |
| **Externalized Topic Names** | Topic names are configured via `application.yml` and injected with `@Value`. |
| **Topology Visualization** | The test logs the complete DAG description of the topology for debugging. |

**Key Files:**
- `src/main/java/com/example/Topology.java` — Stream processing topology: `stream → groupByKey → count → suppress → toStream → to`
- `src/main/java/com/example/Application.java` — `@EnableKafkaStreams`
- `src/test/java/com/example/ApplicationTests.java` — `TopologyTestDriver`, `TestInputTopic`, `TestOutputTopic`, Awaitility assertions

---

## Sample 07 — KIP-848 Next Generation Consumer Rebalance Protocol

| Concept | Details |
|---|---|
| **KIP-848 Server-Side Rebalance** | The consumer uses `group.protocol: consumer` to opt into the new server-side rebalance protocol, where the broker owns partition assignment decisions instead of consumers coordinating among themselves. |
| **KRaft Mode (No ZooKeeper)** | The Kafka broker runs without ZooKeeper using KRaft mode (Kafka Raft metadata quorum). Both controller and broker roles run on the same node. |
| **Multiple Consumer Groups** | Two consumer groups (`sample07-1` and `sample07-2`) independently consume from the same `test-topic`, demonstrating independent offset tracking per group. |
| **Spring Boot Docker Compose Integration** | `spring-boot-docker-compose` auto-starts and stops the Kafka container defined in `compose.yaml`. |
| **Multiple Kafka Listeners (Ports)** | The broker exposes separate listeners for inter-broker, client, external, and controller traffic (ports 9092, 9093, 9094, 29092). |
| **Auto Topic Creation** | `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"` allows `test-topic` to be created automatically upon subscription. |

**Key Files:**
- `compose.yaml` — Kafka broker config: KRaft mode, KIP-848 protocols (`classic,consumer`), multiple listeners
- `src/main/resources/application.yaml` — `group.protocol: consumer`, `StringDeserializer`
- `src/main/java/com/example/sample07/Sample07KafkaListener.java` — Two `@KafkaListener` methods in different consumer groups

---

## Sample 08 — Micrometer Observation Propagation Across Producer & Consumer

| Concept | Details |
|---|---|
| **Micrometer Observation Propagation** | Tracing spans are propagated from a Kafka producer to a consumer, ensuring both sides share the same trace ID. |
| **Observation-Enabled `KafkaTemplate`** | `spring.kafka.template.observation-enabled=true` creates tracing spans for every `send()` call. |
| **Observation-Enabled `@KafkaListener`** | `spring.kafka.listener.observation-enabled=true` creates tracing spans for message consumption, linked to the producer's trace ID. |
| **Micrometer Tracing with Brave (Zipkin)** | `micrometer-tracing-bridge-brave` provides Brave (Zipkin-compatible) tracing; 100% sampling rate is configured. |
| **Trace/Span ID in Logs** | Logging pattern includes `%X{traceId:-},%X{spanId:-}` to correlate producer and consumer log entries. |
| **Test with `TestSpanHandler`** | A test verifies that exactly 2 spans (producer + consumer) are captured and share the same trace ID using `SpansAssert.haveSameTraceId()`. |
| **`@EmbeddedKafka` for Testing** | `@EmbeddedKafka(partitions = 1)` starts an in-memory Kafka broker for the test. |
| **`ProducerListener` Callback** | A custom `ProducerListener` logs produced records on successful send. |
| **`ApplicationRunner` for Bootstrap Data** | An `ApplicationRunner` sends a test message when the application starts, triggering the producer-to-consumer observation flow. |

**Key Files:**
- `src/main/resources/application.properties` — `observation-enabled=true` for template & listener, logging pattern with trace/span IDs, 100% sampling
- `src/main/java/com/example/sample08/Sample08Application.java` — `@KafkaListener`, `ProducerListener`, `ApplicationRunner`
- `src/test/java/com/example/sample08/Sample08ApplicationTests.java` — `@EmbeddedKafka`, `TestSpanHandler`, trace ID verification
- `build.gradle` — Dependencies on `micrometer-tracing-bridge-brave`, `spring-kafka-test`

---

## Quick Reference by Concept

| Concept | Sample(s) |
|---|---|
| Basic `@KafkaListener` & `KafkaTemplate` | 01, 02, 03, 04, 05, 07, 08 |
| JSON Serialization/Deserialization | 01, 02, 03 |
| Dead Letter Topic (DLT) | 01, 02 |
| Error Handling & Retry (`DefaultErrorHandler`) | 01, 02 |
| `@RetryableTopic` (Non-Blocking Retry) | 04 |
| `@DltHandler` | 04 |
| Multi-Method `@KafkaListener` / `@KafkaHandler` | 02 |
| Kafka Transactions | 03 |
| Batch Listeners | 03 |
| Kafka Streams | 06 |
| Stateful Aggregation & Suppression | 06 |
| `TopologyTestDriver` | 06 |
| Global Embedded Kafka | 05 |
| Embedded Kafka (`@EmbeddedKafka`) | 08 |
| KIP-848 Server-Side Rebalance | 07 |
| KRaft Mode (No ZooKeeper) | 07 |
| Micrometer Observation / Tracing | 08 |
| Spring Boot Docker Compose Integration | 07 |
| Programmatic Topic Creation (`NewTopic`) | 01, 02, 03 |
| REST-to-Kafka Bridge | 01, 02, 03, 04 |

