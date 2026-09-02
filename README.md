# Kafka Consumer — Learning Project

Hands-on Java examples building up Apache Kafka consumer concepts
incrementally: a minimal poll loop, consumer groups and rebalancing, manual
offset commits (sync and async), and custom JSON deserialization. Companion
project to [`producer-demo`](https://github.com/DuminduChamal/producer-demo)
— written while learning Kafka from scratch, with the console consumer
replaced step by step by real Java code.

## Prerequisites

- **Java 17+** — required by the Kafka 4.x broker. The `kafka-clients`
  library used here only needs Java 11+, but the broker needs 17+.
- **Maven**
- **A local Kafka 4.3.1 broker**, running in KRaft mode at `localhost:9092`.
  This repo only contains the consumer code — the broker runs separately.

## Topics used by these examples

Most examples read from `keyed-topic` (3 partitions, created for the
producer-side partitioning examples). `JsonConsumer` reads from a separate
`orders-topic`:

```bash
bin/kafka-topics.sh --create --topic keyed-topic \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

bin/kafka-topics.sh --create --topic orders-topic \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

`orders-topic` exists because `keyed-topic` ends up with mixed content
(plain strings from earlier producer examples, JSON from others) — a JSON
deserializer throws on a non-JSON record and kills the poll loop, so JSON
traffic gets its own topic. In general, **a topic should carry one
consistent message schema**; this is exactly the kind of problem a Schema
Registry exists to prevent in production.

## Running an example

Each class has its own `main`. Point the `exec-maven-plugin` at the one you
want by setting `mainClass` in `pom.xml`, then:

```bash
mvn compile exec:java
```

## Examples, in the order they were built

### 1. `SimpleConsumer`
The minimal case: subscribe to `keyed-topic` with `group.id =
learning-consumer-group`, and loop on `poll()` forever, printing each
record's partition, offset, key, and value.

- `group.id` is the concept that doesn't exist on the producer side — it's
  what makes this a *consumer group*. Kafka tracks committed offsets per
  group, and guarantees that within a group, each partition is owned by
  exactly one consumer at a time.
- `auto.offset.reset=earliest` means a brand-new group with no committed
  offsets starts from the beginning of the topic — the same effect as
  `--from-beginning` on the CLI console consumer.
- Uses `enable.auto.commit=true` (the default) — offsets are committed on a
  timer in the background, not tied to whether processing actually finished.
  See `ManualCommitConsumer` below for why that matters.
- Also wires up a `ConsumerRebalanceListener` (`onPartitionsRevoked` /
  `onPartitionsAssigned`) purely to make partition assignment visible when
  experimenting with multiple instances — see below.

**Consumer groups & rebalancing experiment:** run two instances of
`SimpleConsumer` at once (same `group.id`, hardcoded in the class). Starting
the second instance triggers a rebalance: watch both terminals log a
`Partitions revoked` / `Partitions assigned` sequence as Kafka splits
`keyed-topic`'s 3 partitions between the two consumers. Stop one and watch
the survivor reclaim everything. This example uses Kafka's default "classic"
rebalance protocol, where a rebalance revokes *all* of a consumer's
partitions before reassigning, even ones it ends up keeping. Kafka 4.0
introduced an incremental alternative (KIP-848), opted into via
`group.protocol=consumer`, which only moves the partitions that actually
need to move.

### 2. `ManualCommitConsumer`
Same shape as `SimpleConsumer`, but with `enable.auto.commit=false` and an
explicit `consumer.commitSync()` call after each batch is fully processed
(with an artificial `Thread.sleep(2000)` per record to make the processing
window easy to interrupt). Uses its own `group.id`
(`manual-commit-group`) so it has independent offset history from
`SimpleConsumer`.

Demonstrates **at-least-once delivery**: kill the process (Ctrl+C) partway
through a batch, before `commitSync()` runs, then restart it. The
uncommitted portion of that batch gets reprocessed — nothing is lost, but
duplicates are possible. This is the trade-off manual, post-processing
commits are meant to produce, versus auto-commit's time-based commits which
can advance the offset before processing has actually finished.

### 3. `AsyncCommitConsumer`
Replaces the blocking `commitSync()` with non-blocking `commitAsync()`
(with a callback logging success/failure), so the poll loop never stalls
waiting on a broker round-trip. Adds the standard graceful-shutdown idiom:

- A JVM shutdown hook calls `consumer.wakeup()` — the one `KafkaConsumer`
  method safe to call from a different thread — which interrupts a blocked
  `poll()` by throwing `WakeupException`.
- The `WakeupException` is caught to exit the loop cleanly.
- A `finally` block runs one last `commitSync()` before `close()`, since a
  graceful shutdown has no "next" async commit coming along to fix a missed
  one.

Compare its graceful `Ctrl+C` behavior (final `commitSync()` runs, no
reprocessing on restart) against `ManualCommitConsumer`'s abrupt-kill
behavior (no chance to run cleanup, so the in-flight batch gets
reprocessed) — same underlying at-least-once guarantee, different exposure
to it depending on how the process actually stops.

### 4. `JsonConsumer` (with `JsonDeserializer`, `OrderEvent`)
Reads `orders-topic` and deserializes values back into real `OrderEvent`
objects instead of raw strings — the consumer-side mirror of
`producer-demo`'s `JsonSerializer`.

`JsonDeserializer<T>` implements Kafka's `Deserializer<T>` interface. Since
Java generics are erased at runtime, it can't know `T` just from its type
parameter — instead, Kafka instantiates it via a no-arg constructor and
calls `configure(Map<String, ?> configs, boolean isKey)`, and this
implementation reads the target class name out of that config map
(`json.deserializer.value.class`, set on the `Properties` passed to
`KafkaConsumer`). That's the standard way to parameterize a
reflection-instantiated Kafka `Deserializer`.

`OrderEvent` is duplicated here rather than shared with `producer-demo` —
this project doesn't depend on that one. In a real system this gap is
usually closed with a shared schema (Avro/Protobuf) or a shared library
rather than copy-pasted POJOs; duplicating it here keeps the two projects
independent, which is fine at this scale.

## Verifying output

Any of these can be checked against what the CLI console consumer sees:

```bash
bin/kafka-console-consumer.sh --topic keyed-topic --from-beginning \
  --bootstrap-server localhost:9092 \
  --property print.key=true --property print.partition=true --property key.separator=":"
```

## What's next

Not yet covered: the new KIP-848 incremental rebalance protocol
(`group.protocol=consumer`) compared side-by-side against the classic
protocol used here, `max.poll.records` / `max.poll.interval.ms` and their
effect on rebalancing under slow processing, and Avro/Protobuf with a
Schema Registry as the production-grade alternative to hand-rolled JSON
(de)serialization.
