# Design

## Context

State verified in the repository (see proposal.md for motivation):

- Producer: `OrderService` calls `KafkaTemplate.send(...).get()` with no explicit timeout; `OrderExceptionHandler` maps publication failures to `503`. `application.properties` sets only serializers and bootstrap servers; `acks`, idempotence, retries, `delivery.timeout.ms` and `max.block.ms` are not set, so the Kafka client defaults apply. **Verified (task 1.1, `KafkaEffectiveDefaultsTest`, kafka-clients 3.9.1)**: effective `acks=all` (stored internally as `-1`), `enable.idempotence=true`, `retries=2147483647`, `delivery.timeout.ms=120000`, `max.block.ms=60000` (also `request.timeout.ms=30000`, `linger.ms=0`). Consequence: the producer is already acks=all + idempotent by default; task 1.2 makes this explicit and bounds the wait, it does not change the guarantees. With the default `max.block.ms` and `delivery.timeout.ms`, `send(...).get()` can wait up to about 60 s to obtain metadata, or up to 120 s to deliver, before failing, which confirms the unbounded-wait risk addressed by D1.
- Consumers: `PaymentConsumer` (`payment-group`) and `NotificationConsumer` (`notification-group`) are `@KafkaListener` methods that only log. No error handler, retry or DLT is configured explicitly. **Verified (task 1.1)**: no `enable.auto.commit` or `auto.offset.reset` is set by the application; the container runs the real consumers with `enable.auto.commit=false` (seen in the consumer start logs; `ConsumerFactory.isAutoCommit()` still reports the raw client default `true`, which is misleading) and `auto.offset.reset=latest` unless a test overrides it. Ack mode is `BATCH` for both listeners, so offsets are committed after the listener handled the records of a poll, which is at-least-once. No `CommonErrorHandler` is set on the containers, so Spring Kafka's `DefaultErrorHandler` applies: a failing record is delivered 10 times in total (9 retries) with no delay, then only logged and skipped (offset moves on) and **no DLT** exists. A blocking retry: the partition is stuck during the attempts. This is what 2.x replaces.
- Topic: `OrderKafkaConfiguration` declares `order-events` with 1 partition, RF 1. `compose.yaml` has one KRaft broker, `KAFKA_NUM_PARTITIONS: "1"`, no data volume.
- Tests: `OrderFlowEndToEndIntegrationTest` hard-codes partition 0 and waits for 1 assigned partition per group; other integration tests rely on `spring.kafka.listener.auto-startup=false` to avoid competing for the single partition in the fixed groups. Tests require a running broker on `localhost:9092`.
- The archived spec `order-event-flow` does not mention partitions, `acks`, retries or failures, so the new behavior is captured as ADDED capabilities with no MODIFIED requirement.

## Goals / Non-Goals

**Goals:**
- Small, observable demonstrations of: explicit producer reliability, retry then DLT, in-memory idempotence, multi-partition key placement, partition sharing in a group, and rebalance.
- Name the real delivery semantics of the result and avoid claiming exactly-once.

**Non-Goals:**
- Multi-broker cluster, replication/ISR demonstration, `min.insync.replicas` behavior, log compaction, Kafka transactions/EOS, `@RetryableTopic`, DLT reprocessing, database, real payment/notification logic, monitoring.

## Decisions

Decisions marked **[Proposed]** are recommendations, not validated. They are listed again in Open questions.

### D1. Producer reliability: explicit configuration, bounded wait **[Decided and implemented in task 1.2]**
Set explicitly in `application.properties`: `acks=all`, `enable.idempotence=true`, `retries=2147483647`, `delivery.timeout.ms=5000`, `request.timeout.ms=3000` (Kafka requires `delivery.timeout.ms >= request.timeout.ms + linger.ms`, and the 30 s default of `request.timeout.ms` would otherwise be rejected), `max.block.ms=2000`. The HTTP wait is `app.orders.publication-timeout=6s`, applied with `send(...).get(timeout)`; it is deliberately just above `delivery.timeout.ms` so the producer normally reports its own definitive result first and the HTTP bound is only a safety net. A `TimeoutException` is mapped to `OrderPublicationException`, hence the existing `503`. Worst case for a request: up to `max.block.ms` inside `send()` plus the `get` timeout (about 8 s), instead of about 1 to 3 minutes before. Note: after a timeout the outcome is unknown (the record may still reach Kafka), a known limit documented, not solved here.
Rationale: the three stages (record accepted by the producer buffer -> confirmed by the broker -> HTTP response) become independently explainable and testable. Alternative: keep implicit defaults and only document them (less to change, but nothing observable and the unbounded wait stays).
`acks=0/1` are NOT implemented as modes; they are documented as theory because one broker cannot show their durability difference. A test may assert the configured values only.

### D2. Failure trigger for Payment **[Proposed]**
A configuration property naming a `customerId` that makes Payment fail (for example `app.payment.fail-customer-id`), checked in `PaymentConsumer`. Rationale: no change to the `OrderCreated` contract or to `POST /orders`. Alternatives: special `amount` value (couples business data to a test concern); header-based trigger (requires producer changes).

### D3. Retry and DLT: `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` **[Proposed]**
Blocking retry with a fixed backoff and a small attempt count, applied only to the Payment listener container, then publish to a DLT named by the Spring default convention (`order-events.DLT`) **[to validate]**. Rationale: smallest mechanism that shows retry -> exhaustion -> DLT with a visible record; blocking behavior (the partition is paused during retries) is itself a teaching point. `@RetryableTopic` is rejected: non-blocking retry topics add several topics and concepts, not needed for the stated goals.
Constraint to verify: by default the recovered record is sent to the same partition number as the source, so the DLT must be declared explicitly with at least as many partitions as `order-events` (or use a destination resolver). Notification keeps its own container/behavior unchanged, which preserves independence.

### D4. In-memory idempotence **[Proposed]**
A small component holding processed `orderId` values, consulted by `PaymentConsumer`; an id is recorded only after successful processing so retries are not skipped. Limits (lost on restart, not shared across instances, not atomic with a business action, unbounded growth) are documented. Alternative: no component, rely on Kafka only (rejected: nothing to demonstrate). Production approaches (unique constraint, inbox table) stay conceptual.

### D5. Partition count and topic lifecycle **[To validate]**
Make the `order-events` partition count configurable and target 2 for the main topic. Kafka can increase but not decrease partitions, and increasing changes key-to-partition mapping for later records. With no data volume, `docker compose down` followed by `up` resets the broker; document this as the reset path.
For the 1-partition vs 2-partition demonstration, prefer **dedicated temporary topics created by the tests** (unique names and group ids) instead of mutating `order-events`. Rationale: both cases can run in one session, do not depend on broker state, and avoid breaking the main flow. Cost: the demonstration lives in tests plus documented CLI commands, not in the running application.

### D6. Multiple consumers and rebalance: `concurrency` vs several instances **[To validate]**
- Automated checks: two listener containers or `concurrency=2` in one JVM with a unique group per test, asserting assignments through the consumer/Admin API. Deterministic and fast.
- Manual observation: two application instances on different `server.port` values, observed with Kafka UI or `kafka-consumer-groups --describe`. Closer to real scale-out and makes join/leave visible by starting or stopping a process.
Recommended: use `concurrency` (configurable) for tests and the two-instance run for the documented manual rebalance experiment. Not decided until validated. The second instance also needs a different `server.port`, and both consumer groups' members double up, which must be explained.

### D7. Delivery semantics statement **[Proposed]**
After this change: producer to broker is at-least-once with idempotent-producer duplicate protection per producer session; broker to consumers is **at-least-once** (offset committed after listener returns: ack mode `BATCH`, auto-commit off, verified in task 1.1); Payment adds a best-effort in-memory duplicate filter, so the effective result is "at-least-once with limited deduplication", not exactly-once. At-most-once and exactly-once are described only in `interview.md`.

### D8. Test isolation
Introduce unique consumer group ids (and temporary topics where needed) for new tests, and update existing tests to not assume partition 0 or a single assigned partition. Keep `auto-startup=false` where listeners are not needed.

### D9. Malformed message handling **[To validate]**
Whether to add `ErrorHandlingDeserializer` so an undeserializable record does not block the partition and is sent to the DLT. Default proposal: out of the implemented scope; documented as theory. Needs a decision before D3 tasks are finalized.

## Risks / Trade-offs

- Existing tests break with 2 partitions -> adapt them in the same task that changes the partition count; do not change partitions earlier.
- Blocking retries pause a partition -> accepted and documented; fixed small backoff keeps tests fast.
- Rebalance tests are timing-sensitive -> use unique groups, explicit wait for assignment with timeouts, avoid sleeps.
- DLT partition mismatch -> declare the DLT with enough partitions and assert it in a test.
- Idempotence state is per instance -> with two instances, a duplicate can be processed again by the other instance; documented limitation.
- Increasing partitions on an existing topic changes key placement -> documented reset path.
- Timeout values too small can cause false 503 on a slow machine -> choose conservative local values.

## Migration Plan

1. Before the partition task, existing local brokers keep the 1-partition topic; to adopt 2 partitions run `docker compose down` and `up` (no data volume) or alter the topic, as decided in D5.
2. Rollback: revert the change; partitions cannot be reduced on an existing topic, so use the same reset.

## Open Questions

The questions below affect what gets built and MUST be validated by the user before the related task group is applied (each such group starts with a decision-confirmation task).

### Open questions / Decisions to validate
1. Final partition count of `order-events` (2 proposed) and whether the default stays 1 outside the experiment.
2. `concurrency` versus multiple instances for the multi-consumer and rebalance demonstrations (D6).
3. Exact retry/backoff values and number of attempts.
4. DLT name and partitions (D3), and whether DLT records are only observed, never reprocessed.
5. Failure trigger design (D2).
6. 1-partition vs 2-partition test strategy: temporary topics (proposed) versus altering `order-events` (D5).
7. Malformed message handling (D9).
8. ~~Exact producer values and HTTP wait bound~~ Decided in task 1.2 (D1): delivery 5 s, request 3 s, max.block 2 s, HTTP wait 6 s; `acks=all` and idempotence stay explicit.
9. ~~Effective current defaults of the producer and Spring error handling~~ Verified in task 1.1 (see Context). Remaining decision: whether to keep `acks=all` and idempotence explicit even though they already match the defaults (D1: proposed yes, for visibility).
