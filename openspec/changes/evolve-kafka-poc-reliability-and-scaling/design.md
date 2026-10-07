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

### D2. Failure trigger for Payment **[Decided in task 2.1; implementation in 2.2]**
**Decision:** a configuration property `app.payment.fail-customer-id`, empty by default (no failure). When the consumed `OrderCreated.customerId` equals it, `PaymentConsumer` throws an exception instead of simulating the payment. The failure is deterministic: it happens on every delivery of a matching event, so retries are exhausted and the DLT path is reachable. Non-matching events are unaffected.
**Why:** no change to the `OrderCreated` contract or to `POST /orders`, nothing to add to the producer, trivially testable (set the property, post an order with that `customerId`), and clearly a simulation switch, not payment logic. Rejected: special `amount` (couples business data to a test concern), header-based trigger (requires producer changes), random failure (not deterministic).
**Implementation future (2.2):** add the property and the throw in `PaymentConsumer`, with a unit test. Not implemented yet.
**Note on the spec scenario "Retry succeeds":** an always-failing trigger cannot show success on a later attempt. That scenario is covered at handler/unit level in 2.3 (a stub that fails a fixed number of times), not through the trigger.

### D3. Consumer retry and DLT: `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` **[Decided in task 2.1; implementation in 2.3]**
Not to be confused with the **producer** retries of task 1.2 (`retries`, `delivery.timeout.ms`): those re-send a record to the broker. The retries here are **consumer-side**: the listener container re-delivers the same record to the Payment listener after a processing failure.
**Decision (values):**
- Strategy: blocking retry with `FixedBackOff`: interval 1000 ms, 3 retries after the first failure, so 4 deliveries in total and about 3 s of delay. No exponential growth and no maximum delay: a fixed delay is simplest to explain and keeps tests fast.
- After exhaustion: `DeadLetterPublishingRecoverer` publishes the failed record to the DLT, then the offset moves on and Payment continues with the next record.
- Scope: applied to the Payment listener container only. Notification keeps its current behavior (Spring's default handling) and is unaffected.
- DLT name: `order-events.DLT` (Spring default `<topic>.DLT`). Partitions: 1 and replication factor 1, matching `order-events` today. The recoverer sends to the same partition number as the source record by default, so the DLT must always have at least as many partitions as `order-events` (to be revisited in task 4.2). The topic is declared explicitly (not auto-created) in 2.3.
- The recoverer keeps the original key and value and adds headers (original topic, partition, offset, exception). DLT records are only observed, never reprocessed, in this POC.
**Why:** smallest mechanism that shows retry -> exhaustion -> DLT with a visible record. Blocking behavior (the partition waits during retries) is itself a teaching point. `@RetryableTopic` is rejected: it adds retry topics and non-blocking concepts not needed here.
**Implementation future:** 2.3 (handler, DLT declaration, integration test), 2.4 (Notification independence), 2.5 (docs). Nothing of this is implemented yet; today the default handler still applies (10 deliveries, then skip, no DLT; see Context).

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

### D9. Malformed message handling **[Decided in task 2.1; documentation only]**
**Decision:** two failure kinds are kept distinct:
- **Processing error** (the record deserializes into `OrderCreated`, then the listener fails): handled by D3 (retry, then DLT). This is the only kind implemented in this change.
- **Malformed message / deserialization failure** (the payload cannot become `OrderCreated`, so the listener is never called): **not implemented**. `ErrorHandlingDeserializer` is not added; the topic is documented as a theory item (poison pill).
**Why:** the stated goal is observing retry and DLT for a processing failure; handling poison pills adds a wrapper deserializer on the shared consumer configuration, which also affects Notification and its tests. It is a separate concern.
**Facts not verified (À vérifier):** how the current configuration (plain `JsonDeserializer`, no `ErrorHandlingDeserializer`) reacts to a malformed record in Spring Kafka 3.3.10 was NOT tested. Documentation in 2.5 must state it as theory ("a poison pill fails before the listener; the usual remedy is `ErrorHandlingDeserializer` plus DLT") and must not claim an observed behavior. Both consumer groups share the same deserializer settings, so a malformed record concerns both.
**Implementation future:** none in this change, except the documentation in 2.5. Revisit only if the user asks.

### Decision summary (task 2.1)
| Id | Decision | Implemented in |
|---|---|---|
| D2 | `app.payment.fail-customer-id` throws in `PaymentConsumer` | 2.2 |
| D3 | `FixedBackOff(1000 ms, 3 retries)` then `order-events.DLT` (1 partition, RF 1), Payment only | 2.3, 2.4, 2.5 |
| D9 | Malformed messages out of implementation scope, documented as theory | 2.5 (docs) |

Naming note: in this design the DLT choices are part of D3 and D9 is the malformed-message decision.

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
3. ~~Exact retry/backoff values~~ Decided in task 2.1 (D3): `FixedBackOff` 1000 ms, 3 retries (4 deliveries).
4. ~~DLT name and partitions~~ Decided in task 2.1 (D3): `order-events.DLT`, 1 partition, RF 1, observe only. Must follow the partition count of `order-events` in task 4.2.
5. ~~Failure trigger design~~ Decided in task 2.1 (D2): `app.payment.fail-customer-id`.
6. 1-partition vs 2-partition test strategy: temporary topics (proposed) versus altering `order-events` (D5).
7. ~~Malformed message handling~~ Decided in task 2.1 (D9): out of implementation scope, documented as theory; current behavior not verified.
8. ~~Exact producer values and HTTP wait bound~~ Decided in task 1.2 (D1): delivery 5 s, request 3 s, max.block 2 s, HTTP wait 6 s; `acks=all` and idempotence stay explicit.
9. ~~Effective current defaults of the producer and Spring error handling~~ Verified in task 1.1 (see Context). Remaining decision: whether to keep `acks=all` and idempotence explicit even though they already match the defaults (D1: proposed yes, for visibility).
