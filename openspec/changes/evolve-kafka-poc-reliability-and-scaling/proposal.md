# Proposal

## Why

The foundation POC (archived as `define-kafka-poc-foundation`) proves the basic flow: `POST /orders` -> `order-events` -> independent Payment and Notification consumers. It deliberately left out the problems met in real Kafka systems: what the producer guarantees, what happens when a consumer fails, duplicates, and how consumers share partitions. These are required learning goals in `requirements.md` (sections 4 and 10) and are the next learning phase announced in `README.md` and `interview.md`.

## What Changes

- Make producer reliability explicit and observable: `acks`, idempotence, retries, `delivery.timeout.ms`, and a bounded HTTP wait. Today none of these is configured, so Kafka client defaults apply (to be verified against the client version in use) and the blocking `send(...).get()` has no explicit time bound.
- Add controlled, simulated failure to the Payment Consumer, with bounded retry/backoff and a Dead Letter Topic (DLT) that can be observed in Kafka. Today there is no error handling configured beyond Spring Kafka defaults. Notification Consumer stays unchanged and independent.
- Add an in-memory idempotency guard in the Payment Consumer keyed by `orderId`, with documented limits.
- Allow `order-events` to have more than one partition, keeping `orderId` as the message key. Existing tests that assume a single partition (partition 0, one assigned partition) are adapted.
- Demonstrate several consumers in one consumer group: 1 partition + 2 consumers vs 2 partitions + 2 consumers, plus a small join/leave rebalance experiment.
- State the delivery semantics actually provided after the change, and add short theory notes for concepts that are NOT implemented (`acks` values and replication/ISR, `min.insync.replicas`, log compaction, retry vs DLT, exactly-once/transactions).
- No application behavior of `POST /orders` or of the `OrderCreated` contract changes for a successful request.

Out of scope: multiple brokers, replication experiments, database, Kafka transactions/EOS, `@RetryableTopic`, Schema Registry/Avro, Streams, Connect, Debezium, Outbox, Saga, Kubernetes, cloud, auth, frontend, monitoring, real payment/notification logic.

## Capabilities

### New Capabilities
- `producer-reliability`: explicit producer acknowledgement, idempotence, retry and timeout settings, and the distinction between producer-accepted, Kafka-confirmed and HTTP-returned.
- `payment-failure-handling`: simulated Payment failure, bounded retry with backoff, DLT publication, and isolation of Notification Consumer from Payment failures.
- `payment-idempotence`: duplicate `OrderCreated` deliveries for the same `orderId` are not processed twice by the Payment Consumer (in-memory, POC-grade).
- `partitioned-consumption`: multi-partition `order-events` with key-based placement, partition assignment among consumers of one group, per-partition ordering, and observable rebalance.

### Modified Capabilities
<!-- None. The archived `order-event-flow` requirements (create/publish, event contract, independent consumers) remain valid. It says nothing about partition count, acks, retries or failures, so nothing there needs MODIFIED. To be re-checked if a decision below changes the HTTP failure contract. -->

## Impact

- Code (at apply time only): `OrderService`, `OrderKafkaConfiguration`, `PaymentConsumer`, new error-handler/DLT configuration, a small idempotency component, `application.properties`.
- Tests: `OrderFlowEndToEndIntegrationTest` (hard-coded partition 0 and single-partition assertions), `OrderProducerIntegrationTest`, `OrderServiceTest`, plus new targeted tests; test isolation (group IDs/topics) must be reviewed because tests share a persistent local broker and fixed group ids.
- Infrastructure: `compose.yaml` sets `KAFKA_NUM_PARTITIONS: "1"` (default for auto-created topics); the broker stays single-node KRaft. No volume is mounted, so `docker compose down` resets topics.
- Docs: `README.md` and `interview.md` (update "to study later" items, add delivery-semantics and theory sections).
- Prerequisite satisfied: `define-kafka-poc-foundation` is archived and `openspec/specs/order-event-flow` exists.
