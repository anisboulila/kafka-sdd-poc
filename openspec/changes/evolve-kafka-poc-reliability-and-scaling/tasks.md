# Tasks

Each group lands its own tests and documentation (`README.md` / `interview.md`). Groups that depend on an open decision (see design.md "Open questions / Decisions to validate") start with a confirmation task; do not implement past it until the decision is validated.

## 1. Baseline and producer reliability

- [x] 1.1 Verify the effective current producer defaults (`acks`, idempotence, retries, `delivery.timeout.ms`, `max.block.ms`) and the current consumer error-handling/ack behavior with a test or logged configuration; record the findings in design.md replacing the "À vérifier" notes.
- [ ] 1.2 Confirm decision D1 (explicit values and HTTP wait bound), then set the producer reliability properties explicitly and bound the `send(...).get(...)` wait; verify `OrderServiceTest` covers confirmation within the bound and timeout mapped to `503`, and the existing tests still pass.
- [ ] 1.3 Document the accepted / confirmed / HTTP-responded stages and the `acks=0/1/all`, replication, leader/replica, ISR and `min.insync.replicas` theory (marked not demonstrated on one broker) in `README.md` and `interview.md`; verify each requirement scenario of `producer-reliability` has a matching documented answer.

## 2. Payment retry and Dead Letter Topic

- [ ] 2.1 Confirm decisions D2, D3 and D9 (failure trigger, retry/backoff values, DLT name and partitions, malformed-message handling); verify the decisions are written in design.md.
- [ ] 2.2 Add the simulated Payment failure trigger; verify a unit test shows a matching event fails and a non-matching event succeeds, with no change to `OrderCreated` or `POST /orders`.
- [ ] 2.3 Configure bounded retry with backoff and DLT publication for the Payment listener only, and declare the DLT topic; verify an integration test with unique group ids shows retries, then a record on the DLT with the original key and payload.
- [ ] 2.4 Verify Notification independence: an integration test shows Notification processes the failing event once while Payment is retrying/dead-lettered, and Payment continues with the next event.
- [ ] 2.5 Document the failure scenario, how to observe the DLT with Kafka CLI/Kafka UI, and retry vs DLT in `README.md` and `interview.md`; verify the documented commands match the implemented topic name. (Documented commands are checked by the user, not by the assistant.)

## 3. Payment idempotence

- [ ] 3.1 Implement the in-memory `orderId` idempotency guard, recording an id only after successful processing; verify unit tests cover duplicate skip, distinct orders, and retry after failure not skipped.
- [ ] 3.2 Add an integration test that redelivers the same event to the Payment Consumer and verifies a single simulated payment action; verify the full suite still passes.
- [ ] 3.3 Document the idempotence limits (restart, multiple instances, atomicity, growth) and production concepts, plus the delivery semantics statement (at-most-once, at-least-once, exactly-once, offset commit timing, what this POC provides) in `README.md` and `interview.md`; verify the POC is never described as exactly-once.

## 4. Multiple partitions

- [ ] 4.1 Confirm decisions D5 (partition count, topic reset path, test strategy); verify the decision is written in design.md.
- [ ] 4.2 Make the `order-events` partition count configurable and apply the chosen value (including the DLT partition constraint); verify the topic description shows the expected partition count after a broker reset.
- [ ] 4.3 Adapt `OrderFlowEndToEndIntegrationTest`, `OrderProducerIntegrationTest` and related tests so they no longer assume partition 0 or one assigned partition; verify the full Maven suite passes against the new topic.
- [ ] 4.4 Add a test showing events with the same `orderId` share a partition and both consumer groups still receive every event; verify with unique group ids.
- [ ] 4.5 Document partitions, key placement, per-partition ordering (no global order) and the topic reset/alter caveat in `README.md` and `interview.md`; verify against the implemented configuration.

## 5. Multiple consumers in a group

- [ ] 5.1 Confirm decision D6 (`concurrency` vs several instances for tests and for the manual experiment); verify the decision is written in design.md.
- [ ] 5.2 Add a test using a temporary one-partition topic and two consumers in one group; verify exactly one consumer is assigned the partition and the other is idle.
- [ ] 5.3 Add a test using a temporary two-partition topic and two consumers in one group; verify each consumer gets a distinct partition.
- [ ] 5.4 Make Payment consumer concurrency configurable as decided and verify Notification's group assignment is unchanged when Payment scales.
- [ ] 5.5 Document the 1-partition vs 2-partition behavior, why a partition cannot be shared inside a group, and how parallelism is bounded by partitions in `README.md` and `interview.md`; verify against the tests.

## 6. Rebalance

- [ ] 6.1 Make partition assignment and revocation observable (logs and/or documented `kafka-consumer-groups --describe` output) without changing consumer business behavior; verify a test captures the assignment events.
- [ ] 6.2 Add a test where a consumer joins and then leaves a group on a two-partition topic; verify the partitions are redistributed and returned, using explicit waits rather than fixed sleeps.
- [ ] 6.3 Document the manual rebalance experiment (start/stop a second instance or use `concurrency`, as decided) with the expected observations in `README.md` and `interview.md`; verify the documented steps and ports match the implementation. (Manual execution is done by the user.)

## 7. Final consistency check

- [ ] 7.1 Review `README.md`, `interview.md` and the specs for consistency with the implementation, mark which concepts are implemented and observed versus theory (replication, ISR, leader/replica, log compaction, exactly-once/transactions); verify no document claims exactly-once or multi-broker behavior.
- [ ] 7.2 Run the full Maven test suite against the local broker and verify it passes.
