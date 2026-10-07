## Purpose

Defines how the Payment Consumer avoids processing the same order twice when Kafka delivers the same event more than once, using a POC-grade in-memory mechanism with documented limits.

## ADDED Requirements

### Requirement: Payment processes each order once per consumer lifetime
The Payment Consumer MUST NOT perform the simulated payment action more than once for the same `orderId` while its idempotency record is retained, even if the same event is delivered several times.

#### Scenario: Duplicate delivery is ignored
- **WHEN** an `OrderCreated` event with an `orderId` already processed by the Payment Consumer is delivered again
- **THEN** the Payment Consumer MUST NOT repeat the simulated payment and MUST record that a duplicate was skipped

#### Scenario: Distinct orders are processed
- **WHEN** events with different `orderId` values are delivered
- **THEN** the Payment Consumer MUST process each of them

### Requirement: Failed processing is not recorded as processed
An `orderId` MUST NOT be recorded as processed when its Payment processing fails, so that a retry of that event is not skipped as a duplicate.

#### Scenario: Retry after failure is processed
- **WHEN** Payment processing of an event fails and the event is retried
- **THEN** the retry MUST be processed and MUST NOT be skipped as a duplicate

### Requirement: Idempotency limits are documented
The documentation MUST state that the in-memory record is lost on restart, is not shared between instances, is not atomic with a real business operation, and may grow without bound, and MUST name production approaches only as concepts.

#### Scenario: Limits are documented
- **WHEN** a reader consults the project guide or `interview.md`
- **THEN** they MUST find each limitation and the corresponding production concept, without any database being introduced in this POC

### Requirement: Delivery semantics are stated
The documentation MUST define at-most-once, at-least-once, and exactly-once delivery, MUST state which one the POC provides after this change, and MUST NOT describe the POC as exactly-once.

#### Scenario: Effective semantics are stated precisely
- **WHEN** a reader consults the delivery semantics section
- **THEN** they MUST find that Kafka delivery to the consumers is at-least-once, that the Payment duplicate handling is limited by the in-memory scope, and how offset commit timing relative to processing causes loss or duplicates
