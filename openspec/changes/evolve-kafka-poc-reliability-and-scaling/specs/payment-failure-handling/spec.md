## Purpose

Defines how the Payment Consumer reacts to a processing failure with bounded retries and a Dead Letter Topic, while the Notification Consumer remains independent.

## ADDED Requirements

### Requirement: Simulated Payment failure
The Payment Consumer MUST be able to simulate a processing failure for an event selected by an explicit, documented trigger, and MUST NOT fail for events that do not match the trigger.

#### Scenario: Failure is triggered deliberately
- **WHEN** an `OrderCreated` event matching the failure trigger is consumed by the Payment Consumer
- **THEN** the Payment Consumer MUST fail its processing of that event

#### Scenario: Normal events are unaffected
- **WHEN** an `OrderCreated` event that does not match the trigger is consumed
- **THEN** the Payment Consumer MUST complete the simulated payment without error

### Requirement: Bounded retry with backoff
When Payment processing fails, the system MUST retry the same event a bounded number of times with a delay between attempts, and MUST NOT retry indefinitely.

#### Scenario: Failure is retried then exhausted
- **WHEN** Payment processing of an event keeps failing
- **THEN** the system MUST stop after the configured number of attempts

#### Scenario: Retry succeeds
- **WHEN** Payment processing fails on an attempt and succeeds on a later attempt before exhaustion
- **THEN** the event MUST NOT be published to the Dead Letter Topic

### Requirement: Dead Letter Topic publication
When retries are exhausted, the system MUST publish the failed record to a Dead Letter Topic where it can be observed with Kafka tooling, preserving the original key and value and indicating the original topic, partition, offset, and failure reason.

#### Scenario: Exhausted event reaches the DLT
- **WHEN** Payment retries are exhausted for an event
- **THEN** that event MUST be published to the Dead Letter Topic with the original `orderId` key and payload

#### Scenario: Consumption continues after DLT publication
- **WHEN** a failed event has been published to the Dead Letter Topic
- **THEN** the Payment Consumer MUST continue with subsequent events

### Requirement: Notification independence from Payment failure
The Notification Consumer MUST process every `OrderCreated` event regardless of whether the Payment Consumer fails, retries, or sends the event to the Dead Letter Topic.

#### Scenario: Payment fails but Notification succeeds
- **WHEN** the Payment Consumer fails on an event
- **THEN** the Notification Consumer MUST still receive and process that event once under its own consumer group

### Requirement: Retry versus DLT is documented
The documentation MUST explain the difference between retry and Dead Letter Topic, and MUST state that DLT records are not automatically reprocessed by this POC.

#### Scenario: Retry and DLT explanation
- **WHEN** a reader consults `interview.md`
- **THEN** they MUST find when a failure is retried, when it goes to the DLT, and why reprocessing is a separate concern
