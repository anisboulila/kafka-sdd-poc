## Purpose

Defines the reliability settings of the order producer and the observable difference between a record accepted by the producer, a publication confirmed by Kafka, and the HTTP response returned to the client.

## ADDED Requirements

### Requirement: Explicit producer reliability settings
The system MUST configure the order producer explicitly with an acknowledgement level, idempotence, and a bounded delivery timeout, instead of relying on implicit client defaults.

#### Scenario: Reliability settings are visible in configuration
- **WHEN** the application is configured
- **THEN** the producer acknowledgement level, idempotence setting, retry behavior, and delivery timeout MUST be set explicitly and documented with their meaning

#### Scenario: Idempotent producer settings are compatible
- **WHEN** producer idempotence is enabled
- **THEN** the acknowledgement level MUST be `all` so that the configuration is accepted by Kafka

### Requirement: Bounded wait for publication confirmation
The system MUST NOT keep a `POST /orders` request waiting indefinitely for Kafka confirmation, and MUST respond with a non-2xx status when confirmation is not obtained within the configured bound.

#### Scenario: Kafka is unavailable
- **WHEN** a client submits a valid order while Kafka cannot confirm publication within the configured bound
- **THEN** the system MUST respond with the existing `503 Service Unavailable` failure response and MUST NOT return a 2xx response

#### Scenario: Kafka confirms in time
- **WHEN** Kafka confirms publication within the configured bound
- **THEN** the system MUST respond with `201 Created` as before

### Requirement: Distinguish accepted, confirmed, and responded
The documentation MUST explain the three stages of an order publication: record handed to the producer, publication confirmed by Kafka, and HTTP response returned to the client, and which of them the HTTP `201 Created` depends on.

#### Scenario: Stages are documented
- **WHEN** a reader consults the project guide or `interview.md`
- **THEN** they MUST find that `201 Created` is returned only after Kafka confirmation, and what can still happen when the HTTP response is lost after confirmation

### Requirement: Producer concepts not implemented are documented as theory
The documentation MUST describe `acks=0`, `acks=1`, `acks=all`, replication, leader and replicas, ISR, and `min.insync.replicas` as theory, and MUST state that the single-broker setup cannot demonstrate their durability effects.

#### Scenario: Theory notes are present and scoped
- **WHEN** a reader consults the theory notes
- **THEN** each concept MUST be explained briefly and marked as not demonstrated by the local single-broker setup
