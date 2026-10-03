# Proposal

## Why

The project requirements define a learning scenario for Kafka, but the repository has no capability specification or application implementation yet. This change establishes a small, testable contract for the Order API-to-Kafka flow and its independent consumers so later implementation changes can be generated from a shared specification.

## What Changes

- Define an HTTP `POST /orders` flow in which the request supplies `customerId` and `amount`, and the API generates `orderId` and `createdAt`.
- Define the JSON `OrderCreated` event, its `order-events` topic, and `orderId` as its Kafka message key.
- Define independent Payment and Notification consumer responsibilities and the local KRaft-based Kafka setup needed for the happy path.
- Keep the first implementation scope minimal: simulated consumer actions, no database or external services, and no advanced retry, DLT, or delivery-semantics behavior.

## Capabilities

### New Capabilities

- `order-event-flow`: Create and publish an order event, then independently consume it for simulated payment and notification processing.

### Modified Capabilities

None.

## Impact

- Establishes the future HTTP API contract and Kafka event flow; no existing API or application code is present in the repository.
- The implementation is expected to use Java 17, Spring Boot 3.x, Spring Kafka, Maven, and Docker Compose with Kafka in KRaft mode.
- Does not change `requirements.md` or introduce a database, cloud service, frontend, or additional infrastructure.
