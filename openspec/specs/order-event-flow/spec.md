# order-event-flow Specification

## Purpose
Defines the minimal order event flow used to demonstrate Kafka producers, topics, message keys, and independent consumers in the local learning POC.

## Requirements

### Requirement: Create and publish an order
The system MUST accept an order request containing `customerId` and `amount`, generate `orderId` and `createdAt`, and publish the resulting event before reporting successful creation.

#### Scenario: Order is published successfully
- **WHEN** a client submits a valid order to `POST /orders` and Kafka confirms publication
- **THEN** the system MUST respond with `201 Created` and the created `OrderCreated` event

#### Scenario: Kafka does not confirm publication
- **WHEN** the system cannot publish the event or Kafka does not confirm publication
- **THEN** the system MUST NOT report successful order creation with a 2xx response

### Requirement: Publish the OrderCreated event contract
The system MUST serialize each order event as JSON with `orderId` and `customerId` strings, a numeric decimal `amount`, and a UTC ISO-8601 `createdAt` timestamp, and MUST use `orderId` as the Kafka message key.

#### Scenario: Event is published to the order topic
- **WHEN** an order is successfully created
- **THEN** its `OrderCreated` JSON event MUST be published to the `order-events` topic with its `orderId` as the message key

### Requirement: Process order events independently
The Payment Consumer and Notification Consumer MUST each receive the order events independently and perform only their respective simulated action, without requiring a database or external payment or notification service.

#### Scenario: Payment Consumer receives an order
- **WHEN** an `OrderCreated` event is published to `order-events`
- **THEN** the Payment Consumer MUST receive it independently and simulate payment processing

#### Scenario: Notification Consumer receives an order
- **WHEN** an `OrderCreated` event is published to `order-events`
- **THEN** the Notification Consumer MUST receive it independently and simulate sending a notification
