# Tasks

## 1. Bootstrap the Spring Boot application

- [ ] 1.1 Create the Java 17 Maven application with Spring Boot 3.x and Spring Kafka dependencies; verify the project builds with `mvn test`.

## 2. Provide the local Kafka broker

- [x] 2.1 Add a Docker Compose Kafka service configured for single-broker KRaft mode without ZooKeeper; verify `docker compose config` succeeds and the broker starts and reports ready.
- [x] 2.2 Document the local broker startup, stop, and Kafka CLI inspection commands; verify each documented command works against the running Compose service.

## 3. Implement order creation and publication

- [x] 3.1 Implement `POST /orders` to accept `customerId` and decimal `amount`, generate a string `orderId` and UTC ISO-8601 `createdAt`, and return `201 Created` with the event only after Kafka confirms publication; verify endpoint tests cover the response and event fields.
- [ ] 3.2 Configure the `OrderCreated` JSON producer and explicitly declare `order-events` with one partition and replication factor 1, using `orderId` as the record key; verify a producer integration test observes the expected topic, key, and JSON payload.
- [ ] 3.3 Handle failed or unconfirmed publication without returning a 2xx response, and document the endpoint and event contract in the project guide and `interview.md`; verify failure-path tests and the documented request/response example.

## 4. Implement independent simulated consumers

- [ ] 4.1 Implement the Payment Consumer with its own consumer group and a local simulated payment action; verify a consumer test observes the action for a published event.
- [ ] 4.2 Implement the Notification Consumer with a different consumer group and a local simulated notification action; verify a consumer test observes the action for the same published event independently.
- [ ] 4.3 Document both consumer responsibilities, their separate group behavior, and the intentional absence of real integrations; verify the guide explains why both groups receive the event.

## 5. Verify the complete local flow

- [ ] 5.1 Add an end-to-end check that starts the local broker and application, submits an order, confirms the HTTP publication response, and observes both consumer actions; verify the check passes from a clean local startup.
- [ ] 5.2 Document the end-to-end run and expected observations in the project guide and `interview.md`; verify the walkthrough commands and Kafka CLI observations match the implemented setup.
