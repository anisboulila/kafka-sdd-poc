# Design

## Context

The repository currently contains `requirements.md` and the OpenSpec configuration, but no application, Maven build, or Kafka deployment files. The capability contract is in `specs/order-event-flow/spec.md`. The design uses the requested Java 17, Spring Boot 3.x, Spring Kafka, Maven, and local Docker Compose stack.

## Goals / Non-Goals

**Goals:**
- Keep the first runnable flow local, observable, and understandable.
- Make each consumer an independent subscriber to the same event stream.
- Set defaults that can later be changed to demonstrate partitioning, parallelism, and replication.

**Non-Goals:**
- Implement retries, a dead-letter topic, idempotent consumer storage, or exactly-once processing in this foundation.
- Simulate real payment or notification integrations.

## Decisions

- **Use a single local Kafka broker in KRaft mode through Docker Compose.** This avoids ZooKeeper and external infrastructure, as required. A broker container is simpler than introducing a multi-node cluster; replication behavior is deferred.
- **Start `order-events` with one partition and replication factor 1.** This keeps local setup deterministic and supports the initial single-flow demonstration. The trade-off is that this baseline cannot demonstrate consumer parallelism or broker fault tolerance; later changes can increase partitions or add brokers.
- **Use `orderId` as the record key.** Records for the same order then have a stable key for Kafka partitioning. With one partition, all records are ordered within the topic; ordering guarantees across future partitions will be per partition.
- **Give Payment and Notification separate consumer groups.** Kafka will deliver each event to each group independently, instead of load-balancing an event between the two responsibilities. Each consumer simulates its action locally and logs the received order.
- **Wait for Kafka's publication result before returning a successful HTTP response.** The endpoint returns `201 Created` with the event only after confirmation; on failure it returns a non-2xx response. This avoids reporting success before publication, but does not provide transactional coupling or exactly-once behavior.
- **Keep the event deliberately small.** `orderId` and `customerId` are strings, `amount` is a JSON decimal without currency, and `createdAt` is an ISO-8601 UTC timestamp. The request supplies `customerId` and `amount`; the API creates the identifier and timestamp.
- **Declare the topic explicitly in application configuration rather than relying on broker auto-creation.** This makes the one-partition and replication settings visible and repeatable for the local setup. Kafka CLI remains available for inspection and learning exercises.

## Risks / Trade-offs

- **A single partition limits consumer concurrency and global throughput** → Keep that as an intentional first-step constraint; a later learning change can increase partitions and compare consumer-group assignment.
- **Replication factor 1 provides no replica-based availability** → Keep the limitation explicit; demonstrate replication only with a later multi-broker setup.
- **A publication timeout can leave the caller uncertain whether the broker persisted the record** → Do not claim exactly-once order creation; retries, idempotency, and consumer deduplication remain separate learning topics.
- **An amount without a currency is not suitable for real financial processing** → Treat payment as a simulation and keep currency and external payment behavior out of this POC foundation.

## Migration Plan

No existing application or runtime data needs migration. The implementation can introduce the Compose broker, topic configuration, API, producer, and consumers as a new local development flow.
