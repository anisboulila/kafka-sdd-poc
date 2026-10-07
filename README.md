# Kafka SDD POC

This POC demonstrates a minimal order API that publishes `OrderCreated` events to a local Kafka broker.

## Create an order

Start the local Kafka service, then run the Spring Boot application:

```powershell
docker compose -f compose.yaml up -d --wait kafka
mvn spring-boot:run
```

### Request

`POST http://localhost:8080/orders`

The request supplies a customer identifier and a decimal amount:

```json
{
  "customerId": "customer-001",
  "amount": 24.50
}
```

### Successful response

The API generates `orderId` and a UTC ISO-8601 `createdAt`. It returns HTTP `201 Created` only after Kafka confirms that the event was published:

```json
{
  "orderId": "b1d02f1a-d3d2-4a85-8e6e-69202ef3340c",
  "customerId": "customer-001",
  "amount": 24.50,
  "createdAt": "2026-10-06T12:30:00Z"
}
```

The `OrderCreated` JSON payload contains:

| Field | Meaning |
| --- | --- |
| `orderId` | API-generated string identifier and Kafka record key |
| `customerId` | Customer identifier from the request |
| `amount` | Decimal amount from the request |
| `createdAt` | API-generated UTC timestamp in ISO-8601 format |

### Kafka publication

The producer sends the JSON event to the `order-events` topic, using `orderId` as the record key. The topic has one partition and replication factor 1.

The API waits for Kafka's send future to complete before returning success. If Kafka publication fails, is interrupted, or cannot be confirmed, the API returns HTTP `503 Service Unavailable` rather than a 2xx response. This confirms publication to Kafka; it does not mean that a consumer has processed the event.

For example, a publication failure returns a non-2xx problem response:

```json
{
  "title": "Service Unavailable",
  "status": 503,
  "detail": "The order event could not be confirmed by Kafka."
}
```

### Verify the event

Send a request from PowerShell:

```powershell
curl.exe -i -X POST "http://localhost:8080/orders" `
  -H "Content-Type: application/json" `
  --data-raw '{"customerId":"customer-001","amount":24.50}'
```

The response should have status `201 Created` and return the generated `OrderCreated` fields.

Read the records from the beginning, displaying key, partition, and offset:

```powershell
docker compose -f compose.yaml exec kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic order-events `
  --from-beginning `
  --property print.key=true `
  --property print.partition=true `
  --property print.offset=true
```

Alternatively, open Kafka UI at <http://localhost:8081>, select the `order-events` topic, and inspect its messages, keys, partitions, and offsets.

## Consumers

Both consumers independently read the `OrderCreated` JSON event from `order-events`:

```text
                    order-events
                         |
                   OrderCreated
                    /         \
          payment-group    notification-group
                |                 |
       Payment Consumer    Notification Consumer
```

- **Payment Consumer** uses `payment-group` and logs a simulated payment with `orderId` and `amount`.
- **Notification Consumer** uses `notification-group` and logs a simulated notification with `orderId` and `customerId`.

The groups are different so each responsibility receives and processes the same event independently. Kafka tracks each group's offsets separately; one group's progress does not advance the other group's position. Offset values may be numerically equal while referring to independent group positions.

Consumers in the **same group** share work: Kafka assigns each partition to one active consumer in that group at a time. Consumers in **different groups** each get their own view of the topic and can process the same records independently.

The topic currently has one partition, so this POC demonstrates independent consumer groups rather than parallel processing within a group. With several consumers in one group and one partition, only one can be assigned that partition; additional consumers remain idle for it. Multiple partitions and consumer parallelism are topics to explore later.

Both actions are intentionally local simulations, not calls to payment or notification services. This keeps the learning setup self-contained and makes Kafka's topic, group, and offset behavior observable without databases, credentials, or external dependencies.

## Complete local walkthrough

The automated end-to-end integration test exercises the HTTP API against the real local Kafka broker and checks the published record, both consumer actions, each group's committed offset, and lag. It expects Kafka to be running at `localhost:9092`; the test does not start or stop Docker infrastructure. Manual local startup and observation are left to the developer.

1. From the project root, start Kafka and wait for its Compose healthcheck:

   ```powershell
   docker compose -f compose.yaml up -d --wait kafka
   docker compose -f compose.yaml ps kafka
   ```

   The service should report `healthy`.

2. In a separate terminal, start the API:

   ```powershell
   mvn spring-boot:run
   ```

3. Submit an order from another terminal:

   ```powershell
   curl.exe -i -X POST "http://localhost:8080/orders" `
     -H "Content-Type: application/json" `
     --data-raw '{"customerId":"customer-001","amount":24.50}'
   ```

   Expect `201 Created` and an `OrderCreated` JSON response with generated `orderId` and `createdAt`. The API waits until Kafka confirms publication before returning `201`; if publication is not confirmed, it returns `503 Service Unavailable`.

4. Observe the broker record and consumer actions:

   ```powershell
   docker compose -f compose.yaml exec kafka kafka-console-consumer `
     --bootstrap-server localhost:9092 `
     --topic order-events `
     --from-beginning `
     --property print.key=true `
     --property print.partition=true `
     --property print.offset=true
   ```

   The record key is the event's `orderId`; its JSON value contains `orderId`, `customerId`, `amount`, and `createdAt`. The application logs a simulated payment with `orderId` and `amount`, and a simulated notification with the same `orderId` and `customerId`. The distinct groups consume independently.

5. Inspect committed positions and lag for each consumer group:

   ```powershell
   docker compose -f compose.yaml exec kafka kafka-consumer-groups `
     --bootstrap-server localhost:9092 `
     --describe --group payment-group

   docker compose -f compose.yaml exec kafka kafka-consumer-groups `
     --bootstrap-server localhost:9092 `
     --describe --group notification-group
   ```

   The output includes `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG`. Kafka tracks these separately per group; offsets may have the same numeric value without being shared. After both consumers catch up, lag should be zero until another record is produced. Offset commits can take a short time to become visible.

   Alternatively, open Kafka UI at <http://localhost:8081> to inspect the topic records and consumer groups. Expect one record for the order and independent progress for `payment-group` and `notification-group`.
