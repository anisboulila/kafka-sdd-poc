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
