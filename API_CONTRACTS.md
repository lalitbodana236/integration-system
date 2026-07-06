# Integration Platform API Contracts

This file captures the runtime API contracts used to test the integration platform.

## 1) Ingestion Service

- Base URL: `http://localhost:10001`
- Resource: `POST /api/{type}`
- Supported `{type}` values:
  - `inventory`
  - `po`
  - `so`
  - `checklist`
  - `location`

### Query Parameters

- `id` (required): unique request id for the API call
- `customerId` (optional): partition key candidate
- `region` (optional): `INDIA`, `US`, `UK` (case-insensitive; normalized internally)

### Headers

- `Content-Type: application/json`
- `X-Event-Id` (optional): client-provided event id for idempotency
- `X-Correlation-Id` (optional): distributed trace/correlation id

### Request Body

Body is raw JSON string (schema-flexible payload). Typical contract used by load test:

```json
{
  "eventType": "PO",
  "orderId": "PO-00001",
  "customerId": "CUST-1",
  "region": "INDIA",
  "item": "Laptop",
  "quantity": 2,
  "price": 50001,
  "warehouse": "WH-1",
  "timestamp": 1716240000000
}
```

### Success Response

- HTTP: `202 Accepted`
- Content-Type: JSON
- Shape:

```json
{
  "status": "ACCEPTED",
  "message": "Event queued for asynchronous processing",
  "eventId": "uuid-or-client-event-id",
  "correlationId": "uuid",
  "topic": "raw-events"
}
```

### Duplicate Response

- HTTP: `202 Accepted`
- Shape:

```json
{
  "status": "DUPLICATE",
  "message": "Duplicate request ignored",
  "eventId": "client-event-id",
  "correlationId": "uuid",
  "topic": "raw-events"
}
```

## 2) Media Ingestion Endpoint

- Base URL: `http://localhost:10001`
- Resource: `POST /api/media`
- Form-data:
  - `file` (required): multipart file
  - `id` (required): request id
  - `customerId` (optional)
  - `region` (optional)
- Headers:
  - `X-Event-Id` (optional)
  - `X-Correlation-Id` (optional)
- Response shape: same `ApiAckResponse` JSON contract as above.

## 3) End-to-End Event Flow Contract

For a successful ingestion event:

1. Ingestion publishes to `raw-events`
2. Processing consumes `raw-events`, persists workflow state, routes to:
   - `region-india`, `region-us`, or `region-uk`
3. Regional service consumes regional topic, persists region workflow, publishes `notification-events`
4. Notification service consumes `notification-events` and dispatches:
   - `EMAIL`
   - `SMS`
   - `WEBHOOK`

## 4) Retry and DLQ Contract

### Processing Retry Chain

- Main: `raw-events`
- Retry: `retry-processing-5s` -> `retry-processing-1m` -> `retry-processing-10m`
- DLQ: `dlq-processing`

### Regional Retry Chain

- Main: `region-india|region-us|region-uk`
- Retry: `retry-region-5s` -> `retry-region-1m` -> `retry-region-10m`
- DLQ: `dlq-region`

### Notification Retry Chain

- Main: `notification-events`
- Retry: `retry-notification-5s` -> `retry-notification-1m` -> `retry-notification-10m`
- DLQ: `dlq-notification`

### DLQ Event Payload Contract

```json
{
  "payload": "{...original payload...}",
  "exceptionMessage": "error text",
  "stacktrace": "stack trace",
  "retryCount": 3,
  "timestamp": "2026-05-20T10:15:30Z",
  "correlationId": "uuid",
  "eventId": "uuid",
  "region": "INDIA",
  "serviceName": "processing-service",
  "sourceTopic": "raw-events"
}
```

## 5) Health Validation Checklist

Use this checklist before running `IntegrationLoadTest`:

1. `docker compose ps` shows `mysql`, `mongodb`, `kafka`, `zookeeper`, `kafka-ui` up.
2. Ingestion service is running on port `10001`.
3. `regional-service`, `processing-service`, and `notification-service` start without datasource errors.
4. Run `IntegrationLoadTest` and validate:
   - high `Success Count`
   - non-zero `Duplicate Count` (expected from duplicate simulation)
   - low `Failure Count`
   - low `Timeout Count`
