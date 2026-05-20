# Distributed Event-Driven Integration Platform: Deep Dive

This document is a complete technical walkthrough of the project so you can answer architecture and implementation questions with confidence.

## 1. What This Project Is

A multi-service, event-driven integration platform built with:

- Java + Spring Boot
- Apache Kafka
- Redis-style idempotency pattern
- MySQL (workflow/transactional state)
- MongoDB/NoSQL (high-volume archival and history intent)

Services:

1. `ingestion-service` (HTTP entrypoint, idempotent event producer)
2. `processing-service` (core business transform + routing)
3. `regional-service` (region-specific workflow execution)
4. `notification-service` (channel delivery orchestration)

## 2. End-to-End Flow

1. Client calls ingestion APIs (`/api/po`, `/api/so`, `/api/inventory`, `/api/checklist`, `/api/location`, `/api/media`)
2. Ingestion validates + enriches request (`eventId`, `correlationId`) and publishes to Kafka `raw-events`
3. Processing consumes in batch, applies business validations/transforms, persists state, and routes to region topics:
   - `region-india`
   - `region-us`
   - `region-uk`
4. Regional consumers execute region workflow and publish `notification-events`
5. Notification service consumes and fans out delivery to `EMAIL`, `SMS`, `WEBHOOK`
6. Retries are topic-based and exhausted messages go to DLQ topics

## 3. Why Event-Driven (and Why Kafka)

Why event-driven:

- Decouples producer and consumers
- Supports independent scaling by stage
- Enables async workflows and backpressure tolerance
- Improves resilience: transient downstream failure doesn’t immediately fail upstream API

Why Kafka:

- High-throughput append log
- Partitioned ordering
- Consumer groups for horizontal scaling
- Retention + replay capability for debugging/reprocessing

Trade-offs:

- Added operational complexity (brokers, topic lifecycle)
- Eventual consistency (not synchronous transaction boundaries across services)
- Requires careful schema/versioning and idempotency strategy

## 4. Service-by-Service Design

## 4.1 Ingestion Service

Key files:

- [IngestionController.java](C:/Users/lalit/checkouts/integration-system/ingestion-service/src/main/java/com/platfrom/ingestion/controller/IngestionController.java)
- [IngestionEventService.java](C:/Users/lalit/checkouts/integration-system/ingestion-service/src/main/java/com/platfrom/ingestion/service/IngestionEventService.java)
- [KafkaProducerService.java](C:/Users/lalit/checkouts/integration-system/ingestion-service/src/main/java/com/platfrom/ingestion/service/KafkaProducerService.java)
- [CorrelationIdFilter.java](C:/Users/lalit/checkouts/integration-system/ingestion-service/src/main/java/com/platfrom/ingestion/config/CorrelationIdFilter.java)
- [RedisIdempotencyService.java](C:/Users/lalit/checkouts/integration-system/ingestion-service/src/main/java/com/platfrom/ingestion/service/RedisIdempotencyService.java)
- [BaseEvent.java](C:/Users/lalit/checkouts/integration-system/ingestion-service/src/main/java/com/platfrom/ingestion/model/BaseEvent.java)

Responsibilities:

- Receive API requests
- Build canonical event envelope
- Enforce idempotency guard
- Publish asynchronously to `raw-events`
- Return fast ACK (`202`)

Important variables and why:

- `rawEventsTopic`: decouples topic naming from code
- `eventId`: dedupe key across retries/duplicates
- `correlationId`: traceability across distributed hops
- `partitionKey`: `customerId` first, region fallback to preserve ordering context

Why this idempotency approach:

- Current implementation uses in-memory `ConcurrentHashMap` with key format `event:{eventId}` as a Redis-like pattern.
- Real production target is Redis `SETNX` + TTL.

Trade-off:

- In-memory dedupe is process-local and non-durable.
- Redis adds infrastructure but provides cross-instance dedupe and restart safety.

## 4.2 Processing Service

Key files:

- [KafkaConsumers.java](C:/Users/lalit/checkouts/integration-system/processing-service/src/main/java/com/platfrom/processing/service/KafkaConsumers.java)
- [ProcessingService.java](C:/Users/lalit/checkouts/integration-system/processing-service/src/main/java/com/platfrom/processing/service/ProcessingService.java)
- [RetryPublisher.java](C:/Users/lalit/checkouts/integration-system/processing-service/src/main/java/com/platfrom/processing/service/RetryPublisher.java)
- [DlqPublisher.java](C:/Users/lalit/checkouts/integration-system/processing-service/src/main/java/com/platfrom/processing/service/DlqPublisher.java)
- [RegionEventPublisher.java](C:/Users/lalit/checkouts/integration-system/processing-service/src/main/java/com/platfrom/processing/service/RegionEventPublisher.java)
- [ProcessedEvent.java](C:/Users/lalit/checkouts/integration-system/processing-service/src/main/java/com/platfrom/processing/model/ProcessedEvent.java)
- [EventArchiveService.java](C:/Users/lalit/checkouts/integration-system/processing-service/src/main/java/com/platfrom/processing/service/EventArchiveService.java)

Responsibilities:

- Batch consume raw + retry topics
- Apply validation and normalization
- Persist workflow state
- Archive event history
- Route by region
- Retry and DLQ on failure

Why batch listener + manual ack:

- Better throughput (fewer broker round trips)
- Explicit control over commit timing
- Good for high-volume burst behavior

Trade-off:

- More complex error handling than auto-commit
- Need careful per-record handling inside batch

Idempotency model here:

- Checks both `eventId` and `requestId` before processing
- Prevents duplicate writes/replays

Trade-off:

- Extra read checks increase DB load
- Strongly reduces duplicate side effects

NoSQL archival note:

- `EventArchiveService` currently uses in-memory map as an adapter seam.
- Intended production shape is MongoDB collection for append-heavy history/replay.

## 4.3 Regional Service

Key files:

- [RegionalKafkaConsumers.java](C:/Users/lalit/checkouts/integration-system/regional-service/src/main/java/com/platfrom/regional/service/RegionalKafkaConsumers.java)
- [WorkflowProcessor.java](C:/Users/lalit/checkouts/integration-system/regional-service/src/main/java/com/platfrom/regional/service/WorkflowProcessor.java)
- [RegionalRetryPublisher.java](C:/Users/lalit/checkouts/integration-system/regional-service/src/main/java/com/platfrom/regional/service/RegionalRetryPublisher.java)
- [RegionalDlqPublisher.java](C:/Users/lalit/checkouts/integration-system/regional-service/src/main/java/com/platfrom/regional/service/RegionalDlqPublisher.java)
- [NotificationEventPublisher.java](C:/Users/lalit/checkouts/integration-system/regional-service/src/main/java/com/platfrom/regional/service/NotificationEventPublisher.java)

Responsibilities:

- Consume region topics in batch
- Perform region business flow
- Persist regional workflow
- Forward to `notification-events`

Why separate regional stage:

- Isolates region-specific logic and scaling
- Limits blast radius of one region issue
- Enables per-region retry/DLQ insights

## 4.4 Notification Service

Key files:

- [NotificationConsumer.java](C:/Users/lalit/checkouts/integration-system/notification-service/src/main/java/com/platfrom/notification/service/NotificationConsumer.java)
- [NotificationProcessor.java](C:/Users/lalit/checkouts/integration-system/notification-service/src/main/java/com/platfrom/notification/service/NotificationProcessor.java)
- [NotificationRetryPublisher.java](C:/Users/lalit/checkouts/integration-system/notification-service/src/main/java/com/platfrom/notification/service/NotificationRetryPublisher.java)
- [NotificationDlqPublisher.java](C:/Users/lalit/checkouts/integration-system/notification-service/src/main/java/com/platfrom/notification/service/NotificationDlqPublisher.java)
- [EmailSender.java](C:/Users/lalit/checkouts/integration-system/notification-service/src/main/java/com/platfrom/notification/service/EmailSender.java)
- [SmsSender.java](C:/Users/lalit/checkouts/integration-system/notification-service/src/main/java/com/platfrom/notification/service/SmsSender.java)
- [WebhookSender.java](C:/Users/lalit/checkouts/integration-system/notification-service/src/main/java/com/platfrom/notification/service/WebhookSender.java)

Responsibilities:

- Batch consume notification topic and retry topics
- Channel fan-out using `NotificationSender` strategy abstraction
- Idempotent channel-level delivery dedupe (`eventId + channel`)
- Retry and DLQ for failures

Why strategy interface for senders:

- Open/closed principle: add a new channel without changing processor core
- Testability: mock sender per channel

Trade-off:

- Slightly more abstraction overhead than direct inline switch

## 5. Kafka Topic and Retry Design

Main topics:

- `raw-events`
- `region-india`, `region-us`, `region-uk`
- `notification-events`

Retry topics:

- Processing: `retry-processing-5s`, `retry-processing-1m`, `retry-processing-10m`
- Regional: `retry-region-5s`, `retry-region-1m`, `retry-region-10m`
- Notification: `retry-notification-5s`, `retry-notification-1m`, `retry-notification-10m`

DLQ topics:

- `dlq-processing`
- `dlq-region`
- `dlq-notification`

Why staged retry topics:

- Avoids tight immediate retry loops
- Smooths pressure on unstable downstream systems
- Gives predictable retry windows

Trade-off:

- More topics and operational overhead
- Requires clear observability and retention management

## 6. Idempotency: What, How, Why

Where implemented:

- Ingestion dedupe by `eventId`
- Processing dedupe by `eventId` / `requestId`
- Regional dedupe by `eventId` / `requestId`
- Notification dedupe by `eventId + channel`

Why this layered model:

- Each stage protects its own side effects
- Replay or duplicate publish in one stage won’t double-apply in the next

Alternative:

- Exactly-once semantics end-to-end

Why not chosen:

- Cross-service EOS is complex and fragile operationally
- Idempotent consumer + at-least-once delivery is simpler and battle-tested

## 7. Data Model Rationale

MySQL usage:

- Workflow state
- Transactional entities
- Delivery records
- Audit and indexing

NoSQL usage intent:

- Raw payload archival
- High write throughput history
- Replay tooling support

Why polyglot persistence:

- Relational DB for state transitions and constraints
- Document DB for flexible payload storage and analytics-friendly history

Trade-off:

- More operational surface area
- Need consistency and governance between stores

## 8. Observability and Logging

Patterns used:

- Structured log lines with:
  - `correlationId`
  - `eventId`
  - `region`
  - `retryCount`
  - `serviceName`
- Periodic metrics snapshots via scheduled services
- Correlation ID propagated from ingress filter to event headers

Why:

- Fast root-cause tracing across asynchronous service boundaries
- Practical SRE metrics: throughput, retry counts, failures, duplicates

## 9. Why Specific Variables and Methods Exist

## 9.1 `IntegrationLoadTest.java`

File: [IntegrationLoadTest.java](C:/Users/lalit/checkouts/integration-system/IntegrationLoadTest.java)

Key variables:

- `BASE_URL`: central host for all ingestion APIs
- `THREAD_POOL_SIZE=200`: concurrency level to stress async ingestion
- `REQUESTS_PER_API=1000`: balanced load sample per endpoint
- `APIS`: list of endpoint suffixes to test uniformity
- `CLIENT`: shared `HttpClient` (connection reuse, lower overhead)
- `SUCCESS/FAILURE/DUPLICATE/TIMEOUT`: independent counters for result classification
- `TOTAL_LATENCY`: aggregate latency for average computation

Key methods:

- `main`: orchestration, executor lifecycle, latch synchronization, summary
- `sendRequest`: request build, headers, timing, classification, retry simulation
- `resolveRegion`: synthetic traffic split (70% INDIA, 20% US, 10% UK)
- `buildPayload`: deterministic JSON test payload builder
- `printSummary`: throughput and latency reporting

Why duplicate simulation:

- Every 10th request gets deterministic duplicate `eventId` to validate dedupe path

Why retry simulation:

- Every 20th request re-sends same request to emulate client retry behavior

## 10. Alternatives Considered and Trade-offs

1. Synchronous chain with REST-only service-to-service calls
- Simpler initial coding
- Poor resilience and throughput under spikes

2. Kafka Streams / Flink for transformations
- Strong streaming tooling
- Higher complexity for current scope

3. Single retry topic with delayed consumer logic
- Fewer topics
- Harder visibility and less deterministic delay behavior

4. Database-only dedupe without cache
- Strong durability
- Higher latency/hotspot risk at ingress QPS

## 11. Current Limitations and Improvement Path

Current practical limitations:

- Ingestion idempotency service is in-memory, not Redis-backed yet
- Processing archival currently in-memory adapter, not persistent Mongo collection
- Delay semantics are topic-stage based, not true broker-level delayed delivery
- No automated contract tests yet for all API edge cases

Recommended next improvements:

1. Replace in-memory idempotency with Redis `SETNX` + TTL + namespace versioning
2. Implement persistent Mongo archive repository with indexes on `eventId`, `createdAt`, `region`
3. Add OpenAPI spec and request schema validation for ingestion payloads
4. Add integration tests that validate retry->DLQ progression
5. Add consumer lag metrics export to Prometheus/Grafana

## 12. Interview/Discussion Cheat Sheet

If asked “why event-driven?”:

- Decoupling, async scalability, resilience, replayability.

If asked “how do you prevent duplicates?”:

- Layered idempotency keys by stage (`eventId`, `requestId`, `eventId+channel`) plus dedupe checks before side effects.

If asked “why retries via topics?”:

- Backoff without blocking consumers, operationally visible progression, controlled failure escalation to DLQ.

If asked “what does correlationId do?”:

- End-to-end tracing across HTTP request, Kafka headers, logs, and downstream services.

If asked “what are trade-offs?”:

- Operational complexity and eventual consistency are accepted in exchange for throughput, resilience, and independent scaling.

---

For endpoint-level request/response contracts, see:

- [API_CONTRACTS.md](C:/Users/lalit/checkouts/integration-system/API_CONTRACTS.md)
