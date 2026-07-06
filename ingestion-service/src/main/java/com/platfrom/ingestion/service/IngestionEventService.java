package com.platfrom.ingestion.service;

import com.platfrom.ingestion.model.ApiAckResponse;
import com.platfrom.ingestion.model.BaseEvent;
import com.platfrom.ingestion.support.CorrelationContext;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class IngestionEventService {

    private static final Logger log = LoggerFactory.getLogger(IngestionEventService.class);

    @Value("${platform.kafka.topics.raw-events}")
    private String rawEventsTopic;

    private final KafkaProducerService kafkaProducerService;
    private final IdempotencyService idempotencyService;

    public ApiAckResponse ingest(
            String eventType,
            String requestId,
            String payload,
            String customerId,
            String region,
            String eventIdOverride) {
        String correlationId = CorrelationContext.getOrCreate();
        String eventId = StringUtils.hasText(eventIdOverride) ? eventIdOverride : UUID.randomUUID().toString();
        String resolvedRegion = StringUtils.hasText(region) ? region.toLowerCase() : "global";
        String partitionKey = StringUtils.hasText(customerId) ? customerId : resolvedRegion;

        if (!idempotencyService.register(eventId)) {
            log.info(
                    "duplicate_ingestion_ignored serviceName=ingestion-service correlationId={} eventId={} region={} retryCount=0 requestId={}",
                    correlationId,
                    eventId,
                    resolvedRegion,
                    requestId);
            return new ApiAckResponse("DUPLICATE", "Duplicate request ignored", eventId, correlationId, rawEventsTopic);
        }

        BaseEvent event = BaseEvent.builder()
                .eventId(eventId)
                .correlationId(correlationId)
                .requestId(requestId)
                .customerId(customerId)
                .region(resolvedRegion)
                .eventType(eventType)
                .payload(payload)
                .sourceService("ingestion-service")
                .retryCount(0)
                .createdAt(Instant.now())
                .metadata(Map.of("ingestionMode", "async"))
                .build();

        kafkaProducerService.send(rawEventsTopic, partitionKey, event);
        log.info(
                "ingestion_accepted serviceName=ingestion-service correlationId={} eventId={} region={} retryCount=0 requestId={} eventType={}",
                correlationId,
                eventId,
                resolvedRegion,
                requestId,
                eventType);
        return new ApiAckResponse("ACCEPTED", "Event queued for asynchronous processing", eventId, correlationId, rawEventsTopic);
    }
}
