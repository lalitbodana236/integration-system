package com.platfrom.processing.service;

import com.platfrom.processing.exception.RetryableProcessingException;
import com.platfrom.processing.model.BaseEvent;
import com.platfrom.processing.model.ProcessedEvent;
import com.platfrom.processing.repository.ProcessedEventRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;

@Service

public class ProcessingService {

    private static final Logger log = LoggerFactory.getLogger(ProcessingService.class);

    private final ProcessedEventRepository processedEventRepository;
    private final EventArchiveService eventArchiveService;
    private final RegionEventPublisher regionEventPublisher;
    private final ProcessingMetricsService metricsService;
    
    public ProcessingService(ProcessedEventRepository processedEventRepository, EventArchiveService eventArchiveService, RegionEventPublisher regionEventPublisher, ProcessingMetricsService metricsService) {
        this.processedEventRepository = processedEventRepository;
        this.eventArchiveService = eventArchiveService;
        this.regionEventPublisher = regionEventPublisher;
        this.metricsService = metricsService;
    }
    
    @Transactional
    public void process(BaseEvent event) {
        long startedAt = System.currentTimeMillis();
        if (processedEventRepository.existsById(event.getEventId()) || processedEventRepository.existsByRequestId(event.getRequestId())) {
            log.info(
                    "duplicate_processing_skipped serviceName=processing-service correlationId={} eventId={} region={} retryCount={} requestId={}",
                    event.getCorrelationId(),
                    event.getEventId(),
                    event.getRegion(),
                    event.getRetryCount(),
                    event.getRequestId());
            metricsService.incrementDuplicate();
            return;
        }

        validate(event);
        String normalizedPayload = normalize(event.getPayload());

        eventArchiveService.archiveIncoming(event, normalizedPayload);

        ProcessedEvent processedEvent = new ProcessedEvent();
        processedEvent.setEventId(event.getEventId());
        processedEvent.setRequestId(event.getRequestId());
        processedEvent.setCustomerId(event.getCustomerId());
        processedEvent.setRegion(event.getRegion());
        processedEvent.setEventType(event.getEventType());
        processedEvent.setStatus("SUCCESS");
        processedEvent.setWorkflowState("REGION_DISPATCHED");
        processedEvent.setNormalizedPayload(normalizedPayload);
        processedEvent.setCreatedAt(Instant.now());
        processedEvent.setUpdatedAt(Instant.now());

        processedEventRepository.save(processedEvent);
        regionEventPublisher.publish(event, normalizedPayload);
        metricsService.recordProcessingLatency(System.currentTimeMillis() - startedAt);

        log.info(
                "processing_completed serviceName=processing-service correlationId={} eventId={} region={} retryCount={} requestId={} eventType={}",
                event.getCorrelationId(),
                event.getEventId(),
                event.getRegion(),
                event.getRetryCount(),
                event.getRequestId(),
                event.getEventType());
    }

    private void validate(BaseEvent event) {
        if (!StringUtils.hasText(event.getEventId())) {
            throw new IllegalArgumentException("eventId is required");
        }
        if (!StringUtils.hasText(event.getRequestId())) {
            throw new IllegalArgumentException("requestId is required");
        }
        if (!StringUtils.hasText(event.getPayload())) {
            throw new IllegalArgumentException("payload is required");
        }
        if (!StringUtils.hasText(event.getRegion())) {
            throw new RetryableProcessingException("region routing is required");
        }
    }

    private String normalize(String payload) {
        return payload.replaceAll("\\s+", " ").trim();
    }
}
