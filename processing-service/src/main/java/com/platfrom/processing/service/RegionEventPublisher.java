package com.platfrom.processing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.processing.model.BaseEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service

public class RegionEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(RegionEventPublisher.class);

    @Value("${platform.kafka.topics.region-india}")
    private String indiaTopic;

    @Value("${platform.kafka.topics.region-us}")
    private String usTopic;

    @Value("${platform.kafka.topics.region-uk}")
    private String ukTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    public RegionEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }
    
    public void publish(BaseEvent sourceEvent, String normalizedPayload) {
        BaseEvent regionalEvent = BaseEvent.builder()
                .eventId(sourceEvent.getEventId())
                .correlationId(sourceEvent.getCorrelationId())
                .requestId(sourceEvent.getRequestId())
                .customerId(sourceEvent.getCustomerId())
                .region(sourceEvent.getRegion())
                .eventType(sourceEvent.getEventType())
                .payload(normalizedPayload)
                .sourceService("processing-service")
                .retryCount(sourceEvent.getRetryCount())
                .createdAt(sourceEvent.getCreatedAt())
                .metadata(sourceEvent.getMetadata())
                .build();

        String topic = resolveTopic(sourceEvent.getRegion());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, resolveKey(sourceEvent), toJson(regionalEvent));
        record.headers().add(new RecordHeader("correlationId", sourceEvent.getCorrelationId().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("eventId", sourceEvent.getEventId().getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record);

        log.info(
                "region_event_published serviceName=processing-service correlationId={} eventId={} region={} retryCount={} topic={}",
                sourceEvent.getCorrelationId(),
                sourceEvent.getEventId(),
                sourceEvent.getRegion(),
                sourceEvent.getRetryCount(),
                topic);
    }

    private String resolveTopic(String region) {
        return switch (region.toLowerCase()) {
            case "india", "in" -> indiaTopic;
            case "us", "usa" -> usTopic;
            case "uk", "gb" -> ukTopic;
            default -> throw new IllegalArgumentException("Unsupported region: " + region);
        };
    }

    private String resolveKey(BaseEvent event) {
        if (event.getCustomerId() != null && !event.getCustomerId().isBlank()) {
            return event.getCustomerId();
        }
        return event.getRegion();
    }

    private String toJson(BaseEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize regional event", exception);
        }
    }
}
