package com.platfrom.regional.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.regional.model.BaseEvent;
import com.platfrom.regional.model.NotificationEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Service
public class NotificationEventPublisher {

    @Value("${platform.kafka.topics.notification-events}")
    private String notificationTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    public NotificationEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }
    
    public void publish(BaseEvent event) {
        NotificationEvent notificationEvent = NotificationEvent.builder()
                .eventId(event.getEventId())
                .correlationId(event.getCorrelationId())
                .requestId(event.getRequestId())
                .customerId(event.getCustomerId())
                .region(event.getRegion())
                .eventType(event.getEventType())
                .payload(event.getPayload())
                .channels(List.of("EMAIL", "SMS", "WEBHOOK"))
                .createdAt(Instant.now())
                .retryCount(event.getRetryCount())
                .metadata(Map.of("sourceWorkflow", "regional-service"))
                .build();
        kafkaTemplate.send(notificationTopic, event.getCustomerId() == null ? event.getRegion() : event.getCustomerId(), toJson(notificationEvent));
    }

    private String toJson(NotificationEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize notification event", exception);
        }
    }
}
