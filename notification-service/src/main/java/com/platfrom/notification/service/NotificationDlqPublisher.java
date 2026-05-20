package com.platfrom.notification.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.notification.model.DlqEvent;
import com.platfrom.notification.model.NotificationEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;

@Service
public class NotificationDlqPublisher {

    @Value("${platform.kafka.topics.dlq-notification}")
    private String dlqTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    public NotificationDlqPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }
    
    public void publish(ConsumerRecord<String, String> record, NotificationEvent event, Exception exception) {
        DlqEvent dlqEvent = DlqEvent.builder()
                .payload(record.value())
                .exceptionMessage(exception.getMessage())
                .stacktrace(stacktrace(exception))
                .retryCount(event == null || event.getRetryCount() == null ? 0 : event.getRetryCount())
                .timestamp(Instant.now())
                .correlationId(event == null ? "unknown" : event.getCorrelationId())
                .eventId(event == null ? "unknown" : event.getEventId())
                .region(event == null ? "unknown" : event.getRegion())
                .serviceName("notification-service")
                .sourceTopic(record.topic())
                .build();
        kafkaTemplate.send(dlqTopic, record.key(), toJson(dlqEvent));
    }

    private String toJson(DlqEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize notification DLQ event", exception);
        }
    }

    private String stacktrace(Exception exception) {
        StringWriter writer = new StringWriter();
        exception.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }
}
