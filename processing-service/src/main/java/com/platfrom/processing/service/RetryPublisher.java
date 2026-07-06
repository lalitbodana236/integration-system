package com.platfrom.processing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.processing.model.BaseEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class RetryPublisher {

    @Value("${platform.kafka.topics.retry-processing-5s}")
    private String retry5sTopic;

    @Value("${platform.kafka.topics.retry-processing-1m}")
    private String retry1mTopic;

    @Value("${platform.kafka.topics.retry-processing-10m}")
    private String retry10mTopic;

    private final DlqPublisher dlqPublisher;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    public RetryPublisher(DlqPublisher dlqPublisher, KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.dlqPublisher = dlqPublisher;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }
    
    public void publish(ConsumerRecord<String, String> record, BaseEvent event, Exception exception) {
        BaseEvent retryEvent = event == null ? null : BaseEvent.builder()
                .eventId(event.getEventId())
                .correlationId(event.getCorrelationId())
                .requestId(event.getRequestId())
                .customerId(event.getCustomerId())
                .region(event.getRegion())
                .eventType(event.getEventType())
                .payload(event.getPayload())
                .sourceService(event.getSourceService())
                .retryCount(event.getRetryCount() == null ? 1 : event.getRetryCount() + 1)
                .createdAt(event.getCreatedAt())
                .metadata(event.getMetadata())
                .build();

        if (retryEvent == null || retryEvent.getRetryCount() > 3) {
            dlqPublisher.publish(record, event, exception);
            return;
        }

        String topic = switch (retryEvent.getRetryCount()) {
            case 1 -> retry5sTopic;
            case 2 -> retry1mTopic;
            default -> retry10mTopic;
        };

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, record.key(), toJson(retryEvent));
        producerRecord.headers().add(new RecordHeader("retryCount", String.valueOf(retryEvent.getRetryCount()).getBytes(StandardCharsets.UTF_8)));
        producerRecord.headers().add(new RecordHeader("originalTopic", record.topic().getBytes(StandardCharsets.UTF_8)));
        producerRecord.headers().add(new RecordHeader("exceptionMessage", exception.getMessage().getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(producerRecord);
    }

    private String toJson(BaseEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize retry event", exception);
        }
    }
}
