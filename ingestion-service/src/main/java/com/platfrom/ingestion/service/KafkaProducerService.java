package com.platfrom.ingestion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.ingestion.model.BaseEvent;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }
    
    public void send(String topic, String partitionKey, BaseEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionKey, toJson(event));
        addHeader(record, "correlationId", event.getCorrelationId());
        addHeader(record, "eventId", event.getEventId());
        addHeader(record, "region", event.getRegion());
        addHeader(record, "retryCount", String.valueOf(event.getRetryCount()));
        addHeader(record, "serviceName", event.getSourceService());

        kafkaTemplate.send(record).whenComplete((result, exception) -> {
            if (exception != null) {
                log.error(
                        "event_publish_failed serviceName=ingestion-service correlationId={} eventId={} region={} retryCount={} topic={}",
                        event.getCorrelationId(),
                        event.getEventId(),
                        event.getRegion(),
                        event.getRetryCount(),
                        topic,
                        exception);
                return;
            }

            log.info(
                    "event_published serviceName=ingestion-service correlationId={} eventId={} region={} retryCount={} topic={} partition={} offset={}",
                    event.getCorrelationId(),
                    event.getEventId(),
                    event.getRegion(),
                    event.getRetryCount(),
                    topic,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        });
    }

    private void addHeader(ProducerRecord<String, String> record, String key, String value) {
        if (value != null) {
            record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    private String toJson(BaseEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize event", exception);
        }
    }
}
