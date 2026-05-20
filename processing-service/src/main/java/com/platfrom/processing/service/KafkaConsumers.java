package com.platfrom.processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.processing.exception.RetryableProcessingException;
import com.platfrom.processing.model.BaseEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

@Service

public class KafkaConsumers {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumers.class);

    private final ProcessingService processingService;
    private final RetryPublisher retryPublisher;
    private final DlqPublisher dlqPublisher;
    private final ProcessingMetricsService metricsService;
    private final ObjectMapper objectMapper;
    
    
    public KafkaConsumers(ProcessingService processingService, RetryPublisher retryPublisher, DlqPublisher dlqPublisher, ProcessingMetricsService metricsService, ObjectMapper objectMapper) {
        this.processingService = processingService;
        this.retryPublisher = retryPublisher;
        this.dlqPublisher = dlqPublisher;
        this.metricsService = metricsService;
        this.objectMapper = objectMapper;
    }
    
    @KafkaListener(
            topics = {
                    "${platform.kafka.topics.raw-events}",
                    "${platform.kafka.topics.retry-processing-5s}",
                    "${platform.kafka.topics.retry-processing-1m}",
                    "${platform.kafka.topics.retry-processing-10m}"
            },
            containerFactory = "batchKafkaListenerContainerFactory")
    public void consume(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        for (ConsumerRecord<String, String> record : records) {
            BaseEvent event = null;
            try {
                event = objectMapper.readValue(record.value(), BaseEvent.class);
                enrichLoggingContext(event);
                processingService.process(event);
                metricsService.incrementProcessed();
            } catch (RetryableProcessingException exception) {
                metricsService.incrementRetry();
                retryPublisher.publish(record, event, exception);
            } catch (Exception exception) {
                metricsService.incrementFailure();
                dlqPublisher.publish(record, event, exception);
                log.error("processing_record_failed topic={} partition={} offset={}", record.topic(), record.partition(), record.offset(), exception);
            } finally {
                clearLoggingContext();
            }
        }
        acknowledgment.acknowledge();
    }

    private void enrichLoggingContext(BaseEvent event) {
        if (event == null) {
            return;
        }
        MDC.put("correlationId", event.getCorrelationId());
        MDC.put("eventId", event.getEventId());
        MDC.put("region", event.getRegion());
        MDC.put("retryCount", String.valueOf(event.getRetryCount()));
    }

    private void clearLoggingContext() {
        MDC.remove("correlationId");
        MDC.remove("eventId");
        MDC.remove("region");
        MDC.remove("retryCount");
    }
}
