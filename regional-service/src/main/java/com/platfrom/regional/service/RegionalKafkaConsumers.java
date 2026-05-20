package com.platfrom.regional.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.regional.exception.RetryableRegionalException;
import com.platfrom.regional.model.BaseEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class RegionalKafkaConsumers {

    private final WorkflowProcessor workflowProcessor;
    private final RegionalRetryPublisher retryPublisher;
    private final RegionalDlqPublisher dlqPublisher;
    private final ObjectMapper objectMapper;
    
    public RegionalKafkaConsumers(WorkflowProcessor workflowProcessor, RegionalRetryPublisher retryPublisher, RegionalDlqPublisher dlqPublisher, ObjectMapper objectMapper) {
        this.workflowProcessor = workflowProcessor;
        this.retryPublisher = retryPublisher;
        this.dlqPublisher = dlqPublisher;
        this.objectMapper = objectMapper;
    }
    
    @KafkaListener(
            topics = {
                    "${platform.kafka.topics.region-india}",
                    "${platform.kafka.topics.region-us}",
                    "${platform.kafka.topics.region-uk}",
                    "${platform.kafka.topics.retry-region-5s}",
                    "${platform.kafka.topics.retry-region-1m}",
                    "${platform.kafka.topics.retry-region-10m}"
            },
            containerFactory = "regionalBatchKafkaListenerContainerFactory")
    public void consume(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        for (ConsumerRecord<String, String> record : records) {
            BaseEvent event = null;
            try {
                event = objectMapper.readValue(record.value(), BaseEvent.class);
                bindMdc(event);
                workflowProcessor.process(event);
            } catch (RetryableRegionalException exception) {
                retryPublisher.publish(record, event, exception);
            } catch (Exception exception) {
                dlqPublisher.publish(record, event, exception);
            } finally {
                clearMdc();
            }
        }
        acknowledgment.acknowledge();
    }

    private void bindMdc(BaseEvent event) {
        if (event == null) {
            return;
        }
        MDC.put("correlationId", event.getCorrelationId());
        MDC.put("eventId", event.getEventId());
        MDC.put("region", event.getRegion());
        MDC.put("retryCount", String.valueOf(event.getRetryCount()));
    }

    private void clearMdc() {
        MDC.clear();
    }
}
