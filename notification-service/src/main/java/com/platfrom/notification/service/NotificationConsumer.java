package com.platfrom.notification.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.notification.exception.RetryableNotificationException;
import com.platfrom.notification.model.NotificationEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class NotificationConsumer {

    private final NotificationProcessor notificationProcessor;
    private final NotificationRetryPublisher retryPublisher;
    private final NotificationDlqPublisher dlqPublisher;
    private final ObjectMapper objectMapper;
    
    public NotificationConsumer(NotificationProcessor notificationProcessor, NotificationRetryPublisher retryPublisher, NotificationDlqPublisher dlqPublisher, ObjectMapper objectMapper) {
        this.notificationProcessor = notificationProcessor;
        this.retryPublisher = retryPublisher;
        this.dlqPublisher = dlqPublisher;
        this.objectMapper = objectMapper;
    }
    
    @KafkaListener(
            topics = {
                    "${platform.kafka.topics.notification-events}",
                    "${platform.kafka.topics.retry-notification-5s}",
                    "${platform.kafka.topics.retry-notification-1m}",
                    "${platform.kafka.topics.retry-notification-10m}"
            },
            containerFactory = "notificationBatchKafkaListenerContainerFactory")
    public void consume(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        for (ConsumerRecord<String, String> record : records) {
            NotificationEvent event = null;
            try {
                event = objectMapper.readValue(record.value(), NotificationEvent.class);
                bindMdc(event);
                notificationProcessor.process(event);
            } catch (RetryableNotificationException exception) {
                retryPublisher.publish(record, event, exception);
            } catch (Exception exception) {
                dlqPublisher.publish(record, event, exception);
            } finally {
                MDC.clear();
            }
        }
        acknowledgment.acknowledge();
    }

    private void bindMdc(NotificationEvent event) {
        if (event == null) {
            return;
        }
        MDC.put("correlationId", event.getCorrelationId());
        MDC.put("eventId", event.getEventId());
        MDC.put("region", event.getRegion());
        MDC.put("retryCount", String.valueOf(event.getRetryCount()));
    }
}
