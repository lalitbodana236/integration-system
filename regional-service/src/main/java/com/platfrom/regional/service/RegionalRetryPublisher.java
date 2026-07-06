package com.platfrom.regional.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.regional.model.BaseEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class RegionalRetryPublisher {

    @Value("${platform.kafka.topics.retry-region-5s}")
    private String retry5sTopic;

    @Value("${platform.kafka.topics.retry-region-1m}")
    private String retry1mTopic;

    @Value("${platform.kafka.topics.retry-region-10m}")
    private String retry10mTopic;

    private final RegionalDlqPublisher dlqPublisher;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    public RegionalRetryPublisher(RegionalDlqPublisher dlqPublisher, KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.dlqPublisher = dlqPublisher;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }
    
    public void publish(ConsumerRecord<String, String> record, BaseEvent event, Exception exception) {
        if (event == null) {
            dlqPublisher.publish(record, null, exception);
            return;
        }

        int retryCount = event.getRetryCount() == null ? 1 : event.getRetryCount() + 1;
        if (retryCount > 3) {
            dlqPublisher.publish(record, event, exception);
            return;
        }

        event.setRetryCount(retryCount);
        String topic = retryCount == 1 ? retry5sTopic : retryCount == 2 ? retry1mTopic : retry10mTopic;
        kafkaTemplate.send(topic, record.key(), toJson(event));
    }

    private String toJson(BaseEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize retry event", exception);
        }
    }
}
