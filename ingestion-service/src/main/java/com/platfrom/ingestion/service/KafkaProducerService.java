package com.platfrom.ingestion.service;

import com.platfrom.ingestion.model.BaseEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    public void send(String topic, String requestId,String event) {
        kafkaTemplate.send(topic, requestId, event);
    }
}
