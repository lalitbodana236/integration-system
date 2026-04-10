package com.platfrom.processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.processing.model.BaseEvent;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.net.URL;

@Service
@RequiredArgsConstructor
public class KafkaConsumers {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumers.class);
    
    private final ProcessingService service;
    private final ObjectMapper mapper = new ObjectMapper(); // reuse
    
    // ================= COMMON HANDLER =================
    private void handle(String message, String topic) {
        try {
            BaseEvent event = mapper.readValue(message, BaseEvent.class);
            
            log.info("Consumed event | type={} id={} topic={}",
                    event.getEventType(), event.getRequestId(), topic);
            
            service.process(event);
            
            // Special handling for MEDIA
            if ("MEDIA".equalsIgnoreCase(event.getEventType())) {
                processMedia(event);
            }
            
        } catch (Exception e) {
            log.error("Failed to process message from topic={}", topic, e);
            
            // 🔥 Future: send to DLQ
        }
    }
    
    // ================= MEDIA PROCESSING =================
    private void processMedia(BaseEvent event) {
        try {
            String fileUrl = event.getPayload();
            
            log.info("Processing media file: {}", fileUrl);
            
            byte[] data = new URL(fileUrl).openStream().readAllBytes();
            
            // TODO: process file (image/video/doc)
            
        } catch (Exception e) {
            log.error("Media processing failed for id={}", event.getRequestId(), e);
        }
    }
    
    // ================= LISTENERS =================
    
    @KafkaListener(topics = "${topics.inventory}")
    public void inventory(String message) {
        handle(message, "inventory-topic");
    }
    
    @KafkaListener(topics = "${topics.po}")
    public void po(String message) {
        handle(message, "po-topic");
    }
    
    @KafkaListener(topics = "${topics.so}")
    public void so(String message) {
        handle(message, "so-topic");
    }
    
    @KafkaListener(topics = "${topics.media}")
    public void media(String message) {
        handle(message, "media-topic");
    }
    
    @KafkaListener(topics = "${topics.checklist}")
    public void checklist(String message) {
        handle(message, "checklist-topic");
    }
    
    @KafkaListener(topics = "${topics.location}")
    public void location(String message) {
        handle(message, "location-topic");
    }
}