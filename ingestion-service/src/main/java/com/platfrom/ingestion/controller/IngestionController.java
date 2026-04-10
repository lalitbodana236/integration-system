package com.platfrom.ingestion.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platfrom.ingestion.model.BaseEvent;
import com.platfrom.ingestion.service.KafkaProducerService;
import com.platfrom.ingestion.service.StorageService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class IngestionController {
    
    private static final Logger log = LoggerFactory.getLogger(IngestionController.class);
    
    private final KafkaProducerService producer;
    private final StorageService storageService;
    
    private final ObjectMapper mapper = new ObjectMapper(); // reuse
    
    @Value("${topics.inventory}")
    private String inventoryTopic;
    
    @Value("${topics.po}")
    private String poTopic;
    
    @Value("${topics.so}")
    private String soTopic;
    
    @Value("${topics.media}")
    private String mediaTopic;
    
    @Value("${topics.checklist}")
    private String checklistTopic;
    
    @Value("${topics.location}")
    private String locationTopic;
    
    // ================= COMMON EVENT BUILDER =================
    private BaseEvent build(String type, String id, String payload) {
        return new BaseEvent(id, type, payload, System.currentTimeMillis());
    }
    
    // ================= COMMON SEND METHOD =================
    private void sendEvent(String topic, String type, String id, String payload) {
        try {
            BaseEvent event = build(type, id, payload);
            String json = mapper.writeValueAsString(event);
            
            producer.send(topic, id, json);
            
            log.info("Event sent | type={} id={} topic={}", type, id, topic);
            
        } catch (Exception e) {
            log.error("Failed to send event | type={} id={}", type, id, e);
            throw new RuntimeException("Kafka publish failed", e);
        }
    }
    
    // ================= INVENTORY =================
    @PostMapping("/inventory")
    public String inventory(@RequestParam String id, @RequestBody String payload) {
        
        log.info("Received INVENTORY request id={}", id);
        
        sendEvent(inventoryTopic, "INVENTORY", id, payload);
        
        return "Accepted INVENTORY: " + id;
    }
    
    // ================= PO =================
    @PostMapping("/po")
    public String po(@RequestParam String id, @RequestBody String payload) {
        
        log.info("Received PO request id={}", id);
        
        sendEvent(poTopic, "PO", id, payload);
        
        return "Accepted PO: " + id;
    }
    
    // ================= SO =================
    @PostMapping("/so")
    public String so(@RequestParam String id, @RequestBody String payload) {
        
        log.info("Received SO request id={}", id);
        
        sendEvent(soTopic, "SO", id, payload);
        
        return "Accepted SO: " + id;
    }
    
    // ================= CHECKLIST =================
    @PostMapping("/checklist")
    public String checklist(@RequestParam String id, @RequestBody String payload) {
        
        log.info("Received CHECKLIST request id={}", id);
        
        sendEvent(checklistTopic, "CHECKLIST", id, payload);
        
        return "Accepted CHECKLIST: " + id;
    }
    
    // ================= LOCATION =================
    @PostMapping("/location")
    public String location(@RequestParam String id, @RequestBody String payload) {
        
        log.info("Received LOCATION request id={}", id);
        
        sendEvent(locationTopic, "LOCATION", id, payload);
        
        return "Accepted LOCATION: " + id;
    }
    
    // ================= MEDIA =================
    @PostMapping("/media")
    public String upload(
            @RequestParam("file") MultipartFile file,
            @RequestParam("id") String id) throws IOException {
        
        log.info("Uploading media for id={}", id);
        
        String fileUrl = storageService.upload(file);
        
        sendEvent(mediaTopic, "MEDIA", id, fileUrl);
        
        return "Uploaded and queued: " + id;
    }
}