package com.platfrom.processing.service;

import com.platfrom.processing.model.BaseEvent;
import com.platfrom.processing.model.ProcessedEvent;
import com.platfrom.processing.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProcessingService {
    
    private final ProcessedEventRepository repo;
    
    public void process(BaseEvent event) {
        
        if (repo.existsById(event.getRequestId())) {
            System.out.println("Duplicate skipped: " + event.getRequestId());
            return;
        }
        
        try {
            // Simulate business logic
            System.out.println("Processing " + event.getEventType() + " : " + event.getPayload());
            
            ProcessedEvent entity = new ProcessedEvent();
            entity.setRequestId(event.getRequestId());
            entity.setEventType(event.getEventType());
            entity.setStatus("SUCCESS");
            
            repo.save(entity);
            
        } catch (Exception e) {
            throw new RuntimeException("Processing failed");
        }
    }
}