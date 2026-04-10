package com.platfrom.processing.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

import java.io.Serializable;

@Entity
@Data
public class ProcessedEvent implements Serializable {
    
    @Id
    private String requestId;
    
    private String eventType;
    private String status;
}
