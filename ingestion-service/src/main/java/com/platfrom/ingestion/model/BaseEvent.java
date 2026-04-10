package com.platfrom.ingestion.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BaseEvent implements Serializable {
    private String requestId;
    private String eventType;   // INVENTORY, PO, SO, MEDIA, ...
    private String payload;     // JSON string for simplicity
    private long timestamp;
}
