package com.platfrom.ingestion.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BaseEvent implements Serializable {
    private String eventId;
    private String correlationId;
    private String requestId;
    private String customerId;
    private String region;
    private String eventType;
    private String payload;
    private String sourceService;
    private Integer retryCount;
    private Instant createdAt;
    private Map<String, String> metadata;
}
