package com.platfrom.regional.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class NotificationEvent {
    private String eventId;
    private String correlationId;
    private String requestId;
    private String customerId;
    private String region;
    private String eventType;
    private String payload;
    private List<String> channels;
    private Instant createdAt;
    private Integer retryCount;
    private Map<String, String> metadata;
}
