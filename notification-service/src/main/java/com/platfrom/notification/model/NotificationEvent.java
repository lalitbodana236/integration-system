package com.platfrom.notification.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class NotificationEvent implements Serializable {
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
