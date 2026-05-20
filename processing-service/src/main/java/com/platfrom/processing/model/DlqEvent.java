package com.platfrom.processing.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class DlqEvent {
    private String payload;
    private String exceptionMessage;
    private String stacktrace;
    private int retryCount;
    private Instant timestamp;
    private String correlationId;
    private String eventId;
    private String region;
    private String serviceName;
    private String sourceTopic;
}
