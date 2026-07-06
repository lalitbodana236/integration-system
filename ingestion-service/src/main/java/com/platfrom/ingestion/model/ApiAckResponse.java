package com.platfrom.ingestion.model;

public record ApiAckResponse(
        String status,
        String message,
        String eventId,
        String correlationId,
        String topic) {
}
