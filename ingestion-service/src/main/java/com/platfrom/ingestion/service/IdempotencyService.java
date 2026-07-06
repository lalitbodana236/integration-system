package com.platfrom.ingestion.service;

public interface IdempotencyService {
    boolean register(String eventId);
}
