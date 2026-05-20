package com.platfrom.ingestion.service;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class RedisIdempotencyService implements IdempotencyService {

    private final Map<String, Instant> eventRegistry = new ConcurrentHashMap<>();

    @Override
    public boolean register(String eventId) {
        return eventRegistry.putIfAbsent("event:" + eventId, Instant.now()) == null;
    }
}
