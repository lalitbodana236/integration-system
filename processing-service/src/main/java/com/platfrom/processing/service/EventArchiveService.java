package com.platfrom.processing.service;

import com.platfrom.processing.model.BaseEvent;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class EventArchiveService {

    private final Map<String, List<String>> archive = new ConcurrentHashMap<>();

    public void archiveIncoming(BaseEvent event, String normalizedPayload) {
        archive.computeIfAbsent(event.getEventId(), key -> new ArrayList<>())
                .add(Instant.now() + "|" + event.getRegion() + "|" + normalizedPayload);
    }

    public List<String> history(String eventId) {
        return archive.getOrDefault(eventId, List.of());
    }
}
