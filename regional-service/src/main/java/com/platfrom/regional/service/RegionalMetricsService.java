package com.platfrom.regional.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class RegionalMetricsService {

    private static final Logger log = LoggerFactory.getLogger(RegionalMetricsService.class);

    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();

    public void incrementProcessed() {
        processed.incrementAndGet();
    }

    public void incrementDuplicate() {
        duplicates.incrementAndGet();
    }

    @Scheduled(fixedDelay = 30000L)
    public void report() {
        log.info(
                "regional_metrics serviceName=regional-service correlationId=system eventId=system region=all retryCount=0 processed={} duplicates={}",
                processed.get(),
                duplicates.get());
    }
}
