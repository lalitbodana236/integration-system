package com.platfrom.processing.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class ProcessingMetricsService {

    private static final Logger log = LoggerFactory.getLogger(ProcessingMetricsService.class);

    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong retried = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong totalLatencyMs = new AtomicLong();

    public void incrementProcessed() {
        processed.incrementAndGet();
    }

    public void incrementFailure() {
        failed.incrementAndGet();
    }

    public void incrementRetry() {
        retried.incrementAndGet();
    }

    public void incrementDuplicate() {
        duplicates.incrementAndGet();
    }

    public void recordProcessingLatency(long latencyMs) {
        totalLatencyMs.addAndGet(latencyMs);
    }

    @Scheduled(fixedDelay = 30000L)
    public void logSnapshot() {
        long processedCount = processed.get();
        long averageLatency = processedCount == 0 ? 0 : totalLatencyMs.get() / processedCount;
        log.info(
                "processing_metrics serviceName=processing-service correlationId=system eventId=system region=all retryCount=0 throughput={} failures={} retries={} duplicates={} avgLatencyMs={}",
                processedCount,
                failed.get(),
                retried.get(),
                duplicates.get(),
                averageLatency);
    }
}
