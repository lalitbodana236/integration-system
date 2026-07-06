package com.platfrom.notification.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class NotificationMetricsService {

    private static final Logger log = LoggerFactory.getLogger(NotificationMetricsService.class);

    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();

    public void incrementDelivered() {
        delivered.incrementAndGet();
    }

    public void incrementDuplicate() {
        duplicates.incrementAndGet();
    }

    @Scheduled(fixedDelay = 30000L)
    public void report() {
        log.info(
                "notification_metrics serviceName=notification-service correlationId=system eventId=system region=all retryCount=0 delivered={} duplicates={}",
                delivered.get(),
                duplicates.get());
    }
}
