package com.platfrom.notification.service;

import com.platfrom.notification.model.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SmsSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(SmsSender.class);

    @Override
    public boolean supports(String channel) {
        return "SMS".equalsIgnoreCase(channel);
    }

    @Override
    public void send(NotificationEvent event) {
        log.info(
                "sms_dispatched serviceName=notification-service correlationId={} eventId={} region={} retryCount={} requestId={}",
                event.getCorrelationId(),
                event.getEventId(),
                event.getRegion(),
                event.getRetryCount(),
                event.getRequestId());
    }
}
