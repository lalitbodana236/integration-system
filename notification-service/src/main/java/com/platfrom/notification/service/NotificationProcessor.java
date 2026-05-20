package com.platfrom.notification.service;

import com.platfrom.notification.model.NotificationDelivery;
import com.platfrom.notification.model.NotificationEvent;
import com.platfrom.notification.repository.NotificationDeliveryRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class NotificationProcessor {

    private final List<NotificationSender> senders;
    private final NotificationDeliveryRepository deliveryRepository;
    private final NotificationMetricsService metricsService;
    
    public NotificationProcessor(List<NotificationSender> senders, NotificationDeliveryRepository deliveryRepository, NotificationMetricsService metricsService) {
        this.senders = senders;
        this.deliveryRepository = deliveryRepository;
        this.metricsService = metricsService;
    }
    
    @Transactional
    public void process(NotificationEvent event) {
        for (String channel : event.getChannels()) {
            if (deliveryRepository.existsByEventIdAndChannel(event.getEventId(), channel)) {
                metricsService.incrementDuplicate();
                continue;
            }

            NotificationSender sender = senders.stream()
                    .filter(candidate -> candidate.supports(channel))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unsupported channel: " + channel));

            sender.send(event);

            NotificationDelivery delivery = new NotificationDelivery();
            delivery.setDeliveryId(UUID.randomUUID().toString());
            delivery.setEventId(event.getEventId());
            delivery.setCorrelationId(event.getCorrelationId());
            delivery.setChannel(channel);
            delivery.setRegion(event.getRegion());
            delivery.setDeliveryStatus("DELIVERED");
            delivery.setCreatedAt(Instant.now());
            delivery.setUpdatedAt(Instant.now());
            deliveryRepository.save(delivery);
            metricsService.incrementDelivered();
        }
    }
}
