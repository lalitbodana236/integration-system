package com.platfrom.notification.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.Instant;

@Entity
@Data
@Table(name = "notification_delivery", indexes = {
        @Index(name = "idx_notification_region_channel", columnList = "region,channel"),
        @Index(name = "idx_notification_status", columnList = "deliveryStatus")
})
public class NotificationDelivery {

    @Id
    private String deliveryId;

    private String eventId;
    private String correlationId;
    private String channel;
    private String region;
    private String deliveryStatus;
    private Instant createdAt;
    private Instant updatedAt;
}
