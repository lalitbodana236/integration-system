package com.platfrom.notification.repository;

import com.platfrom.notification.model.NotificationDelivery;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificationDeliveryRepository extends JpaRepository<NotificationDelivery, String> {
    boolean existsByEventIdAndChannel(String eventId, String channel);
}
