package com.platfrom.notification.service;

import com.platfrom.notification.model.NotificationEvent;

public interface NotificationSender {
    boolean supports(String channel);

    void send(NotificationEvent event);
}
