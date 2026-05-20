package com.platfrom.notification.exception;

public class RetryableNotificationException extends RuntimeException {
    public RetryableNotificationException(String message) {
        super(message);
    }
}
