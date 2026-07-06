package com.platfrom.regional.exception;

public class RetryableRegionalException extends RuntimeException {
    public RetryableRegionalException(String message) {
        super(message);
    }
}
