package com.platfrom.ingestion.support;

import org.springframework.util.StringUtils;

import java.util.UUID;

public final class CorrelationContext {

    private static final ThreadLocal<String> CORRELATION_ID = new ThreadLocal<>();

    private CorrelationContext() {
    }

    public static void set(String correlationId) {
        CORRELATION_ID.set(correlationId);
    }

    public static String getOrCreate() {
        String current = CORRELATION_ID.get();
        if (StringUtils.hasText(current)) {
            return current;
        }

        String generated = UUID.randomUUID().toString();
        CORRELATION_ID.set(generated);
        return generated;
    }

    public static void clear() {
        CORRELATION_ID.remove();
    }
}
