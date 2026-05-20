package com.platfrom.ingestion.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class RequestLoggingInterceptor implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingInterceptor.class);
    private static final String START_TIME = "requestStartTime";

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        request.setAttribute(START_TIME, System.currentTimeMillis());
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception exception) {
        Object start = request.getAttribute(START_TIME);
        long latency = start instanceof Long value ? System.currentTimeMillis() - value : -1L;
        log.info(
                "http_request_completed serviceName=ingestion-service correlationId={} eventId=na region={} retryCount=0 method={} path={} status={} latencyMs={}",
                MDC.get("correlationId"),
                request.getParameter("region"),
                request.getMethod(),
                request.getRequestURI(),
                response.getStatus(),
                latency);
    }
}
