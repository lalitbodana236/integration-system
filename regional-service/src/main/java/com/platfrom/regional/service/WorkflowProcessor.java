package com.platfrom.regional.service;

import com.platfrom.regional.exception.RetryableRegionalException;
import com.platfrom.regional.model.BaseEvent;
import com.platfrom.regional.model.RegionalWorkflow;
import com.platfrom.regional.repository.RegionalWorkflowRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;

@Service
public class WorkflowProcessor {

    private static final Logger log = LoggerFactory.getLogger(WorkflowProcessor.class);

    private final RegionalWorkflowRepository workflowRepository;
    private final NotificationEventPublisher notificationEventPublisher;
    private final RegionalMetricsService metricsService;
    
    public WorkflowProcessor(RegionalWorkflowRepository workflowRepository, NotificationEventPublisher notificationEventPublisher, RegionalMetricsService metricsService) {
        this.workflowRepository = workflowRepository;
        this.notificationEventPublisher = notificationEventPublisher;
        this.metricsService = metricsService;
    }
    
    @Transactional
    public void process(BaseEvent event) {
        if (workflowRepository.existsById(event.getEventId()) || workflowRepository.existsByRequestId(event.getRequestId())) {
            metricsService.incrementDuplicate();
            log.info(
                    "regional_duplicate_skipped serviceName=regional-service correlationId={} eventId={} region={} retryCount={} requestId={}",
                    event.getCorrelationId(),
                    event.getEventId(),
                    event.getRegion(),
                    event.getRetryCount(),
                    event.getRequestId());
            return;
        }

        validate(event);

        RegionalWorkflow workflow = new RegionalWorkflow();
        workflow.setEventId(event.getEventId());
        workflow.setRequestId(event.getRequestId());
        workflow.setRegion(event.getRegion());
        workflow.setCustomerId(event.getCustomerId());
        workflow.setPayloadSnapshot(event.getPayload());
        workflow.setExecutionStatus("COMPLETED");
        workflow.setWorkflowState("NOTIFICATION_PENDING");
        workflow.setCreatedAt(Instant.now());
        workflow.setUpdatedAt(Instant.now());

        workflowRepository.save(workflow);
        notificationEventPublisher.publish(event);
        metricsService.incrementProcessed();
    }

    private void validate(BaseEvent event) {
        if (!StringUtils.hasText(event.getRegion())) {
            throw new RetryableRegionalException("Region is required");
        }
        if (!StringUtils.hasText(event.getPayload())) {
            throw new IllegalArgumentException("Payload is required");
        }
    }
}
