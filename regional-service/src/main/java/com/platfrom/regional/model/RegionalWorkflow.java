package com.platfrom.regional.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Data;

import java.time.Instant;

@Entity
@Data
@Table(name = "regional_workflow", indexes = {
        @Index(name = "idx_regional_region_state", columnList = "region,workflowState"),
        @Index(name = "idx_regional_customer", columnList = "customerId")
})
public class RegionalWorkflow {

    @Id
    private String eventId;

    @Column(nullable = false, unique = true)
    private String requestId;

    @Column(nullable = false)
    private String region;

    private String customerId;

    private String workflowState;

    private String executionStatus;

    @Column(length = 4000)
    private String payloadSnapshot;

    private Instant createdAt;

    private Instant updatedAt;

    @Version
    private Long version;
}
