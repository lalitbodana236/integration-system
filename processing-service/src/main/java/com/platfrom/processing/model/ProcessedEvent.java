package com.platfrom.processing.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Data;

import java.io.Serializable;
import java.time.Instant;

@Entity
@Data
@Table(name = "processed_events", indexes = {
        @Index(name = "idx_processed_region_type", columnList = "region,eventType"),
        @Index(name = "idx_processed_customer", columnList = "customerId")
})
public class ProcessedEvent implements Serializable {

    @Id
    private String eventId;

    @Column(nullable = false, unique = true)
    private String requestId;

    private String customerId;

    @Column(nullable = false)
    private String region;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false)
    private String status;

    @Column(length = 4000)
    private String normalizedPayload;

    private String workflowState;

    private Instant createdAt;

    private Instant updatedAt;

    @Version
    private Long version;
}
