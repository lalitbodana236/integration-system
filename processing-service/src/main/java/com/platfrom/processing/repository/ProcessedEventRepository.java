package com.platfrom.processing.repository;

import com.platfrom.processing.model.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
    boolean existsByRequestId(String requestId);
}
