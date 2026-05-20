package com.platfrom.regional.repository;

import com.platfrom.regional.model.RegionalWorkflow;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RegionalWorkflowRepository extends JpaRepository<RegionalWorkflow, String> {
    boolean existsByRequestId(String requestId);
}
