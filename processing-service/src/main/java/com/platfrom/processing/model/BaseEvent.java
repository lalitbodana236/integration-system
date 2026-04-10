package com.platfrom.processing.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BaseEvent implements Serializable {
    private String requestId;
    private String eventType;
    private String payload;
    private long timestamp;
}
