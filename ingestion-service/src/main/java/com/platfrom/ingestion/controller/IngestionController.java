package com.platfrom.ingestion.controller;

import com.platfrom.ingestion.model.ApiAckResponse;
import com.platfrom.ingestion.service.IngestionEventService;
import com.platfrom.ingestion.service.StorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class IngestionController {

    private final IngestionEventService ingestionEventService;
    private final StorageService storageService;

    @PostMapping("/inventory")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ApiAckResponse inventory(
            @RequestParam String id,
            @RequestBody String payload,
            @RequestParam(required = false) String customerId,
            @RequestParam(required = false) String region,
            @RequestHeader(value = "X-Event-Id", required = false) String eventId) {
        return ingestionEventService.ingest("INVENTORY", id, payload, customerId, region, eventId);
    }

    @PostMapping("/po")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ApiAckResponse po(
            @RequestParam String id,
            @RequestBody String payload,
            @RequestParam(required = false) String customerId,
            @RequestParam(required = false) String region,
            @RequestHeader(value = "X-Event-Id", required = false) String eventId) {
        return ingestionEventService.ingest("PO", id, payload, customerId, region, eventId);
    }

    @PostMapping("/so")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ApiAckResponse so(
            @RequestParam String id,
            @RequestBody String payload,
            @RequestParam(required = false) String customerId,
            @RequestParam(required = false) String region,
            @RequestHeader(value = "X-Event-Id", required = false) String eventId) {
        return ingestionEventService.ingest("SO", id, payload, customerId, region, eventId);
    }

    @PostMapping("/checklist")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ApiAckResponse checklist(
            @RequestParam String id,
            @RequestBody String payload,
            @RequestParam(required = false) String customerId,
            @RequestParam(required = false) String region,
            @RequestHeader(value = "X-Event-Id", required = false) String eventId) {
        return ingestionEventService.ingest("CHECKLIST", id, payload, customerId, region, eventId);
    }

    @PostMapping("/location")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ApiAckResponse location(
            @RequestParam String id,
            @RequestBody String payload,
            @RequestParam(required = false) String customerId,
            @RequestParam(required = false) String region,
            @RequestHeader(value = "X-Event-Id", required = false) String eventId) {
        return ingestionEventService.ingest("LOCATION", id, payload, customerId, region, eventId);
    }

    @PostMapping("/media")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ApiAckResponse upload(
            @RequestParam("file") MultipartFile file,
            @RequestParam("id") String id,
            @RequestParam(required = false) String customerId,
            @RequestParam(required = false) String region,
            @RequestHeader(value = "X-Event-Id", required = false) String eventId) throws IOException {
        String fileUrl = storageService.upload(file);
        return ingestionEventService.ingest("MEDIA", id, fileUrl, customerId, region, eventId);
    }
}
