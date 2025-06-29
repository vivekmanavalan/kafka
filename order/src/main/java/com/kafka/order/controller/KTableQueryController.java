package com.kafka.order.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.order.model.OrderStream;
import com.kafka.order.service.KTableQueryService;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/api/v1/ktable")
public class KTableQueryController {

    private final KTableQueryService kTableQueryService;

    public KTableQueryController(KTableQueryService kTableQueryService) {
        this.kTableQueryService = kTableQueryService;
    }

    @GetMapping("/peek/{key}")
    public ResponseEntity<?> peekValueForKey(@PathVariable String key) {
        log.info("Peeking value for key: {}", key);
        
        // Check if streams is ready
        if (!kTableQueryService.isStreamsReady()) {
            return ResponseEntity.status(503)
                .body("Kafka Streams is not ready yet. Please try again in a moment.");
        }
        
        // Query the KTable
        OrderStream value = kTableQueryService.peekValueForKey(key);
        
        if (value != null) {
            return ResponseEntity.ok(value);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/status")
    public ResponseEntity<?> getStreamsStatus() {
        boolean isReady = kTableQueryService.isStreamsReady();
        return ResponseEntity.ok("{\"streamsReady\": " + isReady + "}");
    }

    @GetMapping("/count")
    public ResponseEntity<?> getOrderCount() {
        return ResponseEntity.ok(kTableQueryService.getOrderCount());
    }

    
} 