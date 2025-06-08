package com.kafka.order.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.order.model.PaymentEvent;
import com.kafka.order.service.PaymentEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/api/v1/payment-events")
public class PaymentEventController {
    private final PaymentEventProducer paymentEventProducer;

    public PaymentEventController(PaymentEventProducer paymentEventProducer) {
        this.paymentEventProducer = paymentEventProducer;
    }

    @PostMapping
    public ResponseEntity<String> createPaymentEvent(@RequestBody PaymentEvent paymentEvent) {
        paymentEventProducer.sendPaymentEvent(paymentEvent);
        return ResponseEntity.ok("Payment event created successfully");
    }
}
