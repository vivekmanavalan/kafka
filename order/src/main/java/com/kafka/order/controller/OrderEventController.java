package com.kafka.order.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.order.model.OrderEvent;
import com.kafka.order.model.OrderStream;
import com.kafka.order.service.OrderEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/api/v1/order-events")
public class OrderEventController {
    private final OrderEventProducer orderEventProducer;
    private final OrderEventProducer orderStreamProducer;

    public OrderEventController(OrderEventProducer orderEventProducer) {
        this.orderEventProducer = orderEventProducer;
        this.orderStreamProducer = orderEventProducer;
    }

    @PostMapping
    public ResponseEntity<String> createOrderEvent(@RequestBody OrderEvent orderEvent) {
        orderEventProducer.sendOrderEvent(orderEvent);
        return ResponseEntity.ok("Order event created successfully");
    }

    @PostMapping("/stream")
    public ResponseEntity<String> createOrderStream(@RequestBody OrderStream orderStream) {
        orderStreamProducer.sendOrderStream(orderStream);
        return ResponseEntity.ok("Order stream event created successfully");
    }
}
