package com.kafka.order.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.kafka.order.Constants.KafkaConstants;
import com.kafka.order.model.OrderEvent;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class OrderEventProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderEvent(OrderEvent orderEvent) {
        log.info("Sending order event: {}", orderEvent);
        String key = orderEvent.getOrderId();
        kafkaTemplate.send(KafkaConstants.ORDER_EVENT_TOPIC, key, orderEvent);
    }
}
