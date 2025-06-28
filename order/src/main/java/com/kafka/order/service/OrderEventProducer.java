package com.kafka.order.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.kafka.order.Constants.KafkaConstants;
import com.kafka.order.model.OrderEvent;
import com.kafka.order.model.OrderStream;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class OrderEventProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final KafkaTemplate<String, OrderStream> orderStreamKafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate, KafkaTemplate<String, OrderStream> orderStreamKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderStreamKafkaTemplate = orderStreamKafkaTemplate;
    }

    public void sendOrderEvent(OrderEvent orderEvent) {
        log.info("Sending order event: {}", orderEvent);
        String key = orderEvent.getOrderId();
        kafkaTemplate.send(KafkaConstants.ORDER_EVENT_TOPIC, key, orderEvent);
    }

    public void sendOrderStream(OrderStream orderStream) {
        String key = orderStream.getOrderId();
        orderStreamKafkaTemplate.send("order-stream", key, orderStream);
    }
}
