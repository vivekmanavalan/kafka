package com.kafka.order.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.kafka.order.Constants.KafkaConstants;
import com.kafka.order.model.OrderEvent;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KTableConsumer {

    @KafkaListener(topics = KafkaConstants.SIMPLE_KTABLE_OUTPUT_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void consumeKTableOutput(ConsumerRecord<String, OrderEvent> record) {
        log.info("KTable Output - Key: {}, Value: {}", record.key(), record.value());
        log.info("KTable Output - Order ID: {}, Amount: {}, Status: {}", 
            record.value().getOrderId(), 
            record.value().getOrderAmount(), 
            record.value().getOrderStatus());
    }
} 