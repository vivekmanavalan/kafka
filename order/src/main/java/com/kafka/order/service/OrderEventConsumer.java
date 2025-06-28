package com.kafka.order.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.kafka.order.Constants.KafkaConstants;
import com.kafka.order.model.OrderEvent;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class OrderEventConsumer {

    // @KafkaListener(topics = {KafkaConstants.ORDER_EVENT_TOPIC, KafkaConstants.PAYMENT_EVENT_TOPIC}, groupId = "${spring.kafka.consumer.group-id}")
    // public void consumeOrderEvent(ConsumerRecord<String, OrderEvent> record) {
    //     log.info("Received order event: {}", record.value());
    //     log.info("Partition: {}", record.partition());
    //     log.info("Offset: {}", record.offset());
    //     log.info("Key: {}", record.key());
    //     log.info("Timestamp: {}", record.timestamp());
    //     log.info("Offset committed");
    // }

    // //listen to 2 topics in same consumer group

    // @KafkaListener(topics = {KafkaConstants.ORDER_EVENT_TOPIC, KafkaConstants.PAYMENT_EVENT_TOPIC}, groupId = "${spring.kafka.consumer.group-id}")
    // public void consumeOrderEvent2(ConsumerRecord<String, OrderEvent> record) {
    //     log.info("Received order event in consumer 2: {}", record.value());
    //     log.info("Partition: {}", record.partition());
    //     log.info("Offset: {}", record.offset());
    //     log.info("Key: {}", record.key());
    //     log.info("Timestamp: {}", record.timestamp());
    //     log.info("Offset committed");
    // }

    // @KafkaListener(topics = {KafkaConstants.PAYMENT_EVENT_TOPIC, KafkaConstants.ORDER_EVENT_TOPIC}, groupId = "${spring.kafka.consumer.group-id}")
    // public void consumeOrderEvent3(ConsumerRecord<String, OrderEvent> record) {
    //     log.info("Received order event in consumer 3: {}", record.value());
    //     log.info("Partition: {}", record.partition());
    //     log.info("Offset: {}", record.offset());
    //     log.info("Key: {}", record.key());
    //     log.info("Timestamp: {}", record.timestamp());
    //     log.info("Offset committed");
    // }

    // @KafkaListener(topics = {KafkaConstants.PAYMENT_EVENT_TOPIC, KafkaConstants.ORDER_EVENT_TOPIC}, groupId = "${spring.kafka.consumer.group-id}")
    // public void consumeOrderEvent4(ConsumerRecord<String, OrderEvent> record) {
    //     log.info("Received order event in consumer 4: {}", record.value());
    //     log.info("Partition: {}", record.partition());
    //     log.info("Offset: {}", record.offset());
    //     log.info("Key: {}", record.key());
    //     log.info("Timestamp: {}", record.timestamp());
    //     log.info("Offset committed");
    // }

    @KafkaListener(topics = {KafkaConstants.LARGE_ORDER_STREAM_TOPIC}, groupId = "${spring.kafka.consumer.group-id}")
    public void consumeOrderStream(ConsumerRecord<String, OrderEvent> record) {
        log.info("Received order stream: {}", record.value());
        log.info("Order ID: {}", record.value().getOrderId());
        log.info("Order Status: {}", record.value().getOrderStatus());
        log.info("Order Amount: {}", record.value().getOrderAmount());
        log.info("Customer ID: {}", record.value().getCustomerId());
        log.info("Product ID: {}", record.value().getProductId());
        log.info("Offset committed");
    }
}
