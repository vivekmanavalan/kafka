package com.kafka.order.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;
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

    public void sendDataToKtable (OrderStream orderStream){
        String key = orderStream.getOrderId();
        log.info("Order ID received: {}", key);
        orderStreamKafkaTemplate.send(KafkaConstants.SIMPLE_KTABLE_TOPIC, key, orderStream);
    }

    public void convertKTableToStream (KTable<String, OrderStream> dataKTable){
        log.info("Converting KTable data to KStream and pushing to topic: {}", KafkaConstants.SIMPLE_KTABLE_OUTPUT_TOPIC);
        log.info("Received KTable data", dataKTable);
        dataKTable.toStream().mapValues(val -> {
                return OrderEvent.builder()
                    .orderAmount(val.getOrderAmount())
                    .orderId(val.getOrderId())
                    .customerId(val.getOrderId().concat("-cust"))
                    .productId(val.getOrderId().concat("-prod"))
                    .build();
            })
            .to(KafkaConstants.SIMPLE_KTABLE_OUTPUT_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(OrderEvent.class)));
    }
}
