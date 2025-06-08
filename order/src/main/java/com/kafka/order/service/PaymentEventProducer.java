package com.kafka.order.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.kafka.order.Constants.KafkaConstants;
import com.kafka.order.model.PaymentEvent;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class PaymentEventProducer {
    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<String, PaymentEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPaymentEvent(PaymentEvent paymentEvent) {
        log.info("Sending payment event: {}", paymentEvent);
        String key = paymentEvent.getPaymentId();
        kafkaTemplate.send(KafkaConstants.PAYMENT_EVENT_TOPIC, key, paymentEvent);
    }
}
