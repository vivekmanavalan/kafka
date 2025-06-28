package com.kafka.order.config;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import com.kafka.order.Constants.KafkaConstants;

@Configuration
public class TopicConfig {

    // @Bean
    // public NewTopic orderEventTopic() {
    //     return TopicBuilder.name(KafkaConstants.ORDER_EVENT_TOPIC)
    //     .partitions(3)
    //     .replicas(1)
    //     .configs(Map.of("retention.ms", String.valueOf(Duration.ofDays(3).toMillis())))
    //     .build();
    // }

    // @Bean
    // public NewTopic paymentEventTopic() {
    //     return TopicBuilder.name("payment-event")
    //     .partitions(3)
    //     .replicas(1)
    //     .compact()
    //     .configs(Map.of("retention.ms", String.valueOf(Duration.ofDays(3).toMillis())))
    //     .build();
    // }

    //StreamsTopic
    @Bean
    public NewTopic largeOrderStreamTopic() {
        return TopicBuilder.name("large-order-topic")
        .partitions(1)
        .replicas(1)
        .compact()
        .configs(Map.of("retention.ms", String.valueOf(Duration.ofDays(3).toMillis())))
        .build();
    }

    @Bean
    public NewTopic orderStreamTopic() {
        return TopicBuilder.name("order-stream")
            .partitions(1)
            .replicas(1)
            .build();
    }
}
