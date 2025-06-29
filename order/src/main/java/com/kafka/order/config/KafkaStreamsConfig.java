package com.kafka.order.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.kafka.order.model.OrderEvent;
import com.kafka.order.model.OrderStream;
import com.kafka.order.service.OrderEventProducer;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired
    private OrderEventProducer orderEventProducer;

    @Bean
    public KStream<String, OrderStream> orderStreamProcessor(StreamsBuilder builder) {
        //Stream configurations are in the application.properties file
        //KStream is a stream of records. It is a sequence of records that can be processed in parallel.
        //JsonSerde for value
        JsonSerde<OrderStream> jsonOrderStreamSerde = new JsonSerde<>(OrderStream.class);
        KStream<String, OrderStream> orderStream = builder.stream("order-stream", Consumed.with(Serdes.String(), jsonOrderStreamSerde));

        //Filter large orders
        KStream<String, OrderEvent> largeOrderStream = orderStream.filter((key, val) -> {
            return val.getOrderAmount() > 1000;
        }).mapValues(val -> OrderEvent.builder()
        .orderId(val.getOrderId())
        .orderStatus(val.getOrderStatus())
        .orderAmount(val.getOrderAmount())
        .customerId(val.getOrderId().concat("-cust"))
        .productId(val.getOrderId().concat("-prod"))
        .build());

        //Send to large order topic
        log.info("Sending large order stream to large-order-topic {}", largeOrderStream);
        largeOrderStream.to("large-order-topic", Produced.with(Serdes.String(), new JsonSerde<>(OrderEvent.class)));
        return orderStream;
    }

    @Bean
    public KTable<String, OrderStream> simpleKTable(StreamsBuilder builder) {
        JsonSerde<OrderStream> orderStreamJsonSerde = new JsonSerde<>(OrderStream.class);
        KTable<String, OrderStream> orderKTable = builder.table("simple-ktable", 
                Consumed.with(Serdes.String(), orderStreamJsonSerde),
                Materialized.as("order-ktable-store"));

        KTable<String, OrderStream> processKTable = orderKTable.filter((key, val) -> {
            log.info("Processing orderStream using Ktable");
                return val.getOrderAmount() > 2000;
            });
        
        if(processKTable != null){
            orderEventProducer.convertKTableToStream(processKTable);
        } else {
            log.info("No records passed the filer condition");
        }
        return processKTable;
    }
}
