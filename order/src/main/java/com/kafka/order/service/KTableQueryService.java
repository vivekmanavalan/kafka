package com.kafka.order.service;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import com.kafka.order.model.OrderStream;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KTableQueryService {

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public OrderStream peekValueForKey(String key) {
        try {
            // Get the streams instance
            var kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            
            if (kafkaStreams == null) {
                log.warn("Kafka Streams is not ready yet");
                return null;
            }

            // Get the state store
            ReadOnlyKeyValueStore<String, OrderStream> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("order-ktable-store", QueryableStoreTypes.keyValueStore())
            );

            if (store == null) {
                log.warn("State store 'order-ktable-store' not found");
                return null;
            }

            // Query the value for the key
            OrderStream value = store.get(key);
            
            if (value != null) {
                log.info("Found value for key '{}': {}", key, value);
            } else {
                log.info("No value found for key '{}'", key);
            }
            
            return value;
            
        } catch (Exception e) {
            log.error("Error querying KTable for key '{}': {}", key, e.getMessage(), e);
            return null;
        }
    }

    public boolean isStreamsReady() {
        try {
            var kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            return kafkaStreams != null && kafkaStreams.state().name().equals("RUNNING");
        } catch (Exception e) {
            log.error("Error checking streams state: {}", e.getMessage());
            return false;
        }
    }

    public int getOrderCount(){
        var kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if(kafkaStreams==null){
            log.info("Streams is not available now");
            return 0;
        }

        ReadOnlyKeyValueStore<String, OrderStream> store = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("order-ktable-store", QueryableStoreTypes.keyValueStore()));

        if(store == null){
            log.info("NO data in store");
            return 0;
        }
        log.info("Getting the number of records stored in ktable");
        var data = store.all();
        int count = 0;
        while (data.hasNext()){
            log.info("count: {}", count);
            count++;
            data.next();
        }
        data.close();
        return count;
    }
} 