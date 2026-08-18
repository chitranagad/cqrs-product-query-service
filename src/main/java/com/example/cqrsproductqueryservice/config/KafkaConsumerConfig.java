package com.example.cqrsproductqueryservice.config;

import com.example.cqrsproductqueryservice.dto.ProductEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, ProductEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        // 1. Connection Configurations
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "product-query-service-v4");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. Setup Deserializer Strategies purely via the Properties Map
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        // Tells ErrorHandlingDeserializer to delegate parsing tasks to Jackson
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);

        // Tells Jackson the absolute fallback class target type to map data to
        props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, "com.example.cqrsproductqueryservice.dto.ProductEvent");
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");

        // Maps the incoming class paths explicitly
        props.put(JacksonJsonDeserializer.TYPE_MAPPINGS,
                "com.example.cqrsproductcommandservice.dto.ProductEvent:com.example.cqrsproductqueryservice.dto.ProductEvent"
        );

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ProductEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ProductEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}