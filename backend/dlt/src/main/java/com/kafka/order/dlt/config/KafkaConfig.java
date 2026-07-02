package com.kafka.order.dlt.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;

@Configuration
public class KafkaConfig {
    
    @Bean
    public RecordMessageConverter converter() {
        return new JsonMessageConverter();
    }
}