package com.hotelsearch.infrastructure.config;

import com.hotelsearch.application.port.out.HotelSearchPublisher;
import com.hotelsearch.infrastructure.output.kafka.HotelSearchMessage;
import com.hotelsearch.infrastructure.output.kafka.KafkaHotelSearchPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Beans del adaptador de publicación en Kafka. El {@link KafkaTemplate} lo
 * provee la autoconfiguración de Spring Boot con las propiedades
 * {@code spring.kafka.producer.*} de {@code application.yml}.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaProducerConfiguration {

    @Bean
    HotelSearchPublisher hotelSearchPublisher(
            KafkaTemplate<String, HotelSearchMessage> kafkaTemplate,
            @Value("${hotel-search.kafka.searches-topic}") String searchesTopic) {
        return new KafkaHotelSearchPublisher(kafkaTemplate, searchesTopic);
    }
}
