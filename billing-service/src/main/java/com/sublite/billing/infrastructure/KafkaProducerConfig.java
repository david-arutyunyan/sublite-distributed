package com.sublite.billing.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sublite.billing.application.EventEnvelope;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Same shape as subscription-service's KafkaProducerConfig - explicit
 * typed ProducerFactory/KafkaTemplate (generic erasure makes Boot's
 * auto-configured one fragile to inject here), idempotent producer, and
 * JsonSerializer built from Boot's OWN ObjectMapper bean (already has
 * JavaTimeModule + ISO-8601 dates by default) rather than a hand-built
 * one - see subscription-service's KafkaProducerConfig javadoc for the
 * real bug that shipped the one time this wasn't done that way.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, EventEnvelope> producerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JsonSerializer<>(objectMapper));
    }

    @Bean
    public KafkaTemplate<String, EventEnvelope> kafkaTemplate(ProducerFactory<String, EventEnvelope> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
