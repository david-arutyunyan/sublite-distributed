package com.sublite.subscription.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sublite.subscription.application.EventEnvelope;
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
 * An explicit ProducerFactory/KafkaTemplate<String, EventEnvelope>
 * instead of relying on Boot's auto-configured KafkaTemplate<Object,
 * Object> - generic type erasure makes injecting the auto-configured one
 * into a more specific KafkaTemplate<String, EventEnvelope> dependency
 * fragile (Spring's generic-aware bean matching doesn't reliably treat
 * Object as a stand-in for a concrete type parameter here). Explicit and
 * a few lines longer, but it's obvious what's actually being sent on the
 * wire.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, EventEnvelope> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Idempotent PRODUCER (default true in modern Kafka clients, set
        // explicitly to make it visible): dedupes retries caused by the
        // producer's own network hiccups. Not the same thing as
        // idempotent CONSUMER logic (docs/architecture.md's gotcha #4) -
        // this alone does nothing about a consumer seeing this same
        // message twice after a rebalance or a crash-before-commit.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // A serializer INSTANCE, not the class (Kafka would otherwise
        // reflectively construct JsonSerializer with its own default
        // ObjectMapper) - needed so occurredAt (an Instant) writes as an
        // ISO-8601 string matching docs/architecture.md's envelope, not
        // Jackson's default of a numeric epoch-seconds timestamp. That
        // default would have quietly broken the future Go consumer:
        // Go's encoding/json + time.Time expects RFC3339 strings, not a
        // float.
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JsonSerializer<>(objectMapper));
    }

    @Bean
    public KafkaTemplate<String, EventEnvelope> kafkaTemplate(ProducerFactory<String, EventEnvelope> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
