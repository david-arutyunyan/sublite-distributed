package com.sublite.subscription.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    /**
     * The JsonSerializer needs Boot's OWN ObjectMapper bean here, not a
     * bare `new ObjectMapper()` - Boot's autoconfigured one already
     * disables WRITE_DATES_AS_TIMESTAMPS and registers JavaTimeModule by
     * default (that's why every REST response elsewhere in this project
     * already renders Instant as an ISO-8601 string without any special
     * config). A hand-built ObjectMapper doesn't inherit any of that -
     * exactly the gap that shipped occurredAt as a numeric epoch
     * timestamp the first time this class was written, silently, until a
     * live Kafka message caught it. Reuse Boot's, don't rebuild a worse
     * copy of it.
     */
    @Bean
    public ProducerFactory<String, EventEnvelope> producerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Idempotent PRODUCER (default true in modern Kafka clients, set
        // explicitly to make it visible): dedupes retries caused by the
        // producer's own network hiccups. Not the same thing as
        // idempotent CONSUMER logic (docs/architecture.md's gotcha #4) -
        // this alone does nothing about a consumer seeing this same
        // message twice after a rebalance or a crash-before-commit.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JsonSerializer<>(objectMapper));
    }

    @Bean
    public KafkaTemplate<String, EventEnvelope> kafkaTemplate(ProducerFactory<String, EventEnvelope> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
