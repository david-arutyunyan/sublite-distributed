package com.sublite.billing.infrastructure;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumes raw String, not Spring Kafka's JsonDeserializer<EventEnvelope>
 * - deliberately. JsonDeserializer's default behavior trusts a
 * `__TypeId__` header the PRODUCER attaches, naming ITS OWN class
 * (com.sublite.subscription.application.EventEnvelope) - a class that
 * doesn't exist on billing-service's classpath at all (different
 * package, different service), so that header is actively wrong to
 * trust here even though it's structurally the same record. The
 * consumer just parses the JSON itself, against its OWN local
 * EventEnvelope, in SubscriptionEventListener - which is also the only
 * approach that will still work once analytics-service (Go) starts
 * consuming these same topics; a Go service has no Java class headers
 * to trust in the first place.
 *
 * ENABLE_AUTO_COMMIT_CONFIG is off, and AckMode.RECORD replaces it - the
 * gotcha from docs/architecture.md made concrete: Kafka's native
 * auto-commit fires on a timer regardless of whether processing actually
 * succeeded (a crash mid-processing after an auto-commit already fired
 * loses the message, not just duplicates it). RECORD mode commits
 * through Spring's container instead, right after the listener method
 * returns WITHOUT throwing - so a failure naturally leaves the offset
 * uncommitted and the record gets redelivered on restart/rebalance.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "billing-service");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
