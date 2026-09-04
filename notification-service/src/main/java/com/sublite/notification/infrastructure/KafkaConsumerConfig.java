package com.sublite.notification.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.HashMap;
import java.util.Map;

/**
 * Same shape and reasoning as subscription-service's and billing-service's
 * own KafkaConsumerConfig - raw String consumption (never JsonDeserializer
 * with trusted type headers), AckMode.RECORD instead of native auto-commit.
 * group.id "notification-service" is its own, independent of both other
 * groups already reading subscription.events/billing.events - all three
 * get the full stream, exactly the fan-out this project's whole point is
 * to demonstrate.
 *
 * One error handler, shared by BOTH listeners (SubscriptionEventListener
 * on subscription.events, BillingEventListener on billing.events) - the
 * DeadLetterPublishingRecoverer below reads the failing record's OWN
 * topic to pick a destination, so it dead-letters to whichever of the
 * two `.DLQ` topics actually matches.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${sublite.kafka.retry.max-attempts:4}")
    private int retryMaxAttempts;
    @Value("${sublite.kafka.retry.initial-interval-ms:500}")
    private long retryInitialIntervalMs;
    @Value("${sublite.kafka.retry.multiplier:2.0}")
    private double retryMultiplier;
    @Value("${sublite.kafka.retry.max-interval-ms:5000}")
    private long retryMaxIntervalMs;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-service");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Same retry+DLQ mechanism as the other two services' own
     * KafkaConsumerConfig - see billing-service's javadoc for the full
     * reasoning on the retryable/non-retryable split.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> dlqKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dlqKafkaTemplate,
                (record, ex) -> {
                    log.error("Sending to DLQ after {}: topic={}, partition={}, offset={}, key={}",
                            ex.getClass().getSimpleName(), record.topic(), record.partition(), record.offset(), record.key(), ex);
                    return new TopicPartition(record.topic() + ".DLQ", record.partition());
                });

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retryMaxAttempts);
        backOff.setInitialInterval(retryInitialIntervalMs);
        backOff.setMultiplier(retryMultiplier);
        backOff.setMaxInterval(retryMaxIntervalMs);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(
                JsonProcessingException.class,
                IllegalArgumentException.class,
                NullPointerException.class
        );
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry attempt {} for topic={}, partition={}, offset={}: {}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage()));
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }
}
