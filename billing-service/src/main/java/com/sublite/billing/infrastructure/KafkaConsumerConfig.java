package com.sublite.billing.infrastructure;

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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "billing-service");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * The retry+DLQ mechanism promised since step 2 (the `.DLQ` topics
     * have existed since kafka/create-topics.sh, unused until now) and
     * gotcha #3 in docs/architecture.md ("poison pill blocks the whole
     * partition"): this is what actually fixes that, rather than just
     * documenting it.
     *
     * Two classes of failure, handled differently - retrying a
     * malformed message forever is pointless (it will NEVER parse), but
     * giving up immediately on a transient failure (a flaky external
     * payment gateway, a momentarily-unavailable DB) throws away
     * recoverable work:
     *   - "Poison pill" (JsonProcessingException from a non-JSON
     *     payload, IllegalArgumentException from a malformed UUID,
     *     NullPointerException from a missing envelope field) - no
     *     amount of retrying fixes bad data, so these skip straight to
     *     the DLQ.
     *   - Everything else (DB hiccup, PaymentGateway throwing instead
     *     of returning a Declined result) - retried with exponential
     *     backoff, THEN dead-lettered if still failing after
     *     retryMaxAttempts.
     *
     * Either way, the recoverer publishing to the DLQ is what lets the
     * container commit past the bad record and keep consuming the rest
     * of the partition - without it, a poison pill really would wedge
     * the partition forever (DefaultErrorHandler's own fallback, with no
     * recoverer configured, just logs and skips - losing the message
     * with no trace of it anywhere, which is worse).
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
