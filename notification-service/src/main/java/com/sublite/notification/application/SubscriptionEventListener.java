package com.sublite.notification.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sublite.notification.domain.SubscriptionProjection;
import com.sublite.notification.infrastructure.SubscriptionProjectionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * consumer group "notification-service" (see KafkaConsumerConfig) - its
 * own group, independent of billing-service's, so both get the full
 * subscription.events stream. Only acts on SubscriptionCreated for now
 * (the only lifecycle event this project actually publishes yet);
 * everything else is quietly ignored, same forward-compatibility
 * reasoning as the other two services' listeners.
 */
@Component
public class SubscriptionEventListener {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionEventListener.class);
    private static final String CORRELATION_ID_MDC_KEY = "correlationId";

    private final NotificationService notificationService;
    private final SubscriptionProjectionRepository projections;
    private final ObjectMapper objectMapper;

    public SubscriptionEventListener(
            NotificationService notificationService,
            SubscriptionProjectionRepository projections,
            ObjectMapper objectMapper
    ) {
        this.notificationService = notificationService;
        this.projections = projections;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "subscription.events")
    public void onSubscriptionEvent(String value) throws Exception {
        JsonNode envelope = objectMapper.readTree(value);
        String eventType = envelope.get("eventType").asText();
        String correlationId = envelope.get("correlationId").asText();

        MDC.put(CORRELATION_ID_MDC_KEY, correlationId);
        try {
            if (!"SubscriptionCreated".equals(eventType)) {
                log.debug("Ignoring event type not handled by notification-service: {}", eventType);
                return;
            }

            String eventId = envelope.get("eventId").asText();
            JsonNode payload = envelope.get("payload");
            String subscriptionId = payload.get("subscriptionId").asText();
            String customerId = payload.get("customerId").asText();
            String planName = payload.get("planName").asText();

            // Upsert, not insert-once: this projection just needs to
            // reflect the latest known mapping, and a redelivered
            // SubscriptionCreated carries the same (subscriptionId ->
            // customerId) fact anyway - re-writing it is harmless, unlike
            // the Notification document below where a redelivery must NOT
            // produce a second entry.
            projections.save(new SubscriptionProjection(subscriptionId, customerId));

            notificationService.record(eventId, customerId, subscriptionId, "SUBSCRIPTION_CREATED",
                    "Welcome! Your subscription to " + planName + " is being set up.");
        } finally {
            MDC.remove(CORRELATION_ID_MDC_KEY);
        }
    }
}
