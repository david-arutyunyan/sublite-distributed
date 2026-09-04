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

import java.util.Optional;

@Component
public class BillingEventListener {

    private static final Logger log = LoggerFactory.getLogger(BillingEventListener.class);
    private static final String CORRELATION_ID_MDC_KEY = "correlationId";
    private static final String UNKNOWN_CUSTOMER = "unknown";

    private final NotificationService notificationService;
    private final SubscriptionProjectionRepository projections;
    private final ObjectMapper objectMapper;

    public BillingEventListener(
            NotificationService notificationService,
            SubscriptionProjectionRepository projections,
            ObjectMapper objectMapper
    ) {
        this.notificationService = notificationService;
        this.projections = projections;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "billing.events")
    public void onBillingEvent(String value) throws Exception {
        JsonNode envelope = objectMapper.readTree(value);
        String eventType = envelope.get("eventType").asText();
        String correlationId = envelope.get("correlationId").asText();

        MDC.put(CORRELATION_ID_MDC_KEY, correlationId);
        try {
            String eventId = envelope.get("eventId").asText();
            JsonNode payload = envelope.get("payload");
            String subscriptionId = payload.get("subscriptionId").asText();

            // The projection built from subscription.events - see
            // SubscriptionProjection's javadoc for why billing.events
            // alone can't tell us who to notify. A genuine gotcha this
            // demonstrates: events can arrive out of causal order across
            // DIFFERENT topics (no cross-topic ordering guarantee - see
            // docs/architecture.md), so the projection might not exist
            // yet if this consumer somehow got far enough ahead. Falls
            // back to a placeholder rather than dropping the notification
            // outright - losing a customerId isn't a reason to lose the
            // notification history entry itself.
            String customerId = projections.findById(subscriptionId)
                    .map(SubscriptionProjection::getCustomerId)
                    .orElseGet(() -> {
                        log.warn("No subscription projection yet for subscriptionId={} - recording with a placeholder customerId", subscriptionId);
                        return UNKNOWN_CUSTOMER;
                    });

            switch (eventType) {
                case "PaymentSucceeded" -> notificationService.record(eventId, customerId, subscriptionId,
                        "PAYMENT_SUCCEEDED", "Your payment went through - your subscription is now active.");
                case "PaymentFailed" -> notificationService.record(eventId, customerId, subscriptionId,
                        "PAYMENT_FAILED", "We couldn't charge your card (" + reasonOf(payload) + "). Please update your payment details.");
                default -> log.debug("Ignoring event type not handled by notification-service: {}", eventType);
            }
        } finally {
            MDC.remove(CORRELATION_ID_MDC_KEY);
        }
    }

    private static String reasonOf(JsonNode payload) {
        return Optional.ofNullable(payload.get("reason")).map(JsonNode::asText).orElse("unknown reason");
    }
}
