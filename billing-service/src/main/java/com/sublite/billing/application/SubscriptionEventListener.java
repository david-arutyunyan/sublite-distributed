package com.sublite.billing.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * consumer group "billing-service" (see KafkaConsumerConfig) - its own
 * group, independent of any other service that also listens to
 * subscription.events (notification-service will, from step 5-6), so
 * each gets the full stream instead of splitting partitions between
 * unrelated consumers (docs/architecture.md's consumer-groups section).
 *
 * subscription.events carries more event types than just the two handled
 * below (see the catalog in docs/architecture.md) - anything else is
 * quietly ignored, on purpose. Ignoring unknown/not-yet-relevant event
 * types is what lets subscription-service start publishing
 * SubscriptionRenewalDue later without this listener needing to change
 * or reject anything - forward compatibility, not an oversight.
 */
@Component
public class SubscriptionEventListener {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionEventListener.class);
    private static final String CORRELATION_ID_MDC_KEY = "correlationId";

    private final SubscriptionChargeService chargeService;
    private final SubscriptionCancellationService cancellationService;
    private final ObjectMapper objectMapper;

    public SubscriptionEventListener(
            SubscriptionChargeService chargeService,
            SubscriptionCancellationService cancellationService,
            ObjectMapper objectMapper
    ) {
        this.chargeService = chargeService;
        this.cancellationService = cancellationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "subscription.events")
    public void onSubscriptionEvent(String value) throws Exception {
        JsonNode envelope = objectMapper.readTree(value);
        String eventType = envelope.get("eventType").asText();

        // correlationId comes from the EVENT, not an incoming HTTP
        // request (there isn't one) - putting it in MDC here is what
        // makes this listener's own logs, and the outbox events written
        // further down, carry the SAME id the customer's original
        // request had. That's the whole payoff of threading
        // correlationId through the envelope: one id ties
        // subscription-service's log line to billing-service's, across
        // two separate processes.
        String correlationId = envelope.get("correlationId").asText();
        MDC.put(CORRELATION_ID_MDC_KEY, correlationId);
        try {
            UUID eventId = UUID.fromString(envelope.get("eventId").asText());
            JsonNode payload = envelope.get("payload");
            UUID subscriptionId = UUID.fromString(payload.get("subscriptionId").asText());
            UUID correlationUuid = UUID.fromString(correlationId);

            switch (eventType) {
                case "SubscriptionCreated" -> chargeService.chargeNewSubscription(
                        eventId, subscriptionId, payload.get("amount").decimalValue(), payload.get("currency").asText(), correlationUuid);
                // The cancellation saga's trigger - see
                // SubscriptionCancellationService's javadoc. Its outcome
                // (RefundIssued/RefundFailed) is what subscription-service's
                // own BillingEventListener uses to either finish the saga
                // or run the compensating transaction.
                case "SubscriptionCancellationRequested" -> cancellationService.refundSubscription(
                        eventId, subscriptionId, payload.get("amount").decimalValue(), payload.get("currency").asText(), correlationUuid);
                default -> log.debug("Ignoring event type not handled by billing-service: {}", eventType);
            }
        } finally {
            MDC.remove(CORRELATION_ID_MDC_KEY);
        }
    }
}
