package com.sublite.subscription.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * The other end of the loop billing-service's own SubscriptionEventListener
 * started. Consumer group "subscription-service" (see KafkaConsumerConfig)
 * - independent of billing-service's "billing-service" group on the same
 * topic, so each service gets the full billing.events stream.
 *
 * billing.events carries more event types than the four handled below
 * (see the catalog in docs/architecture.md) - anything else is quietly
 * ignored, same forward-compatibility reasoning as its billing-side
 * counterpart.
 */
@Component
public class BillingEventListener {

    private static final Logger log = LoggerFactory.getLogger(BillingEventListener.class);
    private static final String CORRELATION_ID_MDC_KEY = "correlationId";

    private final PaymentOutcomeService paymentOutcomeService;
    private final CancellationService cancellationService;
    private final ObjectMapper objectMapper;

    public BillingEventListener(
            PaymentOutcomeService paymentOutcomeService,
            CancellationService cancellationService,
            ObjectMapper objectMapper
    ) {
        this.paymentOutcomeService = paymentOutcomeService;
        this.cancellationService = cancellationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "billing.events")
    public void onBillingEvent(String value) throws Exception {
        JsonNode envelope = objectMapper.readTree(value);
        String eventType = envelope.get("eventType").asText();
        String correlationId = envelope.get("correlationId").asText();

        MDC.put(CORRELATION_ID_MDC_KEY, correlationId);
        try {
            UUID eventId = UUID.fromString(envelope.get("eventId").asText());
            UUID correlationUuid = UUID.fromString(correlationId);
            JsonNode payload = envelope.get("payload");
            UUID subscriptionId = UUID.fromString(payload.get("subscriptionId").asText());

            switch (eventType) {
                case "PaymentSucceeded" -> paymentOutcomeService.handlePaymentSucceeded(eventId, subscriptionId);
                case "PaymentFailed" -> paymentOutcomeService.handlePaymentFailed(
                        eventId, subscriptionId, payload.get("reason").asText());
                // The cancellation saga's other half - see CancellationService's
                // javadoc. RefundFailed drives the COMPENSATING transaction
                // (abortCancellation(), reverting back to ACTIVE), not just
                // another forward step.
                case "RefundIssued" -> cancellationService.confirmCancellation(eventId, subscriptionId, correlationUuid);
                case "RefundFailed" -> cancellationService.abortCancellation(
                        eventId, subscriptionId, payload.get("reason").asText(), correlationUuid);
                default -> log.debug("Ignoring event type not handled by subscription-service: {}", eventType);
            }
        } finally {
            MDC.remove(CORRELATION_ID_MDC_KEY);
        }
    }
}
