package com.sublite.subscription.application;

import com.sublite.subscription.domain.OutboxEvent;
import com.sublite.subscription.domain.ProcessedMessage;
import com.sublite.subscription.domain.Subscription;
import com.sublite.subscription.domain.SubscriptionNotFoundException;
import com.sublite.subscription.infrastructure.OutboxEventRepository;
import com.sublite.subscription.infrastructure.ProcessedMessageRepository;
import com.sublite.subscription.infrastructure.SubscriptionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The cancellation saga's subscription-service half (step 7) - a
 * choreographed saga with a real compensating transaction, not just a
 * one-way event chain:
 *
 *   requestCancellation()  ACTIVE -> CANCEL_PENDING, publishes
 *                          SubscriptionCancellationRequested
 *   billing-service        consumes it, tries to refund, publishes
 *                          RefundIssued or RefundFailed
 *   confirmCancellation()  (on RefundIssued) CANCEL_PENDING -> CANCELLED -
 *                          the forward path completing
 *   abortCancellation()    (on RefundFailed) CANCEL_PENDING -> ACTIVE -
 *                          the COMPENSATION: undoes requestCancellation()'s
 *                          optimistic transition because the step that
 *                          was supposed to follow it didn't succeed
 *
 * Refund amount is the full plan price, not prorated by remaining period -
 * a deliberate simplification (proration math is a distraction from the
 * saga/compensation pattern this step is actually about).
 */
@Service
public class CancellationService {

    private static final Logger log = LoggerFactory.getLogger(CancellationService.class);
    private static final String SUBSCRIPTION_AGGREGATE = "Subscription";
    private static final String CORRELATION_ID_MDC_KEY = "correlationId";

    private final SubscriptionRepository subscriptions;
    private final ProcessedMessageRepository processedMessages;
    private final OutboxEventRepository outbox;
    private final Clock clock;

    public CancellationService(
            SubscriptionRepository subscriptions,
            ProcessedMessageRepository processedMessages,
            OutboxEventRepository outbox,
            Clock clock
    ) {
        this.subscriptions = subscriptions;
        this.processedMessages = processedMessages;
        this.outbox = outbox;
        this.clock = clock;
    }

    @Transactional
    public Subscription requestCancellation(UUID subscriptionId) {
        // findByIdWithPlan, not plain findById: this method returns the
        // Subscription straight back to the controller, which reads
        // planPrice.getPlan() to build the response AFTER this
        // transaction has closed - see the repository method's own
        // comment for the LazyInitializationException that causes.
        Subscription subscription = subscriptions.findByIdWithPlan(subscriptionId)
                .orElseThrow(() -> new SubscriptionNotFoundException(subscriptionId));

        subscription.requestCancellation();

        UUID correlationId = currentCorrelationId();
        Instant now = Instant.now(clock);
        outbox.save(new OutboxEvent(
                UUID.randomUUID(),
                SUBSCRIPTION_AGGREGATE,
                subscriptionId,
                "SubscriptionCancellationRequested",
                requestedPayload(subscription),
                correlationId,
                now
        ));

        log.info("Cancellation requested: subscriptionId={}, correlationId={}", subscriptionId, correlationId);
        return subscription;
    }

    @Transactional
    public void confirmCancellation(UUID eventId, UUID subscriptionId, UUID correlationId) {
        if (processedMessages.existsById(eventId)) {
            log.info("Skipping already-processed event: eventId={}, subscriptionId={}", eventId, subscriptionId);
            return;
        }

        Instant now = Instant.now(clock);
        subscriptions.findById(subscriptionId).ifPresentOrElse(subscription -> {
            // The boolean matters here, not just the entity's own guard:
            // a stray or racing RefundIssued arriving after the
            // subscription already left CANCEL_PENDING (confirmed or
            // compensated by a different message) must NOT re-publish
            // SubscriptionCancelled - the entity's internal guard already
            // makes the STATE change a no-op, but without checking the
            // return value here, this method would still publish a
            // second, spurious "cancelled" fact and log a false success.
            // A live race (two RefundFailed messages for one saga)
            // caught this exact bug in abortCancellation() below before
            // this method got the same fix applied proactively.
            if (subscription.confirmCancellation()) {
                outbox.save(new OutboxEvent(
                        UUID.randomUUID(),
                        SUBSCRIPTION_AGGREGATE,
                        subscriptionId,
                        "SubscriptionCancelled",
                        terminalPayload(subscription),
                        correlationId,
                        now
                ));
                log.info("Cancellation confirmed: subscriptionId={}, correlationId={}", subscriptionId, correlationId);
            } else {
                log.info("Ignoring RefundIssued - subscription not in CANCEL_PENDING: subscriptionId={}, currentStatus={}",
                        subscriptionId, subscription.getStatus());
            }
        }, () -> log.warn("Received RefundIssued for an unknown subscription: subscriptionId={}", subscriptionId));

        processedMessages.save(new ProcessedMessage(eventId, now));
    }

    @Transactional
    public void abortCancellation(UUID eventId, UUID subscriptionId, String reason, UUID correlationId) {
        if (processedMessages.existsById(eventId)) {
            log.info("Skipping already-processed event: eventId={}, subscriptionId={}", eventId, subscriptionId);
            return;
        }

        Instant now = Instant.now(clock);
        subscriptions.findById(subscriptionId).ifPresentOrElse(subscription -> {
            // Real bug this exact check fixed: caught live when two
            // RefundFailed messages raced for the same saga (a manually
            // injected one and billing-service's own real response, each
            // with its own eventId so the eventId-dedup above correctly
            // let both through - they're genuinely different events, not
            // a redelivery of one). Without this check, BOTH calls
            // published their own SubscriptionCancellationFailed and
            // logged "compensating transaction", even though the second
            // one's abortCancellation() was a pure no-op (already ACTIVE).
            if (subscription.abortCancellation()) {
                outbox.save(new OutboxEvent(
                        UUID.randomUUID(),
                        SUBSCRIPTION_AGGREGATE,
                        subscriptionId,
                        "SubscriptionCancellationFailed",
                        failurePayload(subscription, reason),
                        correlationId,
                        now
                ));
                log.warn("Cancellation FAILED, reverted to ACTIVE (compensating transaction): subscriptionId={}, reason={}, correlationId={}",
                        subscriptionId, reason, correlationId);
            } else {
                log.info("Ignoring RefundFailed - subscription not in CANCEL_PENDING: subscriptionId={}, currentStatus={}",
                        subscriptionId, subscription.getStatus());
            }
        }, () -> log.warn("Received RefundFailed for an unknown subscription: subscriptionId={}", subscriptionId));

        processedMessages.save(new ProcessedMessage(eventId, now));
    }

    private static Map<String, Object> requestedPayload(Subscription subscription) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("subscriptionId", subscription.getId().toString());
        payload.put("customerId", subscription.getCustomerId().toString());
        payload.put("amount", subscription.getPlanPrice().getPrice().amount());
        payload.put("currency", subscription.getPlanPrice().getPrice().currency());
        return payload;
    }

    private static Map<String, Object> terminalPayload(Subscription subscription) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("subscriptionId", subscription.getId().toString());
        payload.put("customerId", subscription.getCustomerId().toString());
        return payload;
    }

    private static Map<String, Object> failurePayload(Subscription subscription, String reason) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("subscriptionId", subscription.getId().toString());
        payload.put("customerId", subscription.getCustomerId().toString());
        payload.put("reason", reason);
        return payload;
    }

    /**
     * Same fallback as SubscriptionPurchaseService's own helper - see its
     * javadoc. Duplicated rather than shared: two call sites within one
     * service is well short of where extracting a utility pays for itself.
     */
    private static UUID currentCorrelationId() {
        String existing = MDC.get(CORRELATION_ID_MDC_KEY);
        if (existing != null) {
            try {
                return UUID.fromString(existing);
            } catch (IllegalArgumentException notAUuid) {
                // falls through to a fresh id
            }
        }
        return UUID.randomUUID();
    }
}
