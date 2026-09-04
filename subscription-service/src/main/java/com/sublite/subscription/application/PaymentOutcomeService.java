package com.sublite.subscription.application;

import com.sublite.subscription.domain.ProcessedMessage;
import com.sublite.subscription.domain.Subscription;
import com.sublite.subscription.infrastructure.ProcessedMessageRepository;
import com.sublite.subscription.infrastructure.SubscriptionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/**
 * The other half of the loop SubscriptionPurchaseService started:
 * purchase() leaves a subscription in PENDING_PAYMENT and publishes
 * SubscriptionCreated; this is what finally moves it to ACTIVE or
 * GRACE_PERIOD once billing-service reports back. No outbox write here -
 * unlike purchase() and billing-service's own charge handling, nothing
 * downstream needs to react to "this subscription is now ACTIVE" yet, so
 * there's no fact to publish (that changes the moment something DOES
 * need to react to it - notification-service in step 5-6, say).
 */
@Service
public class PaymentOutcomeService {

    private static final Logger log = LoggerFactory.getLogger(PaymentOutcomeService.class);

    private final SubscriptionRepository subscriptions;
    private final ProcessedMessageRepository processedMessages;
    private final Clock clock;

    public PaymentOutcomeService(SubscriptionRepository subscriptions, ProcessedMessageRepository processedMessages, Clock clock) {
        this.subscriptions = subscriptions;
        this.processedMessages = processedMessages;
        this.clock = clock;
    }

    @Transactional
    public void handlePaymentSucceeded(UUID eventId, UUID subscriptionId) {
        withDedup(eventId, subscriptionId, () -> withSubscription(subscriptionId, subscription -> {
            subscription.activate();
            log.info("Subscription activated after successful payment: subscriptionId={}", subscriptionId);
        }));
    }

    @Transactional
    public void handlePaymentFailed(UUID eventId, UUID subscriptionId, String reason) {
        withDedup(eventId, subscriptionId, () -> withSubscription(subscriptionId, subscription -> {
            subscription.enterGracePeriod();
            log.info("Subscription entered grace period after failed payment: subscriptionId={}, reason={}", subscriptionId, reason);
        }));
    }

    // The real dedup mechanism (see ProcessedMessage's javadoc) -
    // replaces relying solely on activate()/enterGracePeriod()'s own
    // PENDING_PAYMENT-only guard, which stays in place as a second,
    // narrower layer (it also catches things this table doesn't, like
    // two DIFFERENT events somehow both trying to move the same
    // subscription out of PENDING_PAYMENT).
    private void withDedup(UUID eventId, UUID subscriptionId, Runnable action) {
        if (processedMessages.existsById(eventId)) {
            log.info("Skipping already-processed event: eventId={}, subscriptionId={}", eventId, subscriptionId);
            return;
        }
        action.run();
        processedMessages.save(new ProcessedMessage(eventId, Instant.now(clock)));
    }

    private void withSubscription(UUID subscriptionId, java.util.function.Consumer<Subscription> action) {
        subscriptions.findById(subscriptionId).ifPresentOrElse(
                action,
                () -> log.warn("Received a payment outcome for an unknown subscription: subscriptionId={}", subscriptionId)
        );
    }
}
