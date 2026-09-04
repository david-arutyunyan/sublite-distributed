package com.sublite.subscription.application;

import com.sublite.subscription.domain.Subscription;
import com.sublite.subscription.infrastructure.SubscriptionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    public PaymentOutcomeService(SubscriptionRepository subscriptions) {
        this.subscriptions = subscriptions;
    }

    @Transactional
    public void handlePaymentSucceeded(UUID subscriptionId) {
        withSubscription(subscriptionId, subscription -> {
            subscription.activate();
            log.info("Subscription activated after successful payment: subscriptionId={}", subscriptionId);
        });
    }

    @Transactional
    public void handlePaymentFailed(UUID subscriptionId, String reason) {
        withSubscription(subscriptionId, subscription -> {
            subscription.enterGracePeriod();
            log.info("Subscription entered grace period after failed payment: subscriptionId={}, reason={}", subscriptionId, reason);
        });
    }

    private void withSubscription(UUID subscriptionId, java.util.function.Consumer<Subscription> action) {
        subscriptions.findById(subscriptionId).ifPresentOrElse(
                action,
                () -> log.warn("Received a payment outcome for an unknown subscription: subscriptionId={}", subscriptionId)
        );
    }
}
