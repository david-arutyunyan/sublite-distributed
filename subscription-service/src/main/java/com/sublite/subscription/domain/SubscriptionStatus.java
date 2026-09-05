package com.sublite.subscription.domain;

/**
 * PENDING_PAYMENT is the one real difference from sublite-core's
 * lifecycle: the monolith charges synchronously during purchase() and
 * goes straight to ACTIVE or GRACE_PERIOD. Here, purchase only creates
 * the subscription and publishes SubscriptionCreated (via the outbox) -
 * billing-service (step 4) charges asynchronously and reports back with
 * PaymentSucceeded/PaymentFailed, which is what actually moves a
 * subscription out of PENDING_PAYMENT. Until billing-service exists and
 * consumes that event, every subscription created here just sits in
 * PENDING_PAYMENT - expected for this step, not a bug.
 */
public enum SubscriptionStatus {
    PENDING_PAYMENT,
    ACTIVE,
    GRACE_PERIOD,
    // The cancellation saga's own in-flight state (step 7) - a
    // subscription sits here between "customer asked to cancel" and
    // "billing-service confirmed the refund", the same way
    // PENDING_PAYMENT sits between purchase and the charge outcome.
    // CANCEL_PENDING, not PENDING_CANCELLATION/CANCELLATION_PENDING -
    // those are 21 chars, one over this column's VARCHAR(20).
    CANCEL_PENDING,
    CANCELLED
}
