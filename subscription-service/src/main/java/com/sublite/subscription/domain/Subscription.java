package com.sublite.subscription.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "subscriptions")
public class Subscription {

    @Id
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private UUID customerId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "plan_price_id", nullable = false)
    private PlanPrice planPrice;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private SubscriptionStatus status;

    @Column(name = "current_period_start", nullable = false)
    private Instant currentPeriodStart;

    @Column(name = "current_period_end", nullable = false)
    private Instant currentPeriodEnd;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected Subscription() {
        // JPA
    }

    public Subscription(UUID id, UUID customerId, PlanPrice planPrice, Instant now) {
        this.id = id;
        this.customerId = customerId;
        this.planPrice = planPrice;
        this.status = SubscriptionStatus.PENDING_PAYMENT;
        this.currentPeriodStart = now;
        this.currentPeriodEnd = now.plus(planPrice.getBillingPeriod().approximateDuration());
        this.createdAt = now;
    }

    /**
     * The only two transitions this class supports right now - both
     * guarded to fire only from PENDING_PAYMENT, and both return whether
     * they actually fired. The primary redelivery guard as of step 5-6
     * is PaymentOutcomeService's eventId-keyed processed_messages check;
     * this state guard is a second, narrower layer underneath it -
     * catches a broader class of "this shouldn't happen" (e.g. two
     * different events somehow both trying to move the same subscription
     * out of PENDING_PAYMENT) by making the transition itself a no-op
     * outside the one state it's valid from, not just outside the one
     * eventId it's valid for. The boolean return matters here, not just
     * for symmetry: a caller that skips its own side effects (publishing
     * an event, logging success) when this returns false is what stops a
     * no-op transition from being reported as if it happened - see
     * CancellationService's confirmCancellation()/abortCancellation() for
     * a case where getting this wrong caused a real, live bug.
     */
    public boolean activate() {
        if (status == SubscriptionStatus.PENDING_PAYMENT) {
            this.status = SubscriptionStatus.ACTIVE;
            return true;
        }
        return false;
    }

    public boolean enterGracePeriod() {
        if (status == SubscriptionStatus.PENDING_PAYMENT) {
            this.status = SubscriptionStatus.GRACE_PERIOD;
            return true;
        }
        return false;
    }

    /**
     * Starts the cancellation saga (step 7). Deliberately THROWS instead
     * of silently no-op'ing like every other transition here - this one
     * is called synchronously from an HTTP request
     * (CancellationService.requestCancellation()), not from a Kafka
     * listener. A Kafka listener has to tolerate redelivery by treating
     * an invalid transition as a no-op; an HTTP client cancelling a
     * subscription that's already CANCELLED (or never became ACTIVE)
     * should see an error, not a silent 202 that does nothing.
     */
    public void requestCancellation() {
        if (status != SubscriptionStatus.ACTIVE) {
            throw new InvalidSubscriptionStateException(id, status, "request cancellation");
        }
        this.status = SubscriptionStatus.CANCEL_PENDING;
    }

    /**
     * The saga's happy path, triggered by billing-service's RefundIssued
     * - guarded to CANCEL_PENDING only, same redelivery-safe silent-no-op
     * shape as activate()/enterGracePeriod() (this IS Kafka-listener-
     * triggered, unlike requestCancellation() above). Returns whether it
     * actually fired - see activate()'s javadoc for why that matters.
     */
    public boolean confirmCancellation() {
        if (status == SubscriptionStatus.CANCEL_PENDING) {
            this.status = SubscriptionStatus.CANCELLED;
            return true;
        }
        return false;
    }

    /**
     * The saga's COMPENSATING transaction, triggered by billing-service's
     * RefundFailed: undoes requestCancellation()'s optimistic transition
     * by putting the subscription back exactly where it was. This is
     * what makes the cancellation saga a real saga and not just a
     * one-way event chain - a downstream failure (the refund couldn't be
     * issued) rolls back the upstream state instead of leaving the
     * subscription stuck in CANCEL_PENDING forever. Returns whether it
     * actually fired - see activate()'s javadoc for why that matters.
     */
    public boolean abortCancellation() {
        if (status == SubscriptionStatus.CANCEL_PENDING) {
            this.status = SubscriptionStatus.ACTIVE;
            return true;
        }
        return false;
    }

    public UUID getId() {
        return id;
    }

    public UUID getCustomerId() {
        return customerId;
    }

    public PlanPrice getPlanPrice() {
        return planPrice;
    }

    public SubscriptionStatus getStatus() {
        return status;
    }

    public Instant getCurrentPeriodStart() {
        return currentPeriodStart;
    }

    public Instant getCurrentPeriodEnd() {
        return currentPeriodEnd;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
