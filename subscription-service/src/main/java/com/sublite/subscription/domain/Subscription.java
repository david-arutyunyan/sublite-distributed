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
     * guarded to fire only from PENDING_PAYMENT, which is what makes
     * this safe against a redelivered PaymentSucceeded/PaymentFailed
     * event: applying either one again once the subscription has
     * already left PENDING_PAYMENT is a silent no-op instead of
     * corrupting a later state (e.g. re-activating a since-cancelled
     * subscription). A narrower, state-shape guard than the eventId-
     * keyed dedup table coming in step 5-6 - not a replacement for it,
     * just enough to be correct for the one redelivery scenario that
     * actually exists in this system so far.
     */
    public void activate() {
        if (status == SubscriptionStatus.PENDING_PAYMENT) {
            this.status = SubscriptionStatus.ACTIVE;
        }
    }

    public void enterGracePeriod() {
        if (status == SubscriptionStatus.PENDING_PAYMENT) {
            this.status = SubscriptionStatus.GRACE_PERIOD;
        }
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
