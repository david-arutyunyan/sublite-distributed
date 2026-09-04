package com.sublite.notification.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * A local, read-only projection of exactly the one fact this service
 * needs from subscription-service: which customer a subscription belongs
 * to. Built entirely from SubscriptionCreated events, not a query against
 * subscription-service's own database (there's no shared access to begin
 * with - database-per-service) and not a synchronous call to it either
 * (that would reintroduce the coupling the whole rewrite is meant to
 * remove, and would fail this service's own event processing if
 * subscription-service happened to be down).
 *
 * Why this is needed at all: billing.events is keyed and scoped by
 * subscriptionId only (billing-service doesn't own customer identity
 * either), so PaymentSucceeded/PaymentFailed carry no customerId of
 * their own. This projection is what lets a payment-outcome notification
 * still say who it's for.
 */
@Document(collection = "subscription_projections")
public class SubscriptionProjection {

    @Id
    private String subscriptionId;

    private String customerId;

    protected SubscriptionProjection() {
        // Spring Data
    }

    public SubscriptionProjection(String subscriptionId, String customerId) {
        this.subscriptionId = subscriptionId;
        this.customerId = customerId;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public String getCustomerId() {
        return customerId;
    }
}
