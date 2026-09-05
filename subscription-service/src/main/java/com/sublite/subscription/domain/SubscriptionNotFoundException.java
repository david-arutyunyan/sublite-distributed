package com.sublite.subscription.domain;

import java.util.UUID;

public class SubscriptionNotFoundException extends RuntimeException {

    public SubscriptionNotFoundException(UUID subscriptionId) {
        super("No subscription with id " + subscriptionId);
    }
}
