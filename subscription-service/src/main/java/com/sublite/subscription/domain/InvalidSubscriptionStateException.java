package com.sublite.subscription.domain;

import java.util.UUID;

public class InvalidSubscriptionStateException extends RuntimeException {

    public InvalidSubscriptionStateException(UUID subscriptionId, SubscriptionStatus currentStatus, String attemptedAction) {
        super("Cannot %s for subscription %s - current status is %s".formatted(attemptedAction, subscriptionId, currentStatus));
    }
}
