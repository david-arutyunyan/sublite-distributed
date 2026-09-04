package com.sublite.subscription.domain;

import java.time.Duration;

public enum BillingPeriod {
    MONTHLY,
    YEARLY;

    public Duration approximateDuration() {
        return switch (this) {
            case MONTHLY -> Duration.ofDays(30);
            case YEARLY -> Duration.ofDays(365);
        };
    }
}
