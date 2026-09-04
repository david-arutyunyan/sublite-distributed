package com.sublite.subscription.api.dto;

import com.sublite.subscription.domain.BillingPeriod;
import com.sublite.subscription.domain.Subscription;
import com.sublite.subscription.domain.SubscriptionStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record SubscriptionResponse(
        UUID id,
        UUID customerId,
        String planCode,
        String planName,
        BillingPeriod billingPeriod,
        BigDecimal amount,
        String currency,
        SubscriptionStatus status,
        Instant currentPeriodStart,
        Instant currentPeriodEnd
) {
    public static SubscriptionResponse from(Subscription subscription) {
        var planPrice = subscription.getPlanPrice();
        var plan = planPrice.getPlan();
        return new SubscriptionResponse(
                subscription.getId(),
                subscription.getCustomerId(),
                plan.getCode(),
                plan.getName(),
                planPrice.getBillingPeriod(),
                planPrice.getPrice().amount(),
                planPrice.getPrice().currency(),
                subscription.getStatus(),
                subscription.getCurrentPeriodStart(),
                subscription.getCurrentPeriodEnd()
        );
    }
}
