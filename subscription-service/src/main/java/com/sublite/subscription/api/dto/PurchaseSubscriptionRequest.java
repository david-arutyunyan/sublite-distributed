package com.sublite.subscription.api.dto;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

/**
 * No auth in this rebuild - customerId comes straight from the request
 * body instead of a JWT subject. Deliberate scope cut: this project's
 * point is Kafka/Saga/K8s, and re-deriving sublite-core's whole JWT
 * setup here wouldn't teach anything new. A real system wouldn't trust a
 * client-supplied customerId like this.
 */
public record PurchaseSubscriptionRequest(
        @NotNull UUID customerId,
        @NotNull UUID planPriceId
) {
}
