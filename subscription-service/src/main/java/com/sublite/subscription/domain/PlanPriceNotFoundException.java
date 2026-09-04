package com.sublite.subscription.domain;

import java.util.UUID;

public class PlanPriceNotFoundException extends RuntimeException {

    public PlanPriceNotFoundException(UUID planPriceId) {
        super("No plan price with id " + planPriceId);
    }
}
