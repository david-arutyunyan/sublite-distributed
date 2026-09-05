package com.sublite.billing.infrastructure;

import com.sublite.billing.domain.ChargeResult;
import com.sublite.billing.domain.Money;
import com.sublite.billing.domain.PaymentGateway;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Stands in for a real provider, same idea as sublite-core's own
 * RandomPaymentGateway - simplified here (no idempotency-key-keyed
 * memoization) because the outbox+dedup story for THIS project lives one
 * layer up, in how billing-service consumes subscription.events, not in
 * the gateway itself.
 */
@Component
public class RandomPaymentGateway implements PaymentGateway {

    private static final double DECLINE_RATE = 0.2;

    @Override
    public ChargeResult charge(Money amount) {
        return outcome("INSUFFICIENT_FUNDS");
    }

    @Override
    public ChargeResult refund(Money amount) {
        // A different decline reason than charge()'s - "insufficient
        // funds" doesn't make sense for a refund. Same DECLINE_RATE,
        // a real provider would likely have a different failure profile
        // for refunds vs. charges, but a second knob here would be
        // tuning a fake for realism it doesn't need.
        return outcome("REFUND_PROVIDER_ERROR");
    }

    private static ChargeResult outcome(String declineReason) {
        if (ThreadLocalRandom.current().nextDouble() < DECLINE_RATE) {
            return new ChargeResult.Declined(declineReason);
        }
        return new ChargeResult.Success(java.util.UUID.randomUUID().toString());
    }
}
