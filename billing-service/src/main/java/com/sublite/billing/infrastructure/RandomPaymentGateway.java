package com.sublite.billing.infrastructure;

import com.sublite.billing.domain.ChargeResult;
import com.sublite.billing.domain.Money;
import com.sublite.billing.domain.PaymentGateway;
import com.sublite.billing.domain.PaymentGatewayUnavailableException;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Stands in for a real provider, same idea as sublite-core's own
 * RandomPaymentGateway - simplified here (no idempotency-key-keyed
 * memoization) because the outbox+dedup story for THIS project lives one
 * layer up, in how billing-service consumes subscription.events, not in
 * the gateway itself.
 *
 * Not a @Component (step 8) - ResilientPaymentGateway is the only
 * Spring-managed PaymentGateway now, and constructs this directly as a
 * plain collaborator. Two independent failure axes, not one: a business
 * DECLINE (a normal, expected outcome real money-movement code has to
 * handle regardless of retries) and a TECHNICAL failure (the gateway
 * itself is unreachable/erroring - the thing retry/circuit-breaking
 * actually exists for). Conflating them into one "chance of failure"
 * knob would make the wrapper meaningless: retrying a declined card
 * doesn't make it un-declined.
 */
public class RandomPaymentGateway implements PaymentGateway {

    private static final double DECLINE_RATE = 0.2;
    private static final double TECHNICAL_FAILURE_RATE = 0.1;

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
        double roll = ThreadLocalRandom.current().nextDouble();
        if (roll < TECHNICAL_FAILURE_RATE) {
            throw new PaymentGatewayUnavailableException("simulated payment gateway timeout");
        }
        if (roll < TECHNICAL_FAILURE_RATE + DECLINE_RATE) {
            return new ChargeResult.Declined(declineReason);
        }
        return new ChargeResult.Success(java.util.UUID.randomUUID().toString());
    }
}
