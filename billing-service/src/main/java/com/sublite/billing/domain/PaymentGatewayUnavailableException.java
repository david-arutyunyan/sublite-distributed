package com.sublite.billing.domain;

/**
 * A TECHNICAL failure talking to the payment gateway (timeout, connection
 * refused, 5xx) - distinct from ChargeResult.Declined, which is a normal
 * BUSINESS outcome (insufficient funds, expired card) and not an error at
 * all. This distinction is what ResilientPaymentGateway's retry/circuit
 * breaker actually key off: retrying or circuit-breaking a business
 * decline would be nonsensical (the card isn't suddenly going to have
 * funds on attempt 2), but retrying a transient technical failure - and
 * circuit-breaking a persistent one - is exactly the right response.
 */
public class PaymentGatewayUnavailableException extends RuntimeException {

    public PaymentGatewayUnavailableException(String message) {
        super(message);
    }

    public PaymentGatewayUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
