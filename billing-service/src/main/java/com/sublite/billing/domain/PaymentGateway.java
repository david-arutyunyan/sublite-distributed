package com.sublite.billing.domain;

public interface PaymentGateway {
    ChargeResult charge(Money amount);
}
