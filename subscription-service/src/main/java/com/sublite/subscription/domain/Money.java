package com.sublite.subscription.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.math.BigDecimal;

@Embeddable
public record Money(
        @Column(name = "amount", nullable = false, precision = 10, scale = 2) BigDecimal amount,
        @Column(name = "currency", nullable = false, length = 3) String currency
) {
}
