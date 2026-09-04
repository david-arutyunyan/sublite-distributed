package com.sublite.billing.domain;

/**
 * Same shape as sublite-core's own ChargeResult - a sealed interface
 * over the outcomes a real payment provider can return, standing in for
 * one since there's no real provider to call.
 */
public sealed interface ChargeResult {
    record Success(String providerReference) implements ChargeResult {}
    record Declined(String reason) implements ChargeResult {}
}
