package com.sublite.billing.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payment_attempts")
public class PaymentAttempt {

    @Id
    private UUID id;

    @Column(name = "invoice_id", nullable = false)
    private UUID invoiceId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentAttemptStatus status;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "attempted_at", nullable = false)
    private Instant attemptedAt;

    protected PaymentAttempt() {
        // JPA
    }

    public PaymentAttempt(UUID id, UUID invoiceId, PaymentAttemptStatus status, String failureReason, Instant attemptedAt) {
        this.id = id;
        this.invoiceId = invoiceId;
        this.status = status;
        this.failureReason = failureReason;
        this.attemptedAt = attemptedAt;
    }

    public boolean succeeded() {
        return status == PaymentAttemptStatus.SUCCEEDED;
    }

    public UUID getId() {
        return id;
    }

    public UUID getInvoiceId() {
        return invoiceId;
    }

    public PaymentAttemptStatus getStatus() {
        return status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public Instant getAttemptedAt() {
        return attemptedAt;
    }
}
