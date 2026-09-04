package com.sublite.billing.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * subscriptionId is a plain UUID, not a foreign key - and this time
 * that's not a style choice like it was in sublite-core (a same-database
 * FK it deliberately skipped to foreshadow this exact split), it's a
 * hard architectural fact: billing-service has no access to
 * subscription-service's database at all, so there's nothing here a FK
 * COULD reference even if we wanted one.
 */
@Entity
@Table(name = "invoices")
public class Invoice {

    @Id
    private UUID id;

    @Column(name = "subscription_id", nullable = false)
    private UUID subscriptionId;

    @Embedded
    private Money amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private InvoiceStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected Invoice() {
        // JPA
    }

    public Invoice(UUID id, UUID subscriptionId, Money amount, Instant createdAt) {
        this.id = id;
        this.subscriptionId = subscriptionId;
        this.amount = amount;
        this.status = InvoiceStatus.PENDING;
        this.createdAt = createdAt;
    }

    public void markPaid() {
        this.status = InvoiceStatus.PAID;
    }

    public void markFailed() {
        this.status = InvoiceStatus.FAILED;
    }

    public UUID getId() {
        return id;
    }

    public UUID getSubscriptionId() {
        return subscriptionId;
    }

    public Money getAmount() {
        return amount;
    }

    public InvoiceStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
