package com.sublite.billing.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * The real idempotent-consumer mechanism from docs/architecture.md,
 * replacing the narrower stand-in guards used through step 4
 * (InvoiceRepository.findBySubscriptionId here, the PENDING_PAYMENT-only
 * state transitions on subscription-service's side). A row existing for
 * an eventId IS the fact "this exact message was already handled" -
 * checking for it and recording it happen in the SAME @Transactional
 * method as the business effect (SubscriptionChargeService), so a crash
 * between "did the work" and "recorded that we did it" is impossible:
 * either both happened, or neither did, and a redelivery is safe either way.
 */
@Entity
@Table(name = "processed_messages")
public class ProcessedMessage {

    @Id
    private UUID eventId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedMessage() {
        // JPA
    }

    public ProcessedMessage(UUID eventId, Instant processedAt) {
        this.eventId = eventId;
        this.processedAt = processedAt;
    }

    public UUID getEventId() {
        return eventId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
