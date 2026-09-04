package com.sublite.subscription.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * The real idempotent-consumer mechanism from docs/architecture.md - see
 * billing-service's own ProcessedMessage for the full reasoning (same
 * design, mirrored on this side of the wire). A row existing for an
 * eventId IS the fact "this exact billing.events message was already
 * handled"; checking for it and recording it happen in the same
 * @Transactional method as applying the outcome to the Subscription
 * (PaymentOutcomeService), so the two can't drift apart.
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
