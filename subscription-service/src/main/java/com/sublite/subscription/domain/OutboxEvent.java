package com.sublite.subscription.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * The whole point of this table: writing a row here happens in the SAME
 * local database transaction as the business write it describes (see
 * SubscriptionPurchaseService.purchase()) - Postgres's own ACID
 * guarantees make "the subscription exists" and "there's a fact recorded
 * that needs publishing" atomic, with no distributed transaction across
 * Postgres and Kafka needed. Without this table, the natural-looking
 * alternative - save the subscription, then call kafkaTemplate.send()
 * right after, in the same method - has a real gap: if the process
 * crashes (or Kafka is briefly unreachable) between those two calls, the
 * subscription exists but nothing downstream ever finds out. This table
 * is what makes "publish it" retryable indefinitely and safe to retry:
 * the row just sits here, unpublished, until OutboxPoller successfully
 * sends it.
 *
 * `id` doubles as the event envelope's `eventId` (see docs/architecture.md)
 * - the value a downstream consumer dedups on - so there's no separate
 * column for it. `payload` carries only the event-specific fields; the
 * poller builds the rest of the envelope (eventType/aggregateId/
 * occurredAt/correlationId) from this row's own columns when it publishes.
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false, length = 50)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> payload;

    @Column(name = "correlation_id", nullable = false)
    private UUID correlationId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEvent() {
        // JPA
    }

    public OutboxEvent(
            UUID id,
            String aggregateType,
            UUID aggregateId,
            String eventType,
            Map<String, Object> payload,
            UUID correlationId,
            Instant createdAt
    ) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.correlationId = correlationId;
        this.createdAt = createdAt;
    }

    public void markPublished(Instant now) {
        this.publishedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
