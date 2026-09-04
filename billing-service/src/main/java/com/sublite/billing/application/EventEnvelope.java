package com.sublite.billing.application;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Identical shape to subscription-service's EventEnvelope (see its
 * javadoc and docs/architecture.md) - every service in this project
 * publishes and consumes the same wire format. Duplicated rather than
 * shared through a library on purpose at this project's size: pulling in
 * a shared module now would mean coordinating a release/version across
 * services for a five-field record, more process than the record is
 * worth. Worth reconsidering if a third or fourth copy starts drifting.
 */
public record EventEnvelope(
        UUID eventId,
        String eventType,
        UUID aggregateId,
        Instant occurredAt,
        UUID correlationId,
        Map<String, Object> payload
) {
}
