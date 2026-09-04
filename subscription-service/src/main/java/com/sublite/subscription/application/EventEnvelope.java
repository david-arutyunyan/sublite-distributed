package com.sublite.subscription.application;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * The wire format from docs/architecture.md - every event this project
 * publishes, from every service, is shaped like this. `eventId` is what a
 * consumer dedups on (see the message_id requirement in the project
 * brief); `correlationId` ties a whole saga together across services,
 * the same idea as sublite-core's CorrelationIdFilter, just spanning
 * process boundaries now instead of one request's log lines.
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
