-- Same dedup mechanism as billing-service's own processed_messages table -
-- see billing-service's V3 migration and com.sublite.subscription.domain.
-- ProcessedMessage for the reasoning. Each service keeps its OWN table:
-- "have I handled this eventId" is local knowledge, not something to
-- share across a database-per-service boundary.
CREATE TABLE processed_messages (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
