CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ
);

-- OutboxPoller's whole query (WHERE published_at IS NULL ORDER BY
-- created_at) - a partial index (only unpublished rows) instead of a
-- plain one on published_at: the table is meant to stay mostly-published
-- forever, so indexing every row for a WHERE clause that only ever
-- matches a small pending tail wastes space and write throughput for no
-- read benefit.
CREATE INDEX idx_outbox_events_unpublished ON outbox_events (created_at)
    WHERE published_at IS NULL;
