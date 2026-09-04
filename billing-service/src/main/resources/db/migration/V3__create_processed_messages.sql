-- The real idempotent-consumer dedup table promised in
-- docs/architecture.md's event envelope section. The primary key on
-- event_id IS the "unique index checked before processing" the docs
-- describe - no separate index needed, a PK already gives that.
CREATE TABLE processed_messages (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
