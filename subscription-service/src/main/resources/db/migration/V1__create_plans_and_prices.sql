-- One database per service (not one Postgres instance with per-service
-- schemas, like the monolith) - no schema prefix needed, `public` is
-- this service's own database entirely.
CREATE TABLE plans (
    id UUID PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name TEXT NOT NULL
);

CREATE TABLE plan_prices (
    id UUID PRIMARY KEY,
    plan_id UUID NOT NULL REFERENCES plans (id),
    billing_period VARCHAR(10) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL
);

CREATE INDEX idx_plan_prices_plan_id ON plan_prices (plan_id);
