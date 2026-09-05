CREATE TABLE refunds (
    id UUID PRIMARY KEY,
    subscription_id UUID NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(20) NOT NULL,
    failure_reason VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_refunds_subscription_id ON refunds (subscription_id);
