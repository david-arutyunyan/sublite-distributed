CREATE TABLE invoices (
    id UUID PRIMARY KEY,
    subscription_id UUID NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_invoices_subscription_id ON invoices (subscription_id);

CREATE TABLE payment_attempts (
    id UUID PRIMARY KEY,
    invoice_id UUID NOT NULL REFERENCES invoices (id),
    status VARCHAR(20) NOT NULL,
    failure_reason TEXT,
    attempted_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_payment_attempts_invoice_id ON payment_attempts (invoice_id);
