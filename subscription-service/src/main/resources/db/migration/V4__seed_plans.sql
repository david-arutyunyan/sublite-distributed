-- Fixed ids, not random - so README/curl examples and manual testing can
-- reference a plan price id without first querying for one. No admin
-- CRUD in this rebuild (see Plan.java's javadoc), this is the only way
-- plans get created.
INSERT INTO plans (id, code, name) VALUES
    ('11111111-1111-1111-1111-111111111111', 'sublite-plus', 'Sublite Plus'),
    ('22222222-2222-2222-2222-222222222222', 'sublite-premium', 'Sublite Premium');

INSERT INTO plan_prices (id, plan_id, billing_period, amount, currency) VALUES
    ('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111', 'MONTHLY', 9.99, 'USD'),
    ('44444444-4444-4444-4444-444444444444', '22222222-2222-2222-2222-222222222222', 'YEARLY', 99.00, 'USD');
