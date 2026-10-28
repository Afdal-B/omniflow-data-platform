CREATE SCHEMA ops;

CREATE SCHEMA raw;

CREATE TABLE ops.pipeline_runs (
    pipeline text NOT NULL,
    batch_id text NOT NULL,
    status text NOT NULL,
    ingest_status text NOT NULL,
    load_status text NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    duration_s double precision,
    mode text,
    rows_read bigint,
    rows_valid bigint,
    rows_rejected bigint,
    dlq_rate double precision,
    rejection_reasons jsonb DEFAULT '{}'::jsonb NOT NULL,
    rows_loaded bigint,
    error text,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE raw.events_clickstream (
    event_id text,
    event_type text,
    event_ts timestamp with time zone,
    anonymous_id text,
    customer_id bigint,
    page_url text,
    product_id bigint,
    order_id bigint,
    device text,
    country_code text,
    _kafka_partition integer,
    _kafka_offset bigint,
    _kafka_ts timestamp with time zone,
    _batch_id text,
    _ingested_at timestamp with time zone,
    _source text
);

CREATE TABLE raw.events_clickstream_traffic_5min (
    window_start timestamp with time zone,
    window_end timestamp with time zone,
    event_type text,
    device text,
    event_count bigint,
    unique_anonymous_id bigint,
    _batch_id text,
    _ingested_at timestamp with time zone,
    _source text
);

CREATE TABLE raw.oltp_customers (
    customer_id bigint,
    email text,
    first_name text,
    last_name text,
    country_code text,
    segment text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    _batch_id text,
    _ingested_at timestamp with time zone,
    _source text
);

CREATE TABLE raw.oltp_order_items (
    order_item_id bigint,
    order_id bigint,
    product_id bigint,
    quantity integer,
    unit_price numeric(10,2),
    discount_amount numeric(10,2),
    updated_at timestamp with time zone,
    _batch_id text,
    _ingested_at timestamp with time zone,
    _source text
);

CREATE TABLE raw.oltp_orders (
    order_id bigint,
    customer_id bigint,
    status text,
    channel text,
    currency text,
    order_ts timestamp with time zone,
    updated_at timestamp with time zone,
    _batch_id text,
    _ingested_at timestamp with time zone,
    _source text
);

CREATE TABLE raw.oltp_products (
    product_id bigint,
    sku text,
    product_name text,
    category text,
    unit_price numeric(10,2),
    is_active boolean,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    _batch_id text,
    _ingested_at timestamp with time zone,
    _source text
);

ALTER TABLE ONLY ops.pipeline_runs
    ADD CONSTRAINT pipeline_runs_pkey PRIMARY KEY (pipeline, batch_id);

INSERT INTO raw.oltp_customers VALUES
    (1, 'Alice.Martin@example.com ', 'alice', 'martin', 'fr', 'consumer', '2026-01-10 09:00+00', '2026-01-10 09:00+00', '20260301T100000Z', '2026-03-01 10:00+00', 'oltp.shop.customers'),
    (2, 'bob.diallo@example.com', 'Bob', 'Diallo', 'BE', 'business', '2026-02-01 09:00+00', '2026-02-01 09:00+00', '20260301T100000Z', '2026-03-01 10:00+00', 'oltp.shop.customers'),
    (1, 'alice.martin@example.com', 'Alice', 'Martin', 'FR', 'business', '2026-01-10 09:00+00', '2026-03-01 11:00+00', '20260301T120000Z', '2026-03-01 12:00+00', 'oltp.shop.customers');

INSERT INTO raw.oltp_products VALUES
    (10, 'sku-00010', 'Premium Headphones', 'Electronics', 120.00, true, '2025-09-01 00:00+00', '2025-09-01 00:00+00', '20260301T100000Z', '2026-03-01 10:00+00', 'oltp.shop.products'),
    (11, 'SKU-00011', 'Eco Mug', 'home', 15.50, true, '2025-09-01 00:00+00', '2025-09-01 00:00+00', '20260301T100000Z', '2026-03-01 10:00+00', 'oltp.shop.products');

INSERT INTO raw.oltp_orders VALUES
    (100, 1, 'pending', 'web', 'eur', '2026-03-01 10:30+00', '2026-03-01 10:30+00', '20260301T110000Z', '2026-03-01 11:00+00', 'oltp.shop.orders'),
    (100, 1, 'paid', 'web', 'EUR', '2026-03-01 10:30+00', '2026-03-01 10:45+00', '20260301T120000Z', '2026-03-01 12:00+00', 'oltp.shop.orders'),
    (101, 2, 'cancelled', 'store', 'EUR', '2026-03-02 15:00+00', '2026-03-02 16:00+00', '20260302T170000Z', '2026-03-02 17:00+00', 'oltp.shop.orders');

INSERT INTO raw.oltp_order_items VALUES
    (1000, 100, 10, 1, 120.00, 12.00, '2026-03-01 10:30+00', '20260301T110000Z', '2026-03-01 11:00+00', 'oltp.shop.order_items'),
    (1001, 100, 11, 2, 15.50, 0.00, '2026-03-01 10:30+00', '20260301T110000Z', '2026-03-01 11:00+00', 'oltp.shop.order_items'),
    (1002, 101, 11, 1, 15.50, 0.00, '2026-03-02 15:00+00', '20260302T170000Z', '2026-03-02 17:00+00', 'oltp.shop.order_items');

INSERT INTO raw.events_clickstream VALUES
    ('11111111-1111-4111-8111-111111111111', 'page_view', '2026-03-01 10:00+00', 'anon-1', NULL, '/', NULL, NULL, 'desktop', 'FR', 0, 1, '2026-03-01 10:00+00', '20260301T101500Z', '2026-03-01 10:15+00', 'events.shop.clickstream.v1'),
    ('22222222-2222-4222-8222-222222222222', 'product_view', '2026-03-01 10:05+00', 'anon-1', NULL, '/products/10', 10, NULL, 'desktop', 'FR', 0, 2, '2026-03-01 10:05+00', '20260301T101500Z', '2026-03-01 10:15+00', 'events.shop.clickstream.v1'),
    ('33333333-3333-4333-8333-333333333333', 'purchase', '2026-03-01 10:30+00', 'anon-1', 1, '/checkout/confirmation', NULL, 100, 'desktop', 'FR', 0, 3, '2026-03-01 10:30+00', '20260301T104500Z', '2026-03-01 10:45+00', 'events.shop.clickstream.v1'),
    ('33333333-3333-4333-8333-333333333333', 'purchase', '2026-03-01 10:30+00', 'anon-1', 1, '/checkout/confirmation', NULL, 100, 'desktop', 'FR', 0, 3, '2026-03-01 10:30+00', '20260301T110000Z', '2026-03-01 11:00+00', 'events.shop.clickstream.v1'),
    ('44444444-4444-4444-8444-444444444444', 'page_view', '2026-03-01 12:00+00', 'anon-1', NULL, '/', NULL, NULL, 'desktop', 'FR', 1, 7, '2026-03-01 12:00+00', '20260301T121500Z', '2026-03-01 12:15+00', 'events.shop.clickstream.v1');

INSERT INTO raw.events_clickstream_traffic_5min VALUES
    ('2026-03-01 10:00+00', '2026-03-01 10:05+00', 'page_view', 'desktop', 42, 30, '20260301T101500Z', '2026-03-01 10:15+00', 'events.shop.clickstream.v1'),
    ('2026-03-01 10:05+00', '2026-03-01 10:10+00', 'page_view', 'mobile', 57, 41, '20260301T101500Z', '2026-03-01 10:15+00', 'events.shop.clickstream.v1');

INSERT INTO ops.pipeline_runs
    (pipeline, batch_id, status, ingest_status, load_status, started_at, finished_at, duration_s,
     mode, rows_read, rows_valid, rows_rejected, dlq_rate, rejection_reasons, rows_loaded, error)
VALUES
    ('clickstream_events', '20260301T101500Z', 'success', 'success', 'success', '2026-03-01 10:15+00',
     '2026-03-01 10:16+00', 61.2, 'stream (available now)', 1000, 970, 30, 0.03,
     '{"accepted_values:event_type": 12, "malformed_payload": 18}', 970, NULL),
    ('oltp_orders', '20260301T110000Z', 'failed', 'failed', 'skipped', '2026-03-01 11:00+00',
     '2026-03-01 11:00:20+00', 20.0, NULL, NULL, NULL, NULL, NULL, '{}', NULL,
     'ConnectionError: connection refused');
