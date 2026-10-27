-- Operational database of the fictional shop: the batch source of the platform.
-- Deliberately "realistic": foreign keys are enforced, but business rules are not
-- (no CHECK constraints), which is exactly why the ingestion has a quality gate.

CREATE SCHEMA IF NOT EXISTS shop;

CREATE OR REPLACE FUNCTION shop.touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE shop.customers (
    customer_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email         text        NOT NULL,
    first_name    text        NOT NULL,
    last_name     text        NOT NULL,
    country_code  text        NOT NULL,
    segment       text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shop.products (
    product_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku           text          NOT NULL UNIQUE,
    product_name  text          NOT NULL,
    category      text          NOT NULL,
    unit_price    numeric(10,2) NOT NULL,
    is_active     boolean       NOT NULL DEFAULT true,
    created_at    timestamptz   NOT NULL DEFAULT now(),
    updated_at    timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE shop.orders (
    order_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id   bigint      NOT NULL REFERENCES shop.customers (customer_id),
    status        text        NOT NULL,
    channel       text        NOT NULL,
    currency      text        NOT NULL DEFAULT 'EUR',
    order_ts      timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shop.order_items (
    order_item_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id        bigint        NOT NULL REFERENCES shop.orders (order_id),
    product_id      bigint        NOT NULL REFERENCES shop.products (product_id),
    quantity        integer       NOT NULL,
    unit_price      numeric(10,2) NOT NULL,
    discount_amount numeric(10,2) NOT NULL DEFAULT 0,
    updated_at      timestamptz   NOT NULL DEFAULT now()
);

-- Incremental extraction relies on updated_at: index it and keep it honest with triggers.
CREATE INDEX ON shop.customers   (updated_at);
CREATE INDEX ON shop.products    (updated_at);
CREATE INDEX ON shop.orders      (updated_at);
CREATE INDEX ON shop.order_items (updated_at);
CREATE INDEX ON shop.orders      (status);

CREATE TRIGGER customers_touch   BEFORE UPDATE ON shop.customers   FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();
CREATE TRIGGER products_touch    BEFORE UPDATE ON shop.products    FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();
CREATE TRIGGER orders_touch      BEFORE UPDATE ON shop.orders      FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();
CREATE TRIGGER order_items_touch BEFORE UPDATE ON shop.order_items FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();
