-- Initial history of the shop: customers who signed up over 2 years, 12 months of orders.
-- Generated in SQL to keep the repo small, and deterministic (setseed): every fresh
-- `docker compose up` produces the same dataset.
--
-- Durations are expressed in hours, not days: adding '1 day' to a timestamptz follows the local
-- calendar and shifts by one hour across a daylight-saving change.
--
-- Note on the `x * 0` / `WHERE g > 0` terms below: they correlate each LATERAL subquery with the
-- outer row, which forces Postgres to draw random() once per row instead of once per statement.
SELECT setseed(0.42);

-- ------------------------------------------------------------------ customers (3,000, signed up over 2 years)
INSERT INTO shop.customers (email, first_name, last_name, country_code, segment, created_at, updated_at)
SELECT
    lower(fn) || '.' || lower(ln) || g || '@example.com',
    fn,
    ln,
    (ARRAY['FR', 'FR', 'FR', 'BE', 'CH', 'DE', 'ES', 'IT', 'GB', 'NL'])[1 + floor(random() * 10)::int],
    CASE WHEN random() < 0.85 THEN 'consumer' ELSE 'business' END,
    signup_ts,
    signup_ts
FROM generate_series(1, 3000) AS g
CROSS JOIN LATERAL (
    SELECT
        (ARRAY['Camille', 'Lucas', 'Léa', 'Hugo', 'Chloé', 'Nathan', 'Inès', 'Louis', 'Manon', 'Adam',
               'Sarah', 'Yanis', 'Emma', 'Noah', 'Jade', 'Amine', 'Lina', 'Tom', 'Zoé', 'Malik'])[1 + floor(random() * 20)::int] AS fn,
        (ARRAY['Martin', 'Bernard', 'Dubois', 'Thomas', 'Robert', 'Richard', 'Petit', 'Durand', 'Leroy', 'Moreau',
               'Diallo', 'Nguyen', 'Garcia', 'Benali', 'Rousseau', 'Fournier', 'Girard', 'Mercier', 'Blanc', 'Traoré'])[1 + floor(random() * 20)::int] AS ln,
        now() - random() * interval '17520 hours' AS signup_ts
    WHERE g > 0
) AS person;

-- A few dirty records, as in every real OLTP. They never place orders, and the ingestion
-- quality gate must route them to the Dead Letter Queue.
INSERT INTO shop.customers (email, first_name, last_name, country_code, segment)
VALUES
    ('jean.dupont@', 'Jean', 'Dupont', 'FR', 'consumer'),               -- invalid email
    ('no-email-provided', 'Anne', 'Morel', 'FR', 'consumer'),           -- invalid email
    ('k.schmidt@example.com', 'Klaus', 'Schmidt', 'germany', 'consumer'), -- not an ISO country code
    ('b2b.buyer@example.com', 'Paul', 'Lambert', 'FR', 'enterprise');   -- unknown segment

-- ------------------------------------------------------------------ products (240)
INSERT INTO shop.products (sku, product_name, category, unit_price, created_at, updated_at)
SELECT
    'SKU-' || lpad(g::text, 5, '0'),
    initcap(adj) || ' ' || noun,
    category,
    round((5 + random() * 145)::numeric, 2),
    now() - interval '18000 hours',
    now() - interval '18000 hours'
FROM generate_series(1, 240) AS g
CROSS JOIN LATERAL (
    SELECT
        (ARRAY['electronics', 'home', 'fashion', 'sports', 'beauty', 'books'])[1 + (g % 6)] AS category,
        (ARRAY['essential', 'premium', 'compact', 'eco', 'urban', 'classic', 'smart', 'pro'])[1 + floor(random() * 8)::int] AS adj,
        (ARRAY['Headphones', 'Lamp', 'Jacket', 'Sneakers', 'Serum', 'Novel', 'Backpack', 'Watch', 'Mug', 'Yoga Mat'])[1 + floor(random() * 10)::int] AS noun
) AS p;

-- ------------------------------------------------------------------ orders (15,000 over 12 months) and lines
-- Orders are spread evenly over the last 12 months; each one is placed by a customer who had
-- already signed up at that time. Its status depends on its age: recent orders are still
-- pending/paid, then shipped, then delivered (or cancelled/refunded).
WITH order_draws AS (
    SELECT
        now() - random() * interval '8760 hours' AS order_ts,
        random() AS r,
        (ARRAY['web', 'web', 'web', 'mobile', 'mobile', 'store'])[1 + floor(random() * 6)::int] AS channel
    FROM generate_series(1, 15000)
),
dated_orders AS (
    SELECT d.order_ts, d.r, d.channel, buyer.customer_id
    FROM order_draws AS d
    CROSS JOIN LATERAL (
        SELECT c.customer_id
        FROM shop.customers AS c
        WHERE c.created_at < d.order_ts
          AND c.customer_id <= 3000          -- the dirty customers never order
        ORDER BY random()
        LIMIT 1
    ) AS buyer
),
new_orders AS (
    INSERT INTO shop.orders (customer_id, status, channel, currency, order_ts, updated_at)
    SELECT
        customer_id,
        CASE
            WHEN order_ts > now() - interval '24 hours' THEN
                CASE WHEN r < 0.40 THEN 'pending' WHEN r < 0.95 THEN 'paid' ELSE 'cancelled' END
            WHEN order_ts > now() - interval '120 hours' THEN
                CASE WHEN r < 0.85 THEN 'shipped' WHEN r < 0.95 THEN 'delivered' ELSE 'cancelled' END
            ELSE
                CASE WHEN r < 0.90 THEN 'delivered' WHEN r < 0.96 THEN 'cancelled' ELSE 'refunded' END
        END,
        channel,
        'EUR',
        order_ts,
        -- last status change: a few days after the order, never in the future
        LEAST(order_ts + interval '96 hours', now())
    FROM dated_orders
    RETURNING order_id, order_ts
)
INSERT INTO shop.order_items (order_id, product_id, quantity, unit_price, discount_amount, updated_at)
SELECT
    o.order_id,
    p.product_id,
    1 + floor(random() * 3)::int,
    p.unit_price,
    CASE WHEN random() < 0.2 THEN round(p.unit_price * 0.1, 2) ELSE 0 END,
    o.order_ts
FROM new_orders AS o
CROSS JOIN LATERAL generate_series(1, 1 + floor(random() * 4)::int + (o.order_id * 0)::int) AS line
CROSS JOIN LATERAL (SELECT 1 + floor(random() * 240)::bigint + line * 0 AS product_id) AS pick
JOIN shop.products AS p ON p.product_id = pick.product_id;

ANALYZE shop.customers, shop.products, shop.orders, shop.order_items;
