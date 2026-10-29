SELECT setseed(0.42);

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

INSERT INTO shop.customers (email, first_name, last_name, country_code, segment)
VALUES
    ('jean.dupont@', 'Jean', 'Dupont', 'FR', 'consumer'),
    ('no-email-provided', 'Anne', 'Morel', 'FR', 'consumer'),
    ('k.schmidt@example.com', 'Klaus', 'Schmidt', 'germany', 'consumer'),
    ('b2b.buyer@example.com', 'Paul', 'Lambert', 'FR', 'enterprise');

WITH catalog (position, category, adjectives, nouns) AS (
    VALUES
        (0, 'electronics',
            ARRAY['Wireless', 'Smart', 'Compact', 'Pro', 'Ultra', 'Portable', 'Premium', 'Essential'],
            ARRAY['Headphones', 'Speaker', 'Smartwatch', 'Charger', 'Webcam']),
        (1, 'home',
            ARRAY['Cozy', 'Classic', 'Nordic', 'Minimal', 'Rustic', 'Modern', 'Handmade', 'Scandinavian'],
            ARRAY['Lamp', 'Mug', 'Cushion', 'Vase', 'Blanket']),
        (2, 'fashion',
            ARRAY['Urban', 'Classic', 'Organic', 'Vintage', 'Slim', 'Oversized', 'Waterproof', 'Essential'],
            ARRAY['Jacket', 'Sneakers', 'Scarf', 'Backpack', 'Hoodie']),
        (3, 'sports',
            ARRAY['Pro', 'Lightweight', 'Eco', 'Performance', 'Classic', 'Ultra', 'Compact', 'Training'],
            ARRAY['Yoga Mat', 'Water Bottle', 'Dumbbells', 'Running Shorts', 'Tennis Racket']),
        (4, 'beauty',
            ARRAY['Hydrating', 'Organic', 'Radiant', 'Gentle', 'Night', 'Rose', 'Daily', 'Soothing'],
            ARRAY['Serum', 'Face Cream', 'Shampoo', 'Lipstick', 'Perfume']),
        (5, 'books',
            ARRAY['Bestselling', 'Illustrated', 'Classic', 'Modern', 'Pocket', 'Complete', 'Essential', 'Deluxe'],
            ARRAY['Novel', 'Cookbook', 'Travel Guide', 'Biography', 'Comic Book'])
)
INSERT INTO shop.products (sku, product_name, category, unit_price, created_at, updated_at)
SELECT
    'SKU-' || lpad(g::text, 5, '0'),
    c.adjectives[1 + rank_in_category % 8] || ' ' || c.nouns[1 + rank_in_category / 8],
    c.category,
    round((5 + random() * 145)::numeric, 2),
    now() - interval '18000 hours',
    now() - interval '18000 hours'
FROM generate_series(1, 240) AS g
CROSS JOIN LATERAL (SELECT (g - 1) / 6 AS rank_in_category) AS r
JOIN catalog AS c ON c.position = g % 6
ORDER BY g;

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
          AND c.customer_id <= 3000
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
