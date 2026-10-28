select
    order_id,
    customer_id,
    cast(ordered_at as date)    as order_date,
    ordered_at,
    order_status,
    channel,
    currency,
    line_count,
    total_quantity,
    gross_amount,
    discount_amount,
    net_amount,
    is_revenue,
    updated_at
from {{ ref('int_oltp__orders_enriched') }}
