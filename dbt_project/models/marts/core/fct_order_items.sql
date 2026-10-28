with order_items as (

    select * from {{ ref('int_oltp__order_items_current') }}

),

orders as (

    select * from {{ ref('int_oltp__orders_enriched') }}

)

select
    order_items.order_item_id,
    order_items.order_id,
    order_items.product_id,
    orders.customer_id,
    cast(orders.ordered_at as date)     as order_date,
    orders.order_status,
    orders.channel,
    orders.is_revenue,
    order_items.quantity,
    order_items.unit_price,
    order_items.discount_amount,
    order_items.line_gross_amount,
    order_items.line_net_amount,
    order_items.updated_at              as item_updated_at
from order_items
left join orders
    on order_items.order_id = orders.order_id
