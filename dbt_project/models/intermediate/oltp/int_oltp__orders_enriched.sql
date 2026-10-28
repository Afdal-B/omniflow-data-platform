with orders as (

    {{ dbt_utils.deduplicate(
        relation=ref('stg_oltp__orders'),
        partition_by='order_id',
        order_by='updated_at desc, _ingested_at desc'
    ) }}

),

order_totals as (

    select
        order_id,
        count(*)                    as line_count,
        sum(quantity)               as total_quantity,
        sum(line_gross_amount)      as gross_amount,
        sum(discount_amount)        as discount_amount,
        sum(line_net_amount)        as net_amount
    from {{ ref('int_oltp__order_items_current') }}
    group by order_id

)

select
    orders.order_id,
    orders.customer_id,
    orders.order_status,
    orders.channel,
    orders.currency,
    orders.ordered_at,
    orders.updated_at,
    coalesce(order_totals.line_count, 0)        as line_count,
    coalesce(order_totals.total_quantity, 0)    as total_quantity,
    coalesce(order_totals.gross_amount, 0)      as gross_amount,
    coalesce(order_totals.discount_amount, 0)   as discount_amount,
    coalesce(order_totals.net_amount, 0)        as net_amount,
    orders.order_status in ({{ sql_list(var('revenue_statuses')) }}) as is_revenue
from orders
left join order_totals
    on orders.order_id = order_totals.order_id
