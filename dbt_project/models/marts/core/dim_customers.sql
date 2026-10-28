with customers as (

    select * from {{ ref('int_oltp__customers_current') }}

),

customer_orders as (

    select
        customer_id,
        min(ordered_at)     as first_order_at,
        max(ordered_at)     as last_order_at,
        count(*)            as lifetime_orders,
        sum(net_amount)     as lifetime_revenue
    from {{ ref('int_oltp__orders_enriched') }}
    where is_revenue
    group by customer_id

)

select
    customers.customer_id,
    {{ dbt.hash('customers.email') }}                   as email_hash,
    customers.first_name,
    customers.country_code,
    customers.customer_segment,
    customers.created_at                                as customer_since,
    customer_orders.first_order_at,
    customer_orders.last_order_at,
    coalesce(customer_orders.lifetime_orders, 0)        as lifetime_orders,
    coalesce(customer_orders.lifetime_revenue, 0)       as lifetime_revenue,
    case
        when customer_orders.lifetime_orders is null then 'prospect'
        when customer_orders.lifetime_orders = 1 then 'new'
        else 'returning'
    end                                                 as lifecycle_stage
from customers
left join customer_orders
    on customers.customer_id = customer_orders.customer_id
