{% set funnel_steps = {
    'page_view': 'page_views',
    'search': 'searches',
    'product_view': 'product_views',
    'add_to_cart': 'add_to_carts',
    'begin_checkout': 'checkouts',
    'purchase': 'purchases',
} %}

with events as (

    select * from {{ ref('int_events__sessionized') }}

)

select
    session_id,
    anonymous_id,
    max(customer_id)                                                as customer_id,
    min(event_at)                                                   as session_started_at,
    max(event_at)                                                   as session_ended_at,
    cast(min(event_at) as date)                                     as session_date,
    {{ dbt.datediff('min(event_at)', 'max(event_at)', 'second') }}  as duration_seconds,
    count(*)                                                        as event_count,
    {%- for event_type, column in funnel_steps.items() %}
    sum(case when event_type = '{{ event_type }}' then 1 else 0 end) as {{ column }},
    {%- endfor %}
    max(order_id)                                                   as order_id,
    max(device)                                                     as device,
    max(country_code)                                               as country_code,
    sum(case when event_type = 'purchase' then 1 else 0 end) > 0    as is_converted
from events
group by session_id, anonymous_id
