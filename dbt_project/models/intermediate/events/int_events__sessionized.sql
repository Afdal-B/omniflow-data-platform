with events as (

    select * from {{ ref('int_events__clickstream_deduplicated') }}

),

with_previous_event as (

    select
        *,
        lag(event_at) over (
            partition by anonymous_id
            order by event_at, event_id
        ) as previous_event_at
    from events

),

flagged as (

    select
        *,
        case
            when previous_event_at is null then 1
            when {{ dbt.datediff('previous_event_at', 'event_at', 'minute') }}
                >= {{ var('session_timeout_minutes') }} then 1
            else 0
        end as is_session_start
    from with_previous_event

),

numbered as (

    select
        *,
        sum(is_session_start) over (
            partition by anonymous_id
            order by event_at, event_id
            rows between unbounded preceding and current row
        ) as session_number
    from flagged

)

select
    {{ dbt_utils.generate_surrogate_key(['anonymous_id', 'session_number']) }} as session_id,
    session_number,
    event_id,
    event_type,
    event_at,
    anonymous_id,
    customer_id,
    page_url,
    product_id,
    order_id,
    device,
    country_code
from numbered
