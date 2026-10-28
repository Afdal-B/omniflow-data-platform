with deduplicated as (

    {{ dbt_utils.deduplicate(
        relation=ref('stg_events__traffic_5min'),
        partition_by='window_start, event_type, device',
        order_by='_ingested_at desc'
    ) }}

)

select
    window_start,
    window_end,
    cast(window_start as date)  as traffic_date,
    event_type,
    device,
    event_count,
    unique_visitors
from deduplicated
