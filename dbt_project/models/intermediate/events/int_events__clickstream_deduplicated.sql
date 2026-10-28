with deduplicated as (

    {{ dbt_utils.deduplicate(
        relation=ref('stg_events__clickstream'),
        partition_by='event_id',
        order_by='_ingested_at, _kafka_partition, _kafka_offset'
    ) }}

)

select
    event_id,
    event_type,
    event_at,
    anonymous_id,
    customer_id,
    page_url,
    product_id,
    order_id,
    device,
    country_code,
    published_at
from deduplicated
