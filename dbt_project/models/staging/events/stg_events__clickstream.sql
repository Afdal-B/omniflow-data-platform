with source as (

    select * from {{ source('events', 'clickstream') }}

),

renamed as (

    select
        event_id,
        event_type,
        event_ts            as event_at,
        anonymous_id,
        customer_id,
        page_url,
        product_id,
        order_id,
        device,
        country_code,
        _kafka_ts           as published_at,
        _kafka_partition,
        _kafka_offset,
        _batch_id,
        _ingested_at
    from source

)

select * from renamed
