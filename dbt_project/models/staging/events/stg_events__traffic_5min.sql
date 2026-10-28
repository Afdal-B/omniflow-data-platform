with source as (

    select * from {{ source('events', 'traffic_5min') }}

),

renamed as (

    select
        window_start,
        window_end,
        event_type,
        device,
        event_count,
        unique_anonymous_id     as unique_visitors,
        _batch_id,
        _ingested_at
    from source

)

select * from renamed
