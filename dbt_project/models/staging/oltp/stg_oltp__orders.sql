with source as (

    select * from {{ source('oltp', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        lower(trim(status))     as order_status,
        lower(trim(channel))    as channel,
        upper(trim(currency))   as currency,
        order_ts                as ordered_at,
        updated_at,
        _batch_id,
        _ingested_at
    from source

)

select * from renamed
