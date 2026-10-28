with source as (

    select * from {{ source('oltp', 'order_items') }}

),

renamed as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        cast(unit_price as {{ dbt.type_numeric() }})        as unit_price,
        cast(discount_amount as {{ dbt.type_numeric() }})   as discount_amount,
        updated_at,
        _batch_id,
        _ingested_at
    from source

)

select * from renamed
