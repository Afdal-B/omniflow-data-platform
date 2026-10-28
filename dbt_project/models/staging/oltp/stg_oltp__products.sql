with source as (

    select * from {{ source('oltp', 'products') }}

),

renamed as (

    select
        product_id,
        upper(trim(sku))                                as sku,
        trim(product_name)                              as product_name,
        lower(trim(category))                           as category,
        cast(unit_price as {{ dbt.type_numeric() }})    as unit_price,
        is_active,
        created_at,
        updated_at,
        _batch_id,
        _ingested_at
    from source

)

select * from renamed
