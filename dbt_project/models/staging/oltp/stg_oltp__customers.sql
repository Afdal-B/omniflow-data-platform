with source as (

    select * from {{ source('oltp', 'customers') }}

),

renamed as (

    select
        customer_id,
        lower(trim(email))          as email,
        initcap(trim(first_name))   as first_name,
        initcap(trim(last_name))    as last_name,
        upper(trim(country_code))   as country_code,
        lower(trim(segment))        as customer_segment,
        created_at,
        updated_at,
        _batch_id,
        _ingested_at
    from source

)

select * from renamed
