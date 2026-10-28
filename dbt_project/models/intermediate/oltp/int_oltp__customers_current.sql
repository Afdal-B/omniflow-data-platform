with deduplicated as (

    {{ dbt_utils.deduplicate(
        relation=ref('stg_oltp__customers'),
        partition_by='customer_id',
        order_by='updated_at desc, _ingested_at desc'
    ) }}

)

select
    customer_id,
    email,
    first_name,
    last_name,
    country_code,
    customer_segment,
    created_at,
    updated_at
from deduplicated
