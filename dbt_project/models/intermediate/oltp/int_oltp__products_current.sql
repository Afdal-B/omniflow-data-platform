with deduplicated as (

    {{ dbt_utils.deduplicate(
        relation=ref('stg_oltp__products'),
        partition_by='product_id',
        order_by='_ingested_at desc, updated_at desc'
    ) }}

)

select
    product_id,
    sku,
    product_name,
    category,
    unit_price,
    is_active,
    created_at,
    updated_at
from deduplicated
