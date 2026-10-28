with deduplicated as (

    {{ dbt_utils.deduplicate(
        relation=ref('stg_oltp__order_items'),
        partition_by='order_item_id',
        order_by='updated_at desc, _ingested_at desc'
    ) }}

)

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    quantity * unit_price                       as line_gross_amount,
    quantity * unit_price - discount_amount     as line_net_amount,
    updated_at
from deduplicated
