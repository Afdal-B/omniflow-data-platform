select
    product_id,
    sku,
    product_name,
    category,
    unit_price      as current_unit_price,
    is_active,
    created_at
from {{ ref('int_oltp__products_current') }}
