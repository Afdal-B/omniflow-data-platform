select
    order_id,
    gross_amount,
    discount_amount,
    net_amount
from {{ ref('fct_orders') }}
where abs(net_amount - (gross_amount - discount_amount)) > 0.01
   or net_amount < 0
