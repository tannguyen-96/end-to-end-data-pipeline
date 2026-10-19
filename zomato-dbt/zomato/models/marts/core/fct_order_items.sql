select
    i.order_item_id,
    i.order_id,
    o.customer_id,
    i.restaurant_id,
    i.food_id,
    o.order_date,
    to_number(to_char(o.order_date, 'YYYYMMDD')) as date_key,
    i.price,
    i.quantity,
    i.line_amount
from {{ ref('stg_order_items') }} i
join {{ ref('stg_orders') }} o
    on i.order_id = o.order_id
