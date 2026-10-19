select 
    order_item_id,
    order_id,
    r_id as restaurant_id,
    f_id as food_id,
    try_to_decimal(price,10,2) as price,
    quantity,
    try_to_decimal(line_amount,10,2) as line_amount
from {{ ref('order_items') }}
where is_deleted = false