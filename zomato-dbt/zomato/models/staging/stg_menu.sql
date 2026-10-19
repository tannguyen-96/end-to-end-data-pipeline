select 
    menu_id, 
    r_id as restaurant_id,
    f_id as food_id,
    cuisine, 
    try_to_decimal(price,10,2) as price
from {{ ref('menu') }}
where is_deleted = false