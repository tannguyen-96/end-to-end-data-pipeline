select
    menu_id,
    restaurant_id,
    food_id,
    cuisine,
    price
from {{ ref('stg_menu') }}
