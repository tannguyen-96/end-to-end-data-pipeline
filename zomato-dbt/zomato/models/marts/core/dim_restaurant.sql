select
    restaurant_id,
    restaurant_name,
    city,
    rating,
    rating_count,
    cost,
    cuisine,
    license_no
from {{ ref('stg_restaurant') }}
