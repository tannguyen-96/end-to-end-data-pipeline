select 
    id as restaurant_id,
    name as restaurant_name,
    trim(coalesce(regexp_substr(city, '[^,]+$'), city)) as city,
    try_to_decimal(rating, 2, 1) as rating, -- max 5, only 1 precision, has null values
    coalesce(try_to_number(regexp_substr(rating_count, '[0-9]+')) * iff(regexp_substr(rating_count, '[0-9.]+[Kk].') is not null, 1000, 1), 0) as rating_count,
    try_to_number(regexp_substr(cost, '[0-9]+')) as cost,
    cuisine, 
    lic_no as license_no
from {{ ref('restaurant') }} where try_to_number(id) is not null