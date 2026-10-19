select 
    f_id as food_id,
    coalesce(item, 'Unknown') as food_name, -- keep unnamed items so order lines don't orphan
    coalesce(initcap(veg_or_non_veg), 'Unknown') as veg_or_non_veg -- capitalizes snowflake func
from {{ ref('food') }}
where f_id is not null
    and is_deleted = false