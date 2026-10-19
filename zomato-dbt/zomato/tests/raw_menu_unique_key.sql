-- menu_id + f_id should be unique
select
    menu_id,
    f_id,
    count(*) as row_count
from {{ ref('menu') }}
group by 1, 2
having count(*) > 1