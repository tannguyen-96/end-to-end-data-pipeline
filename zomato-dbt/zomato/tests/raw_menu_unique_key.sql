-- Composite-key uniqueness for menu: fails if any (menu_id, f_id) appears more than once.
select
    menu_id,
    f_id,
    count(*) as row_count
from {{ ref('menu') }}
group by 1, 2
having count(*) > 1