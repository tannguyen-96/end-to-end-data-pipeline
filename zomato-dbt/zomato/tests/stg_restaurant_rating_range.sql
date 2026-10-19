-- restaurant rating must be between 1 and 5 when present
select *
from {{ ref('stg_restaurant') }}
where rating is not null
    and (rating < 1 or rating > 5)
