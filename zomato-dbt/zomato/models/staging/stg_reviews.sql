select
    r.review_id,
    r.order_id,
    r.user_id as customer_id,
    r.restaurant_id as restaurant_id,
    r.rating as rating,
    r.comment as comment,
    DATEADD(DAY, r.review_date, '1970-01-01'::DATE) as review_date
from {{ ref('reviews') }} r
where r.comment is not null
    and trim(r.comment) != ''
    and r.is_deleted = false