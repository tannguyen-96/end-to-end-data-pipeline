with reviews as (
    select * from {{ ref('stg_reviews') }}
),
enrichment as (
    select * from {{ source('ai', 'review_enriched') }}
)
select
    r.review_id,
    r.order_id,
    r.customer_id,
    r.restaurant_id,
    r.rating,
    r.comment,
    r.review_date,
    to_number(to_char(r.review_date, 'YYYYMMDD')) as date_key,
    e.sentiment_label,
    e.sentiment_score,
    e.topic,
    e.key_issue,
    e.model,
    e.enriched_at,
    (e.review_id is not null) as is_enriched
from reviews r
left join enrichment e
    on r.review_id = e.review_id
