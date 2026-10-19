with enriched_reviews as (
    select *
    from {{ ref('fct_reviews') }}
    where is_enriched
),
all_reviews as (
    select
        restaurant_id,
        count(*) as review_count,
        round(avg(rating), 2) as avg_rating
    from {{ ref('fct_reviews') }}
    group by 1
),
by_restaurant as (
    select
        restaurant_id,
        count(*) as enriched_review_count,
        round(avg(sentiment_score), 3) as avg_sentiment_score,
        count_if(sentiment_label = 'POSITIVE') as positive_reviews,
        count_if(sentiment_label = 'NEUTRAL') as neutral_reviews,
        count_if(sentiment_label = 'NEGATIVE') as negative_reviews,
        round(div0(count_if(sentiment_label = 'NEGATIVE'), count(*)), 3) as negative_rate,
        count_if(topic = 'food quality') as food_quality_reviews,
        count_if(topic = 'delivery') as delivery_reviews,
        count_if(topic = 'pricing') as pricing_reviews,
        count_if(topic = 'service') as service_reviews,
        count_if(topic = 'packaging') as packaging_reviews,
        count_if(topic = 'other') as other_reviews
    from enriched_reviews
    group by 1
),
topic_counts as (
    select
        restaurant_id,
        topic,
        count(*) as topic_reviews
    from enriched_reviews
    group by 1, 2
),
top_topic as (
    select restaurant_id, topic as top_topic
    from topic_counts
    qualify row_number() over (
        partition by restaurant_id order by topic_reviews desc, topic
    ) = 1
),
issue_counts as (
    select
        restaurant_id,
        key_issue,
        count(*) as issue_reviews
    from enriched_reviews
    where sentiment_label = 'NEGATIVE' and key_issue is not null
    group by 1, 2
),
top_issue as (
    select restaurant_id, key_issue as top_issue
    from issue_counts
    qualify row_number() over (
        partition by restaurant_id order by issue_reviews desc, key_issue
    ) = 1
)
select
    r.restaurant_id,
    r.restaurant_name,
    r.city,
    r.cuisine,
    coalesce(a.review_count, 0) as review_count,
    coalesce(b.enriched_review_count, 0) as enriched_review_count,
    round(div0(coalesce(b.enriched_review_count, 0), a.review_count), 3) as enriched_rate,
    a.avg_rating,
    b.avg_sentiment_score,
    coalesce(b.positive_reviews, 0) as positive_reviews,
    coalesce(b.neutral_reviews, 0) as neutral_reviews,
    coalesce(b.negative_reviews, 0) as negative_reviews,
    coalesce(b.negative_rate, 0) as negative_rate,
    coalesce(b.food_quality_reviews, 0) as food_quality_reviews,
    coalesce(b.delivery_reviews, 0) as delivery_reviews,
    coalesce(b.pricing_reviews, 0) as pricing_reviews,
    coalesce(b.service_reviews, 0) as service_reviews,
    coalesce(b.packaging_reviews, 0) as packaging_reviews,
    coalesce(b.other_reviews, 0) as other_reviews,
    t.top_topic,
    i.top_issue
from {{ ref('dim_restaurant') }} r
left join all_reviews a
    on r.restaurant_id = a.restaurant_id
left join by_restaurant b
    on r.restaurant_id = b.restaurant_id
left join top_topic t
    on r.restaurant_id = t.restaurant_id
left join top_issue i
    on r.restaurant_id = i.restaurant_id
