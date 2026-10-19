with bounds as (
    select
        min(date_day) as min_date,
        max(date_day) as max_date
    from (
        select order_date as date_day from {{ ref('stg_orders') }}
        union all
        select review_date as date_day from {{ ref('stg_reviews') }}
    )
),
days as (
    select dateadd(day, seq4(), (select min_date from bounds)) as date_day
    from table(generator(rowcount => 10000))
)
select
    date_day,
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    monthname(date_day) as month_name,
    day(date_day) as day_of_month,
    dayofweekiso(date_day) as day_of_week,
    dayname(date_day) as day_name,
    weekiso(date_day) as week_of_year,
    (dayofweekiso(date_day) >= 6) as is_weekend
from days
where date_day <= (select max_date from bounds)