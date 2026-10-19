with customers as (
    select
        customer_id,
        count(*) as order_count,
        count_if(is_delivered) as delivered_orders,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        sum(sales_qty) as units_sold,
        sum(try_to_decimal(sales_amount, 38, 2)) as total_spend,
        avg(try_to_decimal(customer_rating, 38, 2)) as avg_rating_given,
        avg(try_to_decimal(delivery_time_min, 38, 2)) as avg_delivery_time_min
    from {{ ref('fct_orders') }}
    group by customer_id
),
as_of as (
    select max(order_date) as as_of_date
    from {{ ref('fct_orders') }}
)
select
    c.customer_id,
    c.customer_name,
    c.gender,
    c.age,
    c.income_band,
    coalesce(o.order_count, 0) as order_count,
    coalesce(o.delivered_orders, 0) as delivered_orders,
    round(div0(o.delivered_orders, o.order_count), 2) as delivered_rate,
    o.first_order_date,
    o.last_order_date,
    a.as_of_date,
    datediff('day', o.last_order_date, a.as_of_date) as days_since_last_order,
    coalesce(o.units_sold, 0) as units_sold,
    coalesce(o.total_spend, 0) as total_spend,
    round(div0(o.total_spend, o.order_count), 2) as avg_order_value,
    round(o.avg_rating_given, 2) as avg_rating_given,
    round(o.avg_delivery_time_min, 2) as avg_delivery_time_min
from {{ ref('dim_customer') }} c
left join customers o
    on c.customer_id = o.customer_id
cross join as_of a
