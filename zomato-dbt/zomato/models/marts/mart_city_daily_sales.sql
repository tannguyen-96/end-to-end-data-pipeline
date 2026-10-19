with city_day as (
    select
        city,
        date_key,
        count(*) as order_count,
        count_if(is_delivered) as delivered_orders,
        count_if(order_status='Cancelled') as cancelled_orders,
        sum(sales_qty) as units_sold,
        sum(try_to_decimal(subtotal, 38, 2)) as gross_sales,
        sum(try_to_decimal(discount, 38, 2)) as discount_amount,
        sum(try_to_decimal(sales_amount, 38, 2)) as net_sales,
        round(avg(coalesce(try_to_decimal(delivery_time_min, 38, 2), 0)), 2) as avg_delivery_time_min,
        sum(iff(is_delivered, sales_amount, 0)) as gmv
    from {{ ref('fct_orders') }}
    group by city, date_key
)
select
    city || '|' || to_varchar(c.date_key) as city_date_key,
    c.city,
    d.date_day,
    c.date_key,
    d.year,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    c.order_count,
    c.delivered_orders,
    round(div0(c.delivered_orders, c.order_count), 2) as delivered_rate,
    round(div0(c.cancelled_orders, c.order_count), 2) as cancel_rate,
    c.units_sold,
    c.gross_sales,
    c.discount_amount,
    c.net_sales,
    round(div0(c.net_sales, c.order_count), 2) as avg_order_value,
    c.avg_delivery_time_min,
    c.gmv,
    round(div0(c.gmv, c.order_count), 2) as avg_gmv_per_order
from city_day c
join {{ ref('dim_date') }} d
    on c.date_key = d.date_key
