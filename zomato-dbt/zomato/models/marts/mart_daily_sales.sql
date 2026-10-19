with orders as (
    select
        date_key,
        count(*) as order_count,
        count_if(is_delivered) as delivered_orders,
        sum(sales_qty) as units_sold,
        sum(try_to_decimal(subtotal, 38, 2)) as gross_sales,
        sum(try_to_decimal(discount, 38, 2)) as discount_amount,
        sum(try_to_decimal(delivery_fee, 38, 2)) as delivery_fee_amount,
        sum(try_to_decimal(gst, 38, 2)) as gst_amount,
        sum(try_to_decimal(sales_amount, 38, 2)) as net_sales,
        avg(try_to_decimal(customer_rating, 38, 2)) as avg_customer_rating,
        avg(try_to_decimal(delivery_time_min, 38, 2)) as avg_delivery_time_min
    from {{ ref('fct_orders') }}
    group by date_key
)
select
    d.date_day,
    d.date_key,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    coalesce(o.order_count, 0) as order_count,
    coalesce(o.delivered_orders, 0) as delivered_orders,
    round(div0(o.delivered_orders, o.order_count), 2) as delivered_rate,
    coalesce(o.units_sold, 0) as units_sold,
    coalesce(o.gross_sales, 0) as gross_sales,
    coalesce(o.discount_amount, 0) as discount_amount,
    coalesce(o.delivery_fee_amount, 0) as delivery_fee_amount,
    coalesce(o.gst_amount, 0) as gst_amount,
    coalesce(o.net_sales, 0) as net_sales,
    round(div0(o.net_sales, o.order_count), 2) as avg_order_value,
    round(o.avg_customer_rating, 2) as avg_customer_rating,
    round(o.avg_delivery_time_min, 2) as avg_delivery_time_min
from {{ ref('dim_date') }} d
left join orders o
    on d.date_key = o.date_key
