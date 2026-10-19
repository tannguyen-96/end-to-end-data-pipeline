with orders_temp as (
    select
        o.restaurant_id, 
        r.restaurant_name, 
        r.city, 
        r.cuisine,
        count(*) as order_count,
        count_if(is_delivered) as delivered_orders,
        sum(sales_qty) as units_sold,
        sum(try_to_decimal(sales_amount, 38, 2)) as net_sales,
        round(avg(try_to_decimal(customer_rating, 38, 2)), 2) as avg_customer_rating,
        round(avg(try_to_decimal(delivery_time_min, 38, 2)), 2) as avg_delivery_time_min,
        sum(iff(o.is_delivered, o.sales_amount, 0)) as revenue
    from {{ ref('fct_orders') }} o
    left join {{ ref('dim_restaurant') }} r
        on o.restaurant_id = r.restaurant_id
    group by 1,2,3,4
)
select
    restaurant_id,
    restaurant_name,
    city,
    cuisine,
    coalesce(order_count, 0) as order_count,
    coalesce(delivered_orders, 0) as delivered_orders,
    round(div0(delivered_orders, order_count), 2) as delivered_rate,
    coalesce(units_sold, 0) as units_sold,
    coalesce(net_sales, 0) as net_sales,
    round(div0(net_sales, order_count), 2) as avg_order_value,
    avg_customer_rating,
    avg_delivery_time_min,
    coalesce(revenue, 0) as revenue
from orders_temp
