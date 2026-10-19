select
    order_id,
    TO_TIMESTAMP(order_timestamp, 6) as order_timestamp,
    DATEADD(DAY, order_date, '1970-01-01'::DATE) as order_date,
    user_id as customer_id,
    r_id as restaurant_id,
    trim(coalesce(regexp_substr(restaurant_city, '[^,]+$'), restaurant_city)) as city, -- Chrompet,Chennai to Chennai
    cuisine, 
    items_count, 
    sales_qty, 
    subtotal, 
    discount, 
    delivery_fee, 
    gst, 
    sales_amount,
    currency, 
    payment_method, 
    order_status, 
    (order_status='Delivered') as is_delivered,
    customer_rating, 
    delivery_time_min
from {{ ref('orders') }}
where is_deleted = false