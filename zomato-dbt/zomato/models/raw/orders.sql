{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
) }}

-- ZOMATO.RAW.orders — latest CDC state per order_id, merged from the append-only LANDING
-- source. Soft delete: __op = 'd' sets _is_deleted = true (row kept).
-- Debezium encodes numerics as base64 and dates/timestamps as integer days/microseconds,
-- so the money/rating decimals are decoded and the date columns are converted here.

with source as (

    select * from {{ source('landing', 'orders') }}
    {% if is_incremental() %}
    where __source_ts_ms > (select coalesce(max(__source_ts_ms), 0) from {{ this }})
    {% endif %}

),

latest as (

    select
        order_id,
        to_timestamp_ntz(order_timestamp, 6) as order_timestamp,
        dateadd(day, order_date, '1970-01-01'::date) as order_date,
        user_id,
        r_id,
        restaurant_city,
        cuisine,
        items_count,
        sales_qty,
        {{ base64_to_number('subtotal', 100) }} as subtotal,
        {{ base64_to_number('discount', 100) }} as discount,
        {{ base64_to_number('delivery_fee', 100) }} as delivery_fee,
        {{ base64_to_number('gst', 100) }} as gst,
        {{ base64_to_number('sales_amount', 100) }} as sales_amount,
        currency,
        payment_method,
        order_status,
        {{ base64_to_number('customer_rating', 10) }} as customer_rating,
        {{ base64_to_number('delivery_time_min', 10) }} as delivery_time_min,
        __source_ts_ms,
        (__op = 'd') as _is_deleted
    from source
    qualify row_number() over (partition by order_id order by __source_ts_ms desc, idx desc) = 1

)

select * from latest
