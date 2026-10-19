{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
) }}
with source as (
    select * from {{ source('landing', 'orders') }}
    {% if is_incremental() %}
    where _load_date > (select coalesce(max(updated_at), '1970-01-01'::timestamp) from {{ this }})
    {% endif %}
),
latest as (
    select
        order_id,
        order_timestamp,
        order_date,
        user_id,
        r_id,
        restaurant_city,
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
        customer_rating,
        delivery_time_min,
        _load_date as updated_at,
        coalesce(__op = 'd', false) as is_deleted
    from source
    qualify row_number() over (partition by order_id order by _load_date desc, idx desc) = 1
)
select * from latest