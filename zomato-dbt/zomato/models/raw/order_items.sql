{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
) }}

-- ZOMATO.RAW.order_items — latest CDC state per order_item_id, merged from the append-only
-- LANDING source. Soft delete: __op = 'd' sets _is_deleted = true (row kept).

with source as (

    select * from {{ source('landing', 'order_items') }}
    {% if is_incremental() %}
    where __source_ts_ms > (select coalesce(max(__source_ts_ms), 0) from {{ this }})
    {% endif %}

),

latest as (

    select
        order_item_id,
        order_id,
        r_id,
        f_id,
        {{ base64_to_number('price', 100) }} as price,
        quantity,
        {{ base64_to_number('line_amount', 100) }} as line_amount,
        __source_ts_ms,
        (__op = 'd') as _is_deleted
    from source
    qualify row_number() over (partition by order_item_id order by __source_ts_ms desc, idx desc) = 1

)

select * from latest
