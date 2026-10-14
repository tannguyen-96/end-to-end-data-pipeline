{{ config(
    materialized='incremental',
    unique_key='review_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
) }}

-- ZOMATO.RAW.reviews — latest CDC state per review_id, merged from the append-only LANDING
-- source. Soft delete: __op = 'd' sets _is_deleted = true (row kept).

with source as (

    select * from {{ source('landing', 'reviews') }}
    {% if is_incremental() %}
    where __source_ts_ms > (select coalesce(max(__source_ts_ms), 0) from {{ this }})
    {% endif %}

),

latest as (

    select
        review_id,
        order_id,
        user_id,
        restaurant_id,
        rating,
        comment,
        dateadd(day, review_date, '1970-01-01'::date) as review_date,
        __source_ts_ms,
        (__op = 'd') as _is_deleted
    from source
    qualify row_number() over (partition by review_id order by __source_ts_ms desc, idx desc) = 1

)

select * from latest
