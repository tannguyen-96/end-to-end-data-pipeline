{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
) }}

-- ZOMATO.RAW.restaurant — latest CDC state per id, merged from the append-only LANDING
-- source. Soft delete: __op = 'd' sets _is_deleted = true (row kept).

with source as (

    select * from {{ source('landing', 'restaurant') }}
    {% if is_incremental() %}
    where __source_ts_ms > (select coalesce(max(__source_ts_ms), 0) from {{ this }})
    {% endif %}

),

latest as (

    select
        id,
        name,
        city,
        {{ base64_to_number('rating', 10) }} as rating,
        rating_count,
        cost,
        cuisine,
        lic_no,
        link,
        address,
        menu,
        __source_ts_ms,
        (__op = 'd') as _is_deleted
    from source
    qualify row_number() over (partition by id order by __source_ts_ms desc, idx desc) = 1

)

select * from latest
