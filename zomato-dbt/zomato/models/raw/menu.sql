{{ config(
    materialized='incremental',
    unique_key=['menu_id', 'f_id'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
) }}

-- ZOMATO.RAW.menu — latest CDC state per (menu_id, f_id), merged from the append-only
-- LANDING source. Soft delete: __op = 'd' sets _is_deleted = true (row kept).

with source as (

    select * from {{ source('landing', 'menu') }}
    {% if is_incremental() %}
    where __source_ts_ms > (select coalesce(max(__source_ts_ms), 0) from {{ this }})
    {% endif %}

),

latest as (

    select
        menu_id,
        r_id,
        f_id,
        cuisine,
        {{ base64_to_number('price', 100) }} as price,
        __source_ts_ms,
        (__op = 'd') as _is_deleted
    from source
    qualify row_number() over (
        partition by menu_id, f_id
        order by __source_ts_ms desc, idx desc
    ) = 1

)

select * from latest
