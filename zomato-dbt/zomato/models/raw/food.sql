{{ config(
    materialized='incremental',
    unique_key='f_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
) }}

with source as (
    select * from {{ source('landing', 'food') }}
    {% if is_incremental() %}
    where __source_ts_ms > (select coalesce(max(__source_ts_ms), 0) from {{ this }})
    {% endif %}
),
latest as (
    select
        f_id,
        item,
        veg_or_non_veg,
        __source_ts_ms,
        __op = 'd' as _is_deleted
    from source
    qualify row_number() over (partition by f_id order by __source_ts_ms desc, idx desc) = 1
)
select * from latest