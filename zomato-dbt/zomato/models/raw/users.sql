{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
) }}

-- ZOMATO.RAW.users — latest CDC state per user_id, merged from the append-only LANDING
-- source. Soft delete: __op = 'd' sets _is_deleted = true (row kept).
-- NOTE: LANDING.users intentionally has no `password` column, so it is not loaded here.

with source as (

    select * from {{ source('landing', 'users') }}
    {% if is_incremental() %}
    where __source_ts_ms > (select coalesce(max(__source_ts_ms), 0) from {{ this }})
    {% endif %}

),

latest as (

    select
        user_id,
        name,
        email,
        age,
        gender,
        marital_status,
        occupation,
        monthly_income,
        educational_qualifications,
        family_size,
        __source_ts_ms,
        (__op = 'd') as _is_deleted
    from source
    qualify row_number() over (partition by user_id order by __source_ts_ms desc, idx desc) = 1

)

select * from latest
