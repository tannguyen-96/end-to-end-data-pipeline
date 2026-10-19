{{ config(
    materialized='incremental',
    unique_key=['menu_id', 'f_id'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
) }}

with source as (
    select * from {{ source('landing', 'menu') }}
    {% if is_incremental() %}
    where _load_date > (select coalesce(max(updated_at), '1970-01-01'::timestamp) from {{ this }})
    {% endif %}
),
latest as (
    select
        menu_id,
        r_id,
        f_id,
        cuisine,
        price,
        _load_date as updated_at,
        (coalesce(__op = 'd', false)) as is_deleted
    from source
    qualify row_number() over (
        partition by menu_id, f_id
        order by _load_date desc, idx desc
    ) = 1
)
select * from latest