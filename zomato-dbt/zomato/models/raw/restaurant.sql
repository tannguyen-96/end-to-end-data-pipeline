{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
) }}
with source as (
    select * from {{ source('landing', 'restaurant') }}
    {% if is_incremental() %}
    where _load_date > (select coalesce(max(_load_date), '1970-01-01'::timestamp) from {{ this }})
    {% endif %}
),
latest as (
    select
        id,
        name,
        city,
        rating,
        rating_count,
        cost,
        cuisine,
        lic_no,
        link,
        address,
        menu,
        _load_date as updated_at,
        coalesce(__op = 'd', false) as _is_deleted
    from source
    qualify row_number() over (partition by id order by _load_date desc, idx desc) = 1
)
select * from latest