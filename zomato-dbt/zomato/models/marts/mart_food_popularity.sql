with items as (
    select
        food_id,
        sum(quantity) as units_sold,
        count(distinct order_id) as order_count,
        count(distinct restaurant_id) as restaurants_selling,
        sum(line_amount)::number(38, 2) as revenue,
        sum(line_amount)::number(38, 2) / nullif(sum(quantity), 0) as avg_unit_price
    from {{ ref('fct_order_items') }}
    group by food_id
),
menu_coverage as (
    select
        food_id,
        count(distinct restaurant_id) as restaurants_on_menu
    from {{ ref('dim_menu') }}
    group by food_id
)
select
    f.food_id,
    f.food_name,
    f.veg_or_non_veg,
    i.units_sold,
    i.order_count,
    i.revenue,
    i.avg_unit_price,
    i.restaurants_selling,
    coalesce(m.restaurants_on_menu, 0) as restaurants_on_menu
from items i
join {{ ref('dim_food') }} f
    on i.food_id = f.food_id
left join menu_coverage m
    on i.food_id = m.food_id
