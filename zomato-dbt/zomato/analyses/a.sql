-- food
select veg_or_non_veg, count(*)
from raw.food
group by veg_or_non_veg;

-- menu
select 
    count_if(
        cuisine is null
    ) as cuisine_is_null,
    count_if(
        try_to_number(price) IS NULL
    ) as price_is_null,
    count_if(
        MOD(ABS(price), 1) <> 0
    ) as price_is_not_integer,
    count_if(
        price < 0
    ) as price_is_negative
from raw.menu;

select length(split_part(price, '.', 2)), count(*)
from raw.menu
group by length(split_part(price, '.', 2));

-- order_items
select 
    count_if(
        quantity is null
    ) as quantity_is_null,
    count_if(
        MOD(ABS(quantity), 1) <> 0
    ) as quantity_is_not_integer,
    count_if(
        quantity < 0
    ) as quantity_is_negative,
    count_if(
        try_to_number(price) IS NULL
    ) as price_is_null,
    count_if(
        MOD(ABS(price), 1) <> 0
    ) as price_is_not_integer,
    count_if(
        price < 0
    ) as price_is_negative,
    count_if(
        try_to_number(price)*quantity <> try_to_number(line_amount)
    ) as line_amount_mismatch -- 49065 records have mismatched price*quantity and line_amount
from raw.order_items;

select oi.price, oi.quantity, oi.line_amount, o.discount, o.delivery_fee, oi.f_id, oi.r_id
from raw.order_items oi
left join raw.orders o on o.order_id = oi.order_id
where try_to_number(oi.price)*oi.quantity <> try_to_number(oi.line_amount); -- price 189 line_amount 190

select price -- 189.52 actual price in menu
from raw.menu
where r_id = '37941' and f_id = 'fd7500'

-- orders
select * from raw.orders;

select
    count_if(
        delivery_fee < 0
    ) as delivery_fee_is_negative,
    count_if(
        discount < 0
    ) as discount_is_negative
from raw.orders;

select currency, count(*)
from raw.orders
group by currency;

select payment_method, count(*)
from raw.orders
group by payment_method;

-- restaurant
select * from raw.restaurant;

select rating, count(*)
from raw.restaurant
group by rating;

select rating_count, count(*)
from raw.restaurant
group by rating_count;
-- Too Few Ratings -> 0
-- 500+ ratings -> 500
-- null -> 0
-- 5K+ ratings -> 5000
-- 50+ ratings
-- 1K+ ratings
-- 20+ ratings
-- 100+ ratings
-- 10K+ ratings

select cost, count(*)
from raw.restaurant
group by cost;

-- reviews
select * from raw.reviews;

select count_if(
    comment is null
)
from raw.reviews;

-- users
select * from raw.users;