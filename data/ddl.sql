-- DDL for the food-delivery dataset in data/*.csv.
-- Table names are the plain file stems: <name>.csv -> <name>.
-- Target: the local Postgres in docker/postgres (replaces the hosted Supabase).
--
-- Notes on typing:
--   * An unnamed leading index column exists in users/restaurant/food/menu; it is
--     dropped on import (see data/import_data.py).
--   * restaurant.rating carries the placeholder '--' (too few ratings); the loader
--     maps it to NULL.
--   * restaurant.cost / rating_count are free text ('₹ 1 200', '100+ ratings').
--   * No FOREIGN KEY constraints are declared; the relationships in data/ERD.md are
--     logical and enforced by the loader/importer only.

create table if not exists public.users (
    user_id                   integer primary key,
    name                      text,
    email                     text,
    password                  text,
    age                       smallint,
    gender                    text,
    marital_status            text,
    occupation                text,
    monthly_income            text,   -- 'No Income', 'Below Rs.10000', ...
    educational_qualifications text,
    family_size               smallint
);

create table if not exists public.restaurant (
    id            integer primary key,
    name          text,
    city          text,
    rating        numeric(3,1),        -- '--' -> NULL
    rating_count  text,                -- '50+ ratings', 'Too Few Ratings', ...
    cost          text,                -- '₹ 1 200'
    cuisine       text,
    lic_no        text,
    link          text,
    address       text,
    menu          text                 -- 'Menu/<id>.json'
);

create table if not exists public.food (
    f_id            text primary key,
    item            text,
    veg_or_non_veg  text
);

create table if not exists public.menu (
    menu_id  text not null,
    r_id     integer not null,
    f_id     text not null,
    cuisine  text,
    price    numeric(10,2),          -- '₹200 FOR TWO' -> 200 (see import_data.py)
    primary key (menu_id, f_id)
);

create table if not exists public.orders (
    order_id           integer primary key,
    order_timestamp    timestamp,
    order_date         date,
    user_id            integer,
    r_id               integer,
    restaurant_city    text,
    cuisine            text,
    items_count        integer,
    sales_qty          integer,
    subtotal           numeric(10,2),
    discount           numeric(10,2),
    delivery_fee       numeric(10,2),
    gst                numeric(10,2),
    sales_amount       numeric(10,2),
    currency           text,
    payment_method     text,
    order_status       text,
    customer_rating    numeric(3,1),
    delivery_time_min  numeric(7,1)
);

create table if not exists public.order_items (
    order_item_id  integer primary key,
    order_id       integer not null,
    r_id           integer,
    f_id           text,
    price          numeric(10,2),
    quantity       integer,
    line_amount    numeric(10,2)
);

create table if not exists public.reviews (
    review_id      integer primary key,
    order_id       integer,
    user_id        integer,
    restaurant_id  integer,
    rating         smallint,
    comment        text,
    review_date    date
);
