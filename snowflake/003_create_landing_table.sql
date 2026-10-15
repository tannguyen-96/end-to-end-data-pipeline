-- LANDING
-- From s3 to Snowflake Landing Table, multiple parquet files
-- Append only --> New primary key needed
-- idx is an IDENTITY surrogate key (auto-filled by Snowflake; never supplied by COPY)
-- Use __op and __source_ts_ms for change tracking
USE ROLE ACCOUNTADMIN;

CREATE TABLE IF NOT EXISTS ZOMATO.LANDING.users (
  idx NUMBER(38,0) IDENTITY,
  user_id NUMBER(38,0),
  name STRING,
  email STRING,
--  password STRING, 
  age NUMBER(38,0),
  gender STRING,
  marital_status STRING,
  occupation STRING,
  monthly_income STRING,
  educational_qualifications STRING,
  family_size NUMBER(38,0),
  __op STRING,
  __source_ts_ms NUMBER(38,0),
  _load_date TIMESTAMP,
  _filename STRING
);

CREATE TABLE IF NOT EXISTS ZOMATO.LANDING.restaurant (
  idx NUMBER(38,0) IDENTITY,
  id NUMBER(38,0),
  name STRING,
  city STRING,
  rating STRING,
  rating_count STRING,
  cost STRING,
  cuisine STRING,
  lic_no STRING,
  link STRING,
  address STRING,
  menu STRING,
  __op STRING,
  __source_ts_ms NUMBER(38,0),
  _load_date TIMESTAMP,
  _filename STRING
);

CREATE TABLE IF NOT EXISTS ZOMATO.LANDING.food (
  idx NUMBER(38,0) IDENTITY,
  f_id STRING,
  item STRING,
  veg_or_non_veg STRING,
  __op STRING,
  __source_ts_ms NUMBER(38,0),
  _load_date TIMESTAMP,
  _filename STRING
);

CREATE TABLE IF NOT EXISTS ZOMATO.LANDING.menu (
  idx NUMBER(38,0) IDENTITY,
  menu_id STRING,
  r_id NUMBER(38,0),
  f_id STRING,
  cuisine STRING,
  price STRING,
  __op STRING,
  __source_ts_ms NUMBER(38,0),
  _load_date TIMESTAMP,
  _filename STRING
);

CREATE TABLE IF NOT EXISTS ZOMATO.LANDING.orders (
  idx NUMBER(38,0) IDENTITY,
  order_id NUMBER(38,0),
  order_timestamp NUMBER(38,0),
  order_date NUMBER(38,0),
  user_id NUMBER(38,0),
  r_id NUMBER(38,0),
  restaurant_city STRING,
  cuisine STRING,
  items_count NUMBER(38,0),
  sales_qty NUMBER(38,0),
  subtotal STRING,
  discount STRING,
  delivery_fee STRING,
  gst STRING,
  sales_amount STRING,
  currency STRING,
  payment_method STRING,
  order_status STRING,
  customer_rating STRING,
  delivery_time_min STRING,
  __op STRING,
  __source_ts_ms NUMBER(38,0),
  _load_date TIMESTAMP,
  _filename STRING
);

CREATE TABLE IF NOT EXISTS ZOMATO.LANDING.order_items (
  idx NUMBER(38,0) IDENTITY,
  order_item_id NUMBER(38,0),
  order_id NUMBER(38,0),
  r_id NUMBER(38,0),
  f_id STRING,
  price STRING,
  quantity NUMBER(38,0),
  line_amount STRING,
  __op STRING,
  __source_ts_ms NUMBER(38,0),
  _load_date TIMESTAMP,
  _filename STRING
);

CREATE TABLE IF NOT EXISTS ZOMATO.LANDING.reviews (
  idx NUMBER(38,0) IDENTITY,
  review_id NUMBER(38,0),
  order_id NUMBER(38,0),
  user_id NUMBER(38,0),
  restaurant_id NUMBER(38,0),
  rating NUMBER(38,0),
  comment STRING,
  review_date NUMBER(38,0),
  __op STRING,
  __source_ts_ms NUMBER(38,0),
  _load_date TIMESTAMP,
  _filename STRING
);