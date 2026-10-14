USE WAREHOUSE ZOMATO_WAREHOUSE;

COPY INTO ZOMATO.LANDING.orders
  (order_id, order_timestamp, order_date, user_id, r_id, restaurant_city, cuisine, items_count, sales_qty, subtotal, discount, delivery_fee, gst, sales_amount, currency, payment_method, order_status, customer_rating, delivery_time_min, __op, __source_ts_ms, _load_date, _filename)
FROM (
    SELECT
      $1:order_id::NUMBER,
      $1:order_timestamp::NUMBER,
      $1:order_date::NUMBER,
      $1:user_id::NUMBER,
      $1:r_id::NUMBER,
      $1:restaurant_city::STRING,
      $1:cuisine::STRING,
      $1:items_count::NUMBER,
      $1:sales_qty::NUMBER,
      $1:subtotal::STRING,
      $1:discount::STRING,
      $1:delivery_fee::STRING,
      $1:gst::STRING,
      $1:sales_amount::STRING,
      $1:currency::STRING,
      $1:payment_method::STRING,
      $1:order_status::STRING,
      $1:customer_rating::STRING,
      $1:delivery_time_min::STRING,
      $1:__op::STRING,
      $1:__source_ts_ms::NUMBER,
      CURRENT_TIMESTAMP()::TIMESTAMP,
      METADATA$FILENAME
    FROM @ZOMATO.LANDING.S3_CDC_STAGE/orders/load_date={{ params.load_date }}/
)
FILE_FORMAT = (TYPE = PARQUET);
