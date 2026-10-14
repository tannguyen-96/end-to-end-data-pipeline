USE WAREHOUSE ZOMATO_WAREHOUSE;

COPY INTO ZOMATO.LANDING.order_items
  (order_item_id, order_id, r_id, f_id, price, quantity, line_amount, __op, __source_ts_ms, _load_date, _filename)
FROM (
    SELECT
      $1:order_item_id::NUMBER,
      $1:order_id::NUMBER,
      $1:r_id::NUMBER,
      $1:f_id::STRING,
      $1:price::STRING,
      $1:quantity::NUMBER,
      $1:line_amount::STRING,
      $1:__op::STRING,
      $1:__source_ts_ms::NUMBER,
      CURRENT_TIMESTAMP()::TIMESTAMP,
      METADATA$FILENAME
    FROM @ZOMATO.LANDING.S3_CDC_STAGE/order_items/load_date={{ params.load_date }}/
)
FILE_FORMAT = (TYPE = PARQUET);
