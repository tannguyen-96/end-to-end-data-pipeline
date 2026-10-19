USE WAREHOUSE ZOMATO_WAREHOUSE;

COPY INTO ZOMATO.LANDING.food
  (f_id, item, veg_or_non_veg, __op, __source_ts_ms, _load_date, _filename)
FROM (
    SELECT
      $1:f_id::STRING,
      $1:item::STRING,
      $1:veg_or_non_veg::STRING,
      $1:__op::STRING,
      $1:__source_ts_ms::NUMBER,
      CURRENT_TIMESTAMP()::TIMESTAMP,
      METADATA$FILENAME
    FROM @ZOMATO.LANDING.S3_CDC_STAGE/food/load_date={{ ds }}/
)
FILE_FORMAT = (TYPE = PARQUET);