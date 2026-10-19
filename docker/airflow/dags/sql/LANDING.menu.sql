USE WAREHOUSE ZOMATO_WAREHOUSE;

COPY INTO ZOMATO.LANDING.menu
  (menu_id, r_id, f_id, cuisine, price, __op, __source_ts_ms, _load_date, _filename)
FROM (
    SELECT
      $1:menu_id::STRING, -- similar with obj.menu_id.toString()
      $1:r_id::NUMBER,
      $1:f_id::STRING,
      $1:cuisine::STRING,
      $1:price::STRING,
      $1:__op::STRING,
      $1:__source_ts_ms::NUMBER,
      CURRENT_TIMESTAMP()::TIMESTAMP,
      METADATA$FILENAME
    FROM @ZOMATO.LANDING.S3_CDC_STAGE/menu/load_date={{ ds }}/
)
FILE_FORMAT = (TYPE = PARQUET);
