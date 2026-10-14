USE WAREHOUSE ZOMATO_WAREHOUSE;

COPY INTO ZOMATO.LANDING.restaurant
  (id, name, city, rating, rating_count, cost, cuisine, lic_no, link, address, menu, __op, __source_ts_ms, _load_date, _filename)
FROM (
    SELECT
      $1:id::NUMBER,
      $1:name::STRING,
      $1:city::STRING,
      $1:rating::STRING,
      $1:rating_count::STRING,
      $1:cost::STRING,
      $1:cuisine::STRING,
      $1:lic_no::STRING,
      $1:link::STRING,
      $1:address::STRING,
      $1:menu::STRING,
      $1:__op::STRING,
      $1:__source_ts_ms::NUMBER,
      CURRENT_TIMESTAMP()::TIMESTAMP,
      METADATA$FILENAME
    FROM @ZOMATO.LANDING.S3_CDC_STAGE/restaurant/load_date={{ params.load_date }}/
)
FILE_FORMAT = (TYPE = PARQUET);
