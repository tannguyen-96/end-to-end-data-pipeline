USE WAREHOUSE ZOMATO_WAREHOUSE;

COPY INTO ZOMATO.LANDING.reviews
  (review_id, order_id, user_id, restaurant_id, rating, comment, review_date, __op, __source_ts_ms, _load_date, _filename)
FROM (
    SELECT
      $1:review_id::NUMBER,
      $1:order_id::NUMBER,
      $1:user_id::NUMBER,
      $1:restaurant_id::NUMBER,
      $1:rating::NUMBER,
      $1:comment::STRING,
      $1:review_date::NUMBER,
      $1:__op::STRING,
      $1:__source_ts_ms::NUMBER,
      CURRENT_TIMESTAMP()::TIMESTAMP,
      METADATA$FILENAME
    FROM @ZOMATO.LANDING.S3_CDC_STAGE/reviews/load_date={{ params.load_date }}/
)
FILE_FORMAT = (TYPE = PARQUET);
