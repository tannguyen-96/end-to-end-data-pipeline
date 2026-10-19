USE WAREHOUSE ZOMATO_WAREHOUSE;

COPY INTO ZOMATO.LANDING.users
  (user_id, name, email, age, gender, marital_status, occupation, monthly_income, educational_qualifications, family_size, __op, __source_ts_ms, _load_date, _filename)
FROM (
    SELECT
      $1:user_id::NUMBER,
      $1:name::STRING,
      $1:email::STRING,
      $1:age::NUMBER,
      $1:gender::STRING,
      $1:marital_status::STRING,
      $1:occupation::STRING,
      $1:monthly_income::STRING,
      $1:educational_qualifications::STRING,
      $1:family_size::NUMBER,
      $1:__op::STRING,
      $1:__source_ts_ms::NUMBER,
      CURRENT_TIMESTAMP()::TIMESTAMP,
      METADATA$FILENAME
    FROM @ZOMATO.LANDING.S3_CDC_STAGE/users/load_date={{ ds }}/
)
FILE_FORMAT = (TYPE = PARQUET);
