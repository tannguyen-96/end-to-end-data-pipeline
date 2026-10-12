-- =====================================================================
-- Warehouse, database, schemas, role
-- Run in Snowsight as ACCOUNTADMIN
-- =====================================================================
USE ROLE ACCOUNTADMIN;

-- Compute: extra-small, auto-suspend fast so the trial credits last.
CREATE WAREHOUSE IF NOT EXISTS OLIST_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Database + medallion schemas.
CREATE DATABASE IF NOT EXISTS OLIST;
CREATE SCHEMA   IF NOT EXISTS OLIST.BRONZE;    -- Iceberg tables Spark wrote (dbt sources)
CREATE SCHEMA   IF NOT EXISTS OLIST.RAW;       -- only used by the COPY fallback path
CREATE SCHEMA   IF NOT EXISTS OLIST.STAGING;   -- cleaned / conformed (dbt)
CREATE SCHEMA   IF NOT EXISTS OLIST.MARTS;     -- Gold, Iceberg (dbt)
CREATE SCHEMA   IF NOT EXISTS OLIST.SNAPSHOTS; -- SCD2 history (dbt)
CREATE SCHEMA   IF NOT EXISTS OLIST.AI;        -- LLM-enriched tables (OpenAI jobs)

-- A role dbt/Airflow will use.
CREATE ROLE IF NOT EXISTS DBT_ROLE;
GRANT USAGE   ON WAREHOUSE OLIST_WH TO ROLE DBT_ROLE;
GRANT OPERATE ON WAREHOUSE OLIST_WH TO ROLE DBT_ROLE;
GRANT ALL     ON DATABASE  OLIST    TO ROLE DBT_ROLE;
GRANT ALL     ON ALL SCHEMAS IN DATABASE OLIST TO ROLE DBT_ROLE;
GRANT ALL     ON FUTURE SCHEMAS IN DATABASE OLIST TO ROLE DBT_ROLE;
GRANT ALL     ON FUTURE TABLES IN DATABASE OLIST TO ROLE DBT_ROLE;
GRANT ALL     ON FUTURE VIEWS  IN DATABASE OLIST TO ROLE DBT_ROLE;


SET my_user = CURRENT_USER();
GRANT ROLE DBT_ROLE TO USER IDENTIFIER($my_user);

SELECT 'COMPLETED SUCCESSFULLY' AS status;