-- ============================================================================
-- SNOWFLAKE INITIAL SETUP FOR ENERGY DEMO
-- ============================================================================

-- enable cross-region
USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- enable orgadmin
SET USERNAME = (SELECT CURRENT_USER());
GRANT ROLE ORGADMIN TO USER IDENTIFIER($USERNAME);

-- enable cross-cloud auto-fulfillment
USE ROLE ORGADMIN;
SELECT SYSTEM$ENABLE_GLOBAL_DATA_SHARING_FOR_ACCOUNT( 'AWSFF' );

-- create database and schema
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS ENERGY;
CREATE SCHEMA IF NOT EXISTS PROD;
CREATE SCHEMA IF NOT EXISTS DEV;

-- ============================================================================
-- STEP 1: CREATE STORAGE INTEGRATION FOR S3 ACCESS
-- ============================================================================

-- Use the AWS Role and the AWS External Id previously configured in your AWS account
CREATE STORAGE INTEGRATION IF NOT EXISTS energy_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'xx'
  STORAGE_AWS_EXTERNAL_ID = 'xx'
  STORAGE_ALLOWED_LOCATIONS = ('s3://xx/');

-- Get the IAM user ARN for AWS IAM trust policy
-- STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::409633134729:user/698a1000-s
DESC STORAGE INTEGRATION energy_s3_integration;

-- Copy the STORAGE_AWS_IAM_USER_ARN
-- Update the trust relationship in your AWS Role: snow_eu_demo98_energy_role

  
-- ============================================================================
-- STEP 2: CREATE EXTERNAL VOLUME (for Iceberg tables)
-- ============================================================================

-- External volume allows Snowflake-managed Iceberg tables to store metadata
-- and data in your S3 bucket while Snowflake manages the table format
CREATE EXTERNAL VOLUME IF NOT EXISTS energy_external_volume
  STORAGE_LOCATIONS =
    (
      (
    NAME = 'eu-demo98-energy'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://xx/'
    STORAGE_AWS_ROLE_ARN = 'xx'
    STORAGE_AWS_EXTERNAL_ID = 'xx'
      )
    )
;

-- Verify storage access
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('energy_external_volume');

-- ============================================================================
-- STEP 3: CREATE EXTERNAL STAGE (for loading non-Iceberg data)
-- ============================================================================

USE SCHEMA PROD;

-- Create external stage
CREATE OR REPLACE STAGE energy_s3_stage
  STORAGE_INTEGRATION = energy_s3_integration
  URL = 's3://xx/';

ls @energy_s3_stage/smart_meter/;

-- ============================================================================
-- STEP 4: CREATE EXTERNAL STAGE (for loading non-Iceberg data)
-- ============================================================================

-- Create Snowflake table for customers
CREATE OR REPLACE TABLE CUSTOMERS (
  customer_id STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  ssn STRING,
  date_of_birth DATE,
  street_address STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  credit_card_number STRING,
  customer_type STRING,
  service_plan STRING,
  billing_cycle STRING,
  enrollment_date DATE
);

-- Load customers data from S3
COPY INTO CUSTOMERS
FROM @energy_s3_stage/customers_pii.parquet
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

select * from customers;

-- ============================================================================
-- STEP 5: CREATE ICEBERG PROD TABLE
-- ============================================================================

CREATE OR REPLACE ICEBERG TABLE PROD.SMART_METER_READINGS (
  meter_id STRING,
  customer_id STRING,
  timestamp timestamp_ltz(6),
  consumption_kwh FLOAT,
  voltage FLOAT,
  power_factor FLOAT,
  rate_per_kwh FLOAT,
  cost_usd FLOAT,
  reading_status STRING,
  temperature_celsius FLOAT
)
COMMENT = 'Smart meter energy consumption readings'
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='energy_external_volume'
BASE_LOCATION='smart_meter/'
;

-- ============================================================================
-- STEP 6: LOAD DATA INTO ICEBERG PROD TABLE
-- ============================================================================

-- Load the data into the iceberg prod table
-- ADD_FILES_COPY: Snowflake performs a server-side copy of the original Parquet files into the base location
-- of the Iceberg table, then registers the files to the table.
-- ADD_FILES_REFERENCE: Snowflake directly reference the original file locations in the table without copying the data.
COPY INTO PROD.SMART_METER_READINGS
  FROM @energy_s3_stage/smart_meter/
  FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE)
  -- LOAD_MODE = ADD_FILES_COPY
  LOAD_MODE = ADD_FILES_REFERENCE
  MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
  -- PURGE = True // Automatically remove the data files from the original location 
  ;

-- ============================================================================
-- SIMPLE DATA MASKING - SMART METER READINGS
-- ============================================================================

USE DATABASE ENERGY;
USE SCHEMA PROD;
USE ROLE ACCOUNTADMIN;
SET USERNAME = (SELECT CURRENT_USER());

-- ============================================================================
-- PART 1: CREATE ROLES
-- ============================================================================

CREATE ROLE IF NOT EXISTS ANALYST;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST;
GRANT USAGE ON DATABASE ENERGY TO ROLE ANALYST;
GRANT USAGE ON SCHEMA PROD TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA PROD TO ROLE ANALYST;

GRANT ROLE ANALYST TO USER IDENTIFIER($USERNAME);

-- ============================================================================
-- PART 2: CREATE MASKING POLICIES
-- ============================================================================

-- Mask identifiers (show last 4 chars)
CREATE OR REPLACE MASKING POLICY mask_id AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    ELSE '****' || RIGHT(val, 4)
  END;

-- Mask measurements (round to 0.5)
CREATE OR REPLACE MASKING POLICY mask_measurement AS (val FLOAT) RETURNS FLOAT ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    ELSE ROUND(val * 2) / 2
  END;

-- ============================================================================
-- PART 3: APPLY MASKING TO COLUMNS
-- ============================================================================

ALTER ICEBERG TABLE SMART_METER_READINGS MODIFY COLUMN METER_ID
  SET MASKING POLICY mask_id;

ALTER ICEBERG TABLE SMART_METER_READINGS MODIFY COLUMN CONSUMPTION_KWH
  SET MASKING POLICY mask_measurement;

ALTER ICEBERG TABLE SMART_METER_READINGS MODIFY COLUMN COST_USD
  SET MASKING POLICY mask_measurement;

-- ============================================================================
-- PART 4: TEST THE MASKING
-- ============================================================================

-- As ACCOUNTADMIN (sees real values)
USE ROLE ACCOUNTADMIN;
SELECT 
    METER_ID,           -- MTR351343
    CONSUMPTION_KWH,    -- 0.858
    COST_USD            -- 0.10
FROM SMART_METER_READINGS
LIMIT 5;

-- As ANALYST (sees masked values)
USE ROLE ANALYST;
SELECT 
    METER_ID,           -- ****1343
    CONSUMPTION_KWH,    -- 1.0
    COST_USD            -- 0.0
FROM SMART_METER_READINGS
LIMIT 5;

-- ============================================================================
-- SETUP SNOWFLAKE INTELLIGENCE
-- ============================================================================

USE ROLE ACCOUNTADMIN;
SET USERNAME = (SELECT CURRENT_USER());

-- Create database for Snowflake Intelligence
CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
GRANT USAGE ON DATABASE snowflake_intelligence TO ROLE PUBLIC;

-- Create schema for agents
CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;
GRANT USAGE ON SCHEMA snowflake_intelligence.agents TO ROLE PUBLIC;

-- Grant agent creation privilege
GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE ACCOUNTADMIN;

-- Set default role and warehouse (required)
ALTER USER identifier($USERNAME) SET DEFAULT_ROLE = ACCOUNTADMIN;
ALTER USER identifier($USERNAME) SET DEFAULT_WAREHOUSE = compute_wh;