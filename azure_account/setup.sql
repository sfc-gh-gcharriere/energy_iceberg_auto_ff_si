-- ============================================================================
-- SNOWFLAKE INITIAL SETUP FOR ENERGY DEMO
-- ============================================================================

-- enable cross-region
USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ============================================================================
-- PART 1: CREATE DATABASE AND SCHEMA
-- ============================================================================

CREATE DATABASE IF NOT EXISTS SALES;
CREATE SCHEMA IF NOT EXISTS PROD;

-- ============================================================================
-- PART 2: CREATE INTERNAL STAGE FOR PARQUET FILES
-- ============================================================================

CREATE OR REPLACE STAGE sales_internal_stage
  COMMENT = 'Internal stage for sales parquet files';

-- ============================================================================
-- PART 3: UPLOAD FILES TO INTERNAL STAGE (via SnowSQL or UI)
-- ============================================================================

-- load parquet files into the stage using SnowSight UI

-- List files in stage to verify upload
LIST @sales_internal_stage;


-- ============================================================================
-- PART 4: CREATE SALES TABLES
-- ============================================================================

-- Sales Representatives table
CREATE OR REPLACE TABLE SALES_REPRESENTATIVES (
  rep_id STRING NOT NULL,
  rep_name STRING,
  email STRING,
  phone STRING,
  territory STRING,
  hire_date DATE,
  commission_rate FLOAT
);

-- Products table
CREATE OR REPLACE TABLE PRODUCTS (
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  base_price FLOAT,
  description STRING
);

-- Transactions table
CREATE OR REPLACE TABLE TRANSACTIONS (
  transaction_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  rep_id STRING,
  product_id STRING,
  transaction_date TIMESTAMP,
  quantity INTEGER,
  unit_price FLOAT,
  discount_percent float,
  total_amount FLOAT,
  payment_method STRING,
  transaction_status STRING,
  channel string
);

-- Customer Interactions table
CREATE OR REPLACE TABLE CUSTOMER_INTERACTIONS (
  interaction_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  rep_id STRING,
  interaction_date TIMESTAMP,
  interaction_type STRING,
  reason STRING,
  duration_minutes  NUMBER(38,0),
  resolution STRING,
  satisfaction_score NUMBER(38,0),
  notes STRING
);

-- ============================================================================
-- PART 5: LOAD DATA FROM INTERNAL STAGE
-- ============================================================================
    
-- Load Sales Representatives
COPY INTO SALES_REPRESENTATIVES
FROM @sales_internal_stage/sales_representatives.parquet
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT';

SELECT COUNT(*) as rep_count FROM SALES_REPRESENTATIVES;
select * from sales_representatives;

-- Load Products
COPY INTO PRODUCTS
FROM @sales_internal_stage/products.parquet
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT';

SELECT COUNT(*) FROM PRODUCTS;
select * from PRODUCTS;

-- Load Transactions
COPY INTO TRANSACTIONS (
    transaction_id,
    customer_id,
    rep_id,
    product_id,
    transaction_date,
    quantity,
    unit_price,
    discount_percent,
    total_amount,
    payment_method,
    transaction_status,
    channel
)
FROM (
  SELECT  
    $1:transaction_id::STRING,
    $1:customer_id::STRING,
    $1:rep_id::STRING,
    $1:product_id::STRING,
    TO_TIMESTAMP($1:transaction_date::NUMBER / 1000000000),
    $1:quantity::NUMBER,
    $1:unit_price::FLOAT,
    $1:discount_percent::FLOAT,
    $1:total_amount::FLOAT,
    $1:payment_method::STRING,
    $1:transaction_status::STRING,
    $1:channel::STRING
  FROM @sales_internal_stage/transactions.parquet
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'ABORT_STATEMENT';

SELECT COUNT(*) as transaction_count FROM TRANSACTIONS;
select * from transactions;


-- Load Customer Interactions
COPY INTO CUSTOMER_INTERACTIONS (
    interaction_id,
    customer_id,
    rep_id,
    interaction_date,
    interaction_type,
    reason,
    duration_minutes,
    resolution,
    satisfaction_score,
    notes
)
FROM (
  SELECT
    $1:interaction_id::STRING,
    $1:customer_id::STRING,
    $1:rep_id::STRING,
    TO_TIMESTAMP($1:interaction_date::NUMBER / 1000000000),
    $1:interaction_type::STRING,
    $1:reason::STRING,
    $1:duration_minutes::NUMBER,
    $1:resolution::STRING,
    $1:satisfaction_score::INT,
    $1:notes::STRING
  FROM @sales_internal_stage/customer_interactions.parquet
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'ABORT_STATEMENT';


SELECT COUNT(*) as interaction_count FROM CUSTOMER_INTERACTIONS;
select * from customer_interactions;

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

-- ============================================================================
-- SETUP EMAIL INTEGRATION
-- ============================================================================

USE SCHEMA snowflake_intelligence.agents;

-- Create notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION snowflake_intelligence_email_integration
  TYPE=EMAIL
  ENABLED=TRUE
  DEFAULT_SUBJECT = 'Snowflake Intelligence';

-- Create email sending procedure
CREATE OR REPLACE PROCEDURE send_email(
    recipient_email VARCHAR,
    subject VARCHAR,
    body VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_email'
AS
$$
def send_email(session, recipient_email, subject, body):
    try:
        escaped_body = body.replace("'", "''")
        session.sql(f"""
            CALL SYSTEM$SEND_EMAIL(
                'snowflake_intelligence_email_integration',
                '{recipient_email}',
                '{subject}',
                '{escaped_body}',
                'text/html'
            )
        """).collect()
        return "Email sent successfully"
    except Exception as e:
        return f"Error sending email: {str(e)}"
$$;