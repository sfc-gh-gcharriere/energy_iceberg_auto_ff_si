-- ============================================================================
-- SIMPLE DATA MASKING - SMART METER READINGS
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE ENERGY;
USE SCHEMA DEV;

-- ============================================================================
-- PART 1: CREATE MASKING POLICIES
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

ALTER ICEBERG TABLE DEV.SMART_METER_READINGS MODIFY COLUMN METER_ID
  SET MASKING POLICY mask_id;

ALTER ICEBERG TABLE DEV.SMART_METER_READINGS MODIFY COLUMN CONSUMPTION_KWH
  SET MASKING POLICY mask_measurement;

ALTER ICEBERG TABLE DEV.SMART_METER_READINGS MODIFY COLUMN COST_USD
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