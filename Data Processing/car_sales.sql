--1. EXPLORE RAW DATA
-- Preview dataset (first 10 rows)
SELECT *
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
LIMIT 10;

-- Count total number of records
SELECT COUNT(*) AS total_rows
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES";

-- 2. CHECK DISTINCT VALUES FOR KEY COLUMNS
-- Distinct vehicle years
SELECT DISTINCT year
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
ORDER BY year ASC;

-- Distinct manufacturers
SELECT DISTINCT make
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
ORDER BY make;

-- Distinct vehicle models
SELECT DISTINCT model
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
ORDER BY model;

-- Distinct trim levels
SELECT DISTINCT trim
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
ORDER BY trim;

-- Distinct body types
SELECT DISTINCT body
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
ORDER BY body;

-- Distinct transmission types
SELECT DISTINCT transmission
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
ORDER BY transmission;

-- Distinct sale states
SELECT DISTINCT state
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
ORDER BY state;

-- Distinct exterior colors
SELECT DISTINCT color
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
ORDER BY color;

-- Distinct interior types/colors
SELECT DISTINCT interior
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
ORDER BY interior;

-- Distinct sellers
SELECT DISTINCT seller
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
ORDER BY seller;

-- 3. SUMMARY STATISTICS FOR NUMERIC COLUMNS
-- Most common car brands by count
SELECT 
    make,
    COUNT(*) AS count
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES"
GROUP BY make
ORDER BY count DESC;

-- Odometer statistics
SELECT
    MIN(odometer) AS min_odometer,
    AVG(odometer) AS avg_odometer,
    MAX(odometer) AS max_odometer
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES";

-- Condition statistics
SELECT
    MIN(condition) AS min_condition,
    AVG(condition) AS avg_condition,
    MAX(condition) AS max_condition
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES";

-- 4. BACKUP DATASET (SAFETY COPY BEFORE CLEANING)

CREATE TABLE "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES_backup" AS
SELECT *
FROM "CAR_SALES_ANALYSIS"."RAW"."CAR_SALES";

-- 5. IDENTIFY ROWS WITH MISSING OR EMPTY FIELDS

SELECT *
FROM CAR_SALES_ANALYSIS.RAW.CAR_SALES
WHERE 
    vin IS NULL OR TRIM(vin) = '' OR
    make IS NULL OR TRIM(make) = '' OR
    model IS NULL OR TRIM(model) = '' OR
    sellingprice IS NULL OR TRIM(sellingprice) = '' OR
    saledate IS NULL OR TRIM(saledate) = '';

-- 6. DELETE INVALID OR INCOMPLETE ROWS

DELETE FROM CAR_SALES_ANALYSIS.RAW.CAR_SALES
WHERE 
    vin IS NULL OR TRIM(vin) = '' OR
    make IS NULL OR TRIM(make) = '' OR
    model IS NULL OR TRIM(model) = '' OR
    sellingprice IS NULL OR TRIM(sellingprice) = '' OR
    saledate IS NULL OR TRIM(saledate) = '';

-- 7. CLEAN NUMERIC FIELDS (REMOVE COMMAS, CONVERT TO NUMBER)

SELECT
    TRY_TO_NUMBER(REPLACE(sellingprice, ',', '')) AS selling_price_clean,
    TRY_TO_NUMBER(REPLACE(costprice, ',', '')) AS cost_price_clean
FROM CAR_SALES_ANALYSIS.RAW.CAR_SALES;

-- 8. CLEAN + TRANSFORM VEHICLE SALES DATA

WITH cleaned AS (
    SELECT
        -- Core vehicle fields
        year,
        make,
        model,
        trim,
        body,
        transmission,
        vin,
        state,
        condition,
        odometer,
        color,
        interior,
        seller,

        -- Clean MMR and selling price
        TRY_TO_NUMBER(mmr) AS mmr_clean,
        TRY_TO_NUMBER(sellingprice) AS sellingprice_num,

        -- Parse sale date and time
        TO_DATE(saledate, 'YYYY/MM/DD') AS sale_date,
        CAST(saletime AS TIME) AS sale_time

    FROM CAR_SALES_ANALYSIS.RAW.CAR_SALES
    WHERE
        -- Filter out missing or invalid values
        year IS NOT NULL
        AND make IS NOT NULL
        AND model IS NOT NULL
        AND sellingprice IS NOT NULL
        AND mmr IS NOT NULL
        AND vin IS NOT NULL
        AND interior IS NOT NULL AND interior != '—'
        AND color IS NOT NULL AND color != '—'
        AND transmission IS NOT NULL
        AND condition IS NOT NULL
        AND body IS NOT NULL
)

, with_profit AS (
    SELECT
        *,
        -- Calculate profit margin
        CASE
            WHEN sellingprice_num > 0 THEN
                ROUND((sellingprice_num - mmr_clean) / sellingprice_num, 4)
            ELSE 0
        END AS profit_margin,

        -- Each VIN = 1 unit sold
        1 AS units_sold,

        -- Revenue per row
        sellingprice_num * 1 AS total_revenue,

        -- Categorize vehicle condition
        CASE
            WHEN condition >= 40 THEN 'EXCELLENT'
            WHEN condition >= 25 THEN 'GOOD'
            WHEN condition >= 10 THEN 'FAIR'
            ELSE 'POOR'
        END AS condition_category
    FROM cleaned
)
-- 9. FINAL OUTPUT WITH TIME, MARGIN, AND DATE FEATURES

SELECT
    *,
    YEAR(sale_date) AS sale_year,
    MONTHNAME(sale_date) AS sale_month,
    QUARTER(sale_date) AS sale_quarter,

    -- Categorize profit margin levels
    CASE
        WHEN profit_margin >= 0.20 THEN 'HIGH MARGIN'
        WHEN profit_margin >= 0.10 THEN 'MEDIUM MARGIN'
        ELSE 'LOW MARGIN'
    END AS margin_tier

FROM with_profit
ORDER BY sale_date, sale_time;
