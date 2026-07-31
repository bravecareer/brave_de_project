{{ config(
    materialized='table',
    tags=['dimension', 'core']
) }}

WITH date_spine AS (
    -- Generate date sequence from 2020 to 2030
    SELECT 
        DATEADD(
            day, 
            ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1,
            '2020-01-01'::DATE
        ) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))  -- 11 years * 365.25 days
),

date_dimension AS (
    SELECT
        -- Surrogate Key
        TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_key,
        
        -- Full Date
        date_day AS full_date,
        
        -- Year, Quarter, Month, Day
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        MONTH(date_day) AS month,
        DAY(date_day) AS day,
        DAYOFWEEK(date_day) AS day_of_week,  -- 0=Sunday, 6=Saturday
        DAYOFYEAR(date_day) AS day_of_year,
        
        -- Week
        WEEKOFYEAR(date_day) AS week_of_year,
        
        -- Month Name
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Mon') AS month_abbr,
        
        -- Day Name
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'Dy') AS day_abbr,
        
        -- Business Logic
        CASE 
            WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        -- Fiscal Year (assuming Apr 1 start)
        CASE 
            WHEN MONTH(date_day) >= 4 THEN YEAR(date_day)
            ELSE YEAR(date_day) - 1
        END AS fiscal_year,
        
        -- Fiscal Quarter
        CASE 
            WHEN MONTH(date_day) IN (4, 5, 6) THEN 1
            WHEN MONTH(date_day) IN (7, 8, 9) THEN 2
            WHEN MONTH(date_day) IN (10, 11, 12) THEN 3
            ELSE 4
        END AS fiscal_quarter,
        
        -- First/Last Day of Month
        DATE_TRUNC('month', date_day) AS first_day_of_month,
        LAST_DAY(date_day) AS last_day_of_month,
        
        -- Audit
        CURRENT_TIMESTAMP() AS loaded_at
        
    FROM date_spine
    WHERE date_day BETWEEN '2020-01-01' AND '2030-12-31'
)

SELECT * FROM date_dimension
ORDER BY date_key