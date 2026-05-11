/*
****************************************************************************************************
MODEL: dim_post_sales_pipeline_kl
PURPOSE: 
    - Track the progression of a user/product combination through the sales funnel for completed transactions.
    - Measure the time duration (velocity) between key milestones from discovery to purchase.
SOURCE: 
    - raw.user_journey
BUSINESS VALUE:
    - Analyzes the efficiency of the conversion path for successful sales.
    - Provides historical velocity data to understand typical customer decision cycles.
    - Helps Marketing and Product teams identify the touchpoints that lead to faster checkouts.
****************************************************************************************************
*/

{{ config(
    materialized='table'
) }}

-- Step 1: Identify the first timestamp for each milestone per user/product pair
WITH milestone_timestamps AS (
    SELECT
        user_id,
        product_id,
        -- Using MIN to find the very first touchpoint for each stage
        MIN(CASE WHEN search_event_id IS NOT NULL THEN to_timestamp(replace(timestamp,' UTC','')) END) AS search_time,  -- ✅ 修正: 通过搜索发现
        MIN(CASE WHEN has_atc = TRUE THEN to_timestamp(replace(timestamp,' UTC','')) END) AS atc_time,
        MIN(CASE WHEN has_purchase = TRUE THEN to_timestamp(replace(timestamp,' UTC','')) END) AS purchase_time
    FROM {{ source('de_project', 'user_journey') }}
    GROUP BY user_id, product_id
),

-- Step 2: Calculate metrics using the earliest touchpoint as start
pipeline_metrics AS (
    SELECT
        user_id,
        product_id,
        search_time,
        atc_time,
        purchase_time,
        DATEDIFF('minute', search_time, atc_time) AS search_to_atc_min,
        DATEDIFF('minute', atc_time, purchase_time) AS atc_to_purchase_min,
        -- Using LEAST and NVL to pick the earliest starting point between Search and ATC
        DATEDIFF('minute', 
            LEAST(NVL(search_time, atc_time), NVL(atc_time, search_time)), 
            purchase_time
        ) AS total_duration_min
    FROM milestone_timestamps
)

-- Step 3: Filter the data we need
SELECT * 
FROM pipeline_metrics
WHERE purchase_time IS NOT NULL 
  AND (search_time IS NOT NULL OR atc_time IS NOT NULL)