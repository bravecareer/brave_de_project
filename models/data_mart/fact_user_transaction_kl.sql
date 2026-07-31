/*
****************************************************************************************************
MODEL: fact_user_transaction_kl
PURPOSE: 
    - Consolidate purchase events with product pricing to calculate revenue.
    - Analyze "Deal Size" and customer basket composition at a daily grain.
    - Support cross-selling analysis via product category aggregation.
SOURCE: 
    - raw.user_journey (Filter: has_purchase = TRUE)
    - raw.product_data (For price and category enrichment)
BUSINESS VALUE:
    - Identifies High/Medium/Low value transactions (Deal Size).
    - Enables "Products per Order" metrics (Product Count).
    - Cross Selling Detection by using LISTAGG.
    - Provides visibility into multi-category purchasing behavior (Category List).
****************************************************************************************************
*/

{{ config(
    materialized='incremental',
    unique_key=['user_id', 'product_id', 'timestamp']
) }}

-- Step 1: Extract successful transactions and join with product metadata
WITH base_transactions AS (
    SELECT
        uj.user_id,
        uj.product_id,
        uj.search_event_id,
        uj.session_id,
        uj.cart_id,
        -- Standardize timestamp for window function calculations not within 'UTC' for future category by 'Date'
        to_timestamp(replace(uj.timestamp,' UTC','')) AS timestamp,
        p.product_category,
        p.price
    FROM {{ source('de_project', 'user_journey') }} uj
    LEFT JOIN {{ source('de_project', 'product_data') }} p 
      ON uj.product_id = p.product_id
    WHERE uj.has_purchase = TRUE
),

-- Step 2: Calculate aggregate metrics per user/day using Window Functions
-- This preserves individual row detail while adding group-level context
transaction_aggregation AS (
    SELECT
        *,
        -- Total spend per user per day to determine deal size
        SUM(price) OVER (PARTITION BY user_id, CAST(timestamp AS DATE)) AS daily_user_spend,
        
        -- Count of unique products purchased by user on this date
        COUNT(DISTINCT product_id) OVER (PARTITION BY user_id, CAST(timestamp AS DATE)) AS products_count,
        
        -- Concatenate unique categories purchased by user on this date
        LISTAGG(DISTINCT product_category, ', ') OVER (PARTITION BY user_id, CAST(timestamp AS DATE)) AS product_categories
    FROM base_transactions
)

-- Step 3: Final output with Deal Size classification logic
SELECT
    user_id,
    product_id,
    timestamp,
    search_event_id,
    session_id,
    cart_id,
    price AS item_price,
    daily_user_spend AS total_amount,
    products_count,
    product_categories,
    -- Segmentation based on daily total spend
    CASE 
        WHEN daily_user_spend >= 500 THEN 'Large'
        WHEN daily_user_spend BETWEEN 100 AND 499 THEN 'Medium'
        ELSE 'Small'
    END AS deal_size
FROM transaction_aggregation