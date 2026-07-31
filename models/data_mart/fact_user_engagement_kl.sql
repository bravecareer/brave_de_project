/*
****************************************************************************************************
MODEL: fact_user_engagement_kl
PURPOSE: 
    - Enrich core user interaction data with geographical, marketing, and device-class dimensions.
    - Support multi-channel marketing impact and territory management analysis.
SOURCE: 
    - raw.user_journey
    - raw.user_data (Filtering for active accounts)
    - raw.product_data (Validating product catalog alignment)
BUSINESS VALUE:
    - Provides visibility into "where" (GEO) and "how" (DEVICE) users are engaging.
    - Connects search behavior (SEARCH_TERMS) to marketing initiatives (MKT_CAMPAIGN).
    - Enables profile-based conversion analysis for Sales and Product teams.
****************************************************************************************************
*/

{{ config(
    materialized='incremental',
    unique_key=['user_id', 'product_id', 'search_event_id', 'timestamp']
) }}

-- Step 1: Broaden raw journey data by adding your requested business dimensions
WITH engagement_base AS (
    SELECT
        uj.user_id,
        uj.product_id,
        uj.search_event_id,
        uj.session_id,
        -- Standardize timestamp for downstream analysis
        to_timestamp(replace(uj.timestamp,' UTC','')) AS timestamp,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        -- Geography for Territory Management
        uj.geo_country,
        uj.geo_zipcode,
        -- Marketing for Multi-Channel Analysis
        uj.search_terms,        -- ✅ 修正: search_terms (复数)
        uj.mkt_campaign,
        uj.mkt_source,
        -- Platform for Customer Profile Analysis
        uj.device_class
    FROM {{ source('de_project', 'user_journey') }} uj
),

-- Step 2: Validate against active user accounts
valid_users AS (
    SELECT
        u.user_id
    FROM {{ source('de_project', 'user_data') }} u
    WHERE u.account_status = 'active'
),

-- Step 3: Validate against existing product master data
valid_products AS (
    SELECT
        p.product_id
    FROM {{ source('de_project', 'product_data') }} p
),

-- Step 4: Final join to ensure data quality and integrity
final_engagement AS (
    SELECT
        eb.*
    FROM engagement_base eb
    INNER JOIN valid_users vu ON eb.user_id = vu.user_id
    INNER JOIN valid_products vp ON eb.product_id = vp.product_id
)

SELECT * FROM final_engagement