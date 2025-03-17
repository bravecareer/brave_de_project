{{ config(
    materialized='incremental',
    schema='PROJECT_TEST',
    unique_key=['campaign_id', 'search_date'],
    cluster_by=['campaign_id', 'search_date']
) }}

WITH campaign_data AS (
  SELECT 
    -- Standardize campaign name; if null, we’ll handle in the dimension
    LOWER(TRIM(MKT_CAMPAIGN)) AS campaign,
    CAST(event_timestamp AS DATE) AS search_date,
    has_atc,
    has_purchase,
    load_timestamp
  FROM {{ ref('stg_user_journey_sae') }}
  WHERE MKT_CAMPAIGN IS NOT NULL
),

aggregated AS (
  SELECT
    campaign AS campaign_id,
    search_date,
    COUNT(*) AS total_interactions,
    SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) AS atc_events,
    SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS purchase_events,
    MAX(load_timestamp) AS last_updated
  FROM campaign_data
  GROUP BY campaign, search_date
)

SELECT *
FROM aggregated
