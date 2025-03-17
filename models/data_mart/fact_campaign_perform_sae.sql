{{ config(
    materialized='incremental',
    schema='PROJECT_TEST',
    unique_key=['campaign_id', 'search_date'],
    cluster_by=['campaign_id', 'search_date']
) }}

WITH campaign_data AS (
  SELECT 
    LOWER(TRIM(MKT_CAMPAIGN)) AS campaign_id,
    CAST(event_timestamp AS DATE) AS search_date,
    has_atc,
    has_purchase,
    load_timestamp
  FROM {{ ref('stg_user_journey_sae') }}
  WHERE MKT_CAMPAIGN IS NOT NULL
),

aggregated AS (
  SELECT
    campaign_id,
    search_date,
    COUNT(*) AS total_interactions,
    SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) AS atc_events,
    SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS purchase_events,
    MAX(load_timestamp) AS last_updated
  FROM campaign_data
  GROUP BY campaign_id, search_date
),

-- Deduplicate dim_campaign_sae in case there are duplicates.
deduped_dim AS (
  SELECT 
    campaign_id, 
    campaign_source, 
    campaign_medium,
    ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY last_updated DESC) AS rn
  FROM {{ ref('dim_campaign_sae') }}
)

SELECT
  agg.campaign_id,
  dd.campaign_source,
  dd.campaign_medium,
  agg.search_date,
  agg.total_interactions,
  agg.atc_events,
  agg.purchase_events,
  agg.last_updated
FROM aggregated AS agg
LEFT JOIN deduped_dim AS dd
  ON agg.campaign_id = dd.campaign_id
WHERE dd.rn = 1