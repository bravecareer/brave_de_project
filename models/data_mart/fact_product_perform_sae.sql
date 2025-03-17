{{ config(
    materialized='incremental',
    schema='PROJECT_TEST',
    unique_key=['product_id', 'search_date'],
    cluster_by=['product_id', 'search_date']
) }}

WITH base AS (
  SELECT 
    product_id,
    CAST(event_timestamp AS DATE) AS search_date,
    has_atc,
    has_purchase,
    load_timestamp
  FROM {{ ref('stg_user_journey_sae') }}
  WHERE product_id IS NOT NULL
),

aggregated AS (
  SELECT
    product_id,
    search_date,
    COUNT(*) AS total_views,
    SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) AS atc_events,
    SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS purchase_events,
    MAX(load_timestamp) AS last_updated
  FROM base
  GROUP BY product_id, search_date
)

SELECT *
FROM aggregated
