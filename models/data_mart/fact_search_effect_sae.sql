{{ config(
    materialized='incremental',
    schema='PROJECT_TEST',
    unique_key='search_event_id',
    cluster_by=['user_id', 'search_date']
) }}

WITH base AS (
  SELECT 
    search_event_id,
    event_timestamp,
    CAST(event_timestamp AS DATE) AS search_date,
    user_id,
    product_id,
    has_qv,
    has_pdp,
    has_atc,
    has_purchase,
    search_results_count,
    session_id,
    search_request_id,
    load_timestamp
  FROM {{ ref('stg_user_journey_sae') }}
  WHERE search_event_id IS NOT NULL
),

aggregated AS (
  -- For a given search event, we can derive a journey score and capture event metrics
  SELECT 
    search_event_id,
    user_id,
    product_id,
    search_date,
    MAX(event_timestamp) AS last_event_timestamp,
    SUM(CASE WHEN has_qv THEN 1 ELSE 0 END) AS qv_count,
    SUM(CASE WHEN has_pdp THEN 1 ELSE 0 END) AS pdp_count,
    SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) AS atc_count,
    SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS purchase_count,
    -- Example journey score (you can adjust weights as needed)
    (SUM(CASE WHEN has_qv THEN 1 ELSE 0 END) * 1 +
     SUM(CASE WHEN has_pdp THEN 1 ELSE 0 END) * 2 +
     SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) * 3 +
     SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) * 4) AS journey_score,
    MAX(load_timestamp) AS load_timestamp
  FROM base
  GROUP BY search_event_id, user_id, product_id, search_date
)

SELECT *
FROM aggregated
