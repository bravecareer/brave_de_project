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
    TRIM(SEARCH_TERMS) AS search_terms,
    load_timestamp
  FROM {{ ref('stg_user_journey_sae') }}
  WHERE search_event_id IS NOT NULL
),

aggregated AS (
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
    (SUM(CASE WHEN has_qv THEN 1 ELSE 0 END) * 1 +
     SUM(CASE WHEN has_pdp THEN 1 ELSE 0 END) * 2 +
     SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) * 3 +
     SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) * 4) AS journey_score,
    MAX(load_timestamp) AS load_timestamp,
    MAX(search_terms) AS search_terms
  FROM base
  GROUP BY search_event_id, user_id, product_id, search_date
),

-- Deduplicate dim_user_sae.
deduped_user AS (
  SELECT 
    user_id,
    full_name,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY load_timestamp DESC) AS rn
  FROM {{ ref('dim_user_sae') }}
),

-- Deduplicate dim_product_sae.
deduped_product AS (
  SELECT 
    product_id,
    product_name,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY load_timestamp DESC) AS rn
  FROM {{ ref('dim_product_sae') }}
),

-- Deduplicate dim_search_terms_sae.
deduped_search_terms AS (
  SELECT 
    search_term,
    search_term_id,
    ROW_NUMBER() OVER (PARTITION BY search_term ORDER BY search_term_id) AS rn
  FROM {{ ref('dim_search_terms_sae') }}
)

SELECT
  agg.search_event_id,
  agg.user_id,                        -- FK to dim_user_sae
  du.full_name,
  agg.product_id,                     -- FK to dim_product_sae
  dp.product_name,
  agg.search_date,
  agg.last_event_timestamp,
  agg.qv_count,
  agg.pdp_count,
  agg.atc_count,
  agg.purchase_count,
  agg.journey_score,
  agg.load_timestamp,
  dst.search_term_id,                 -- FK to dim_search_terms_sae
  dst.search_term,
  dst.intent_category
FROM aggregated AS agg
LEFT JOIN deduped_user AS du
  ON agg.user_id = du.user_id
LEFT JOIN deduped_product AS dp
  ON agg.product_id = dp.product_id
LEFT JOIN deduped_search_terms AS dst
  ON LOWER(agg.search_terms) = LOWER(dst.search_term)
WHERE du.rn = 1
  AND dp.rn = 1
  AND dst.rn = 1