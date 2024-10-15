{{ config(
   materialized='incremental',
   unique_key=['campaign_id', 'search_event_id']
) }}

WITH user_journey AS (
   SELECT
       uj.search_event_id,
       uj.user_id,
       uj.has_atc,
       uj.has_purchase,
       uj.product_id,
       uj.mkt_campaign AS campaign_id
   FROM {{ source('de_project', 'user_journey') }} uj
   WHERE uj.mkt_campaign IS NOT NULL
),

campaign_performance AS (
   SELECT
      uj.campaign_id,
      COUNT(uj.search_event_id) AS total_views,
      SUM(CASE WHEN uj.has_atc THEN 1 ELSE 0 END) AS total_atc_events,
      SUM(CASE WHEN uj.has_purchase THEN 1 ELSE 0 END) AS total_purchases
   FROM user_journey uj
   GROUP BY uj.campaign_id
)

SELECT * FROM campaign_performance
