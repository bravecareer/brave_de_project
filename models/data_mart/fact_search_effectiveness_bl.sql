{{ config(
   materialized='incremental',
   unique_key='product_id'
) }}


WITH search_effectiveness AS (
   SELECT
       uj.product_id,
       p.product_name,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.search_event_id
   FROM {{ ref('stg_user_journey_bl') }} uj
   LEFT JOIN {{ ref('dim_product_bl') }} p
     ON uj.product_id = p.product_id
)


SELECT
   product_id,
   product_name,
   COUNT(has_qv) AS views,
   COUNT(has_pdp) AS detail_views,
   COUNT(has_atc) AS atc_events,
   COUNT(has_purchase) AS purchases,
   COUNT(search_event_id) AS searches
 FROM search_effectiveness
GROUP BY product_id, product_name