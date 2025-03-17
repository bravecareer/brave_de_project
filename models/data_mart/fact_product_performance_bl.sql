{{ config(
   materialized='table',
   unique_key='product_id'
) }}

WITH product_performance AS (
    SELECT 
       p.product_id,
       p.product_name,
       p.price,
       COUNT(uj.has_qv) AS views,
       COUNT(uj.has_pdp) AS detail_views,
       COUNT(uj.has_atc) AS atc_events,
       COUNT(uj.has_purchase) AS purchases
  FROM {{ ref('stg_user_journey_bl') }} uj 
  LEFT JOIN {{ ref('dim_product_bl') }} p
    ON uj.product_id = p.product_id
 GROUP BY p.product_id, p.product_name, p.price
 )

 SELECT * FROM product_performance