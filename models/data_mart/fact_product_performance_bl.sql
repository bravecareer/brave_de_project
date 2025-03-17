{{ config(
   materialized='incremental',
   unique_key='product_id'
) }}

WITH product_performance AS (
    SELECT 
       p.product_id,
       p.product_name
       p.price,
       COUNT(ue.has_qv) AS views,
       COUNT(ue.has_pdp) AS detail_views,
       COUNT(ue.has_atc) AS atc_events,
       COUNT(ue.has_purchase) AS purchases
  FROM {{ ref('fact_user_engagement_bl') }} ue 
  LEFT JOIN {{ ref('dim_product_data_bl') }} p
    ON ue.product_id = p.product_id
 GROUP BY p.product_id
 )

 SELECT * FROM product_performance