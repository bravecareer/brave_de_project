{{ config(
   materialized='incremental',
   unique_key=['search_event_id', 'product_id']
) }}

WITH user_journey AS (
   SELECT
       uj.product_id,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.search_event_id
   FROM {{ source('de_project', 'user_journey') }} uj
),

product_performance_data AS (
   SELECT
       uj.product_id,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.search_event_id,

       p.product_category,
       p.price,
       p.product_color,
       CAST(SUBSTR(p.warranty_period, 1, 2) AS INTEGER) AS warranty_period_in_month,
       p.rating,
       p.weight_grams,
       p.discount_percentage
   FROM user_journey uj
   LEFT JOIN {{ source('de_project', 'product_data') }} p
     ON uj.product_id = p.product_id
)

SELECT * FROM product_performance_data
