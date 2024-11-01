{{ config(
   materialized='incremental',
   unique_key='product_id'
) }}

SELECT
   product_id,
   COUNT(has_qv) AS total_qv_events,
   COUNT(has_pdp) AS total_pdp_views,
   COUNT(has_atc) AS total_atc_events,
   COUNT(has_purchase) AS quantity_sold
FROM {{ ref('stg_user_journey') }}
GROUP BY product_id
