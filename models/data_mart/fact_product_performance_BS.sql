{{ config(
   materialized='incremental',
   unique_key=['product_id','product_name'],
   cluster_by=['product_id']
   
) }}

WITH search_events AS (
    SELECT *
    FROM {{ref('product_metrics_transformation_BS')}}
    
),

valid_products AS (
    SELECT
        PRODUCT_ID, 
        PRODUCT_NAME, 
        PRICE,  
        RATING, 
        DISCOUNT_PERCENTAGE
    FROM  {{ source('de_project', 'product_data') }}  --Filter out columns whichg are not required
)
SELECT
    se.PRODUCT_ID,
    p.PRODUCT_NAME,
    se.total_searches,
    se.searches_with_pdp as pdp_views,
    se.searches_with_atc AS total_atc_events,
    p.rating,
    se.searches_with_purchase AS total_purchases,     
    ROUND(se.searches_with_purchase * p.PRICE, 2) AS revenue_wo_discount,
    ROUND(se.searches_with_purchase * (p.PRICE * (1 - p.DISCOUNT_PERCENTAGE / 100)), 2) AS revenue_after_discount,
    current_timestamp() AS dbt_loaded_at,
    'user_journey_transform_BS' AS dbt_source
FROM search_events se
JOIN valid_products p ON se.product_id = p.product_id
WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
1=1
{% endif %}
ORDER BY revenue_after_discount DESC