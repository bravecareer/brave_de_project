{{ config(
   materialized='incremental',
   unique_key=['product_id','product_name'],
   cluster_by='product_id'  
   
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
    FROM  {{ source('de_project', 'product_data') }}  --Filter out columns which are not required
)
SELECT
    se.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.rating,
    HAS_PDP ,
    HAS_ATC,
    HAS_PURCHASE,
    SEARCH_RESULTS_COUNT,  
    USER_ID,  
    se.SEARCH_EVENT_ID,     
    p.price,
    current_timestamp() AS dbt_loaded_at,
    'user_journey_transform_BS' AS dbt_source
FROM search_events se
JOIN valid_products p ON se.product_id = p.product_id
WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
TRUE
{% endif %}
