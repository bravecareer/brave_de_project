{{ config(
   materialized='incremental',
   unique_key =['SEARCH_TERMS','PRODUCT_ID'],
   cluster_by=['SEARCH_TERMS','PRODUCT_ID']
   
) }}

WITH search_events AS(
    SELECT 
        *
    FROM {{ ref('user_journey_transformation_BS') }} -- Reference to the user_journey_transformation table
    
),
    valid_products AS (
    SELECT
       p.product_id, p.product_name
    FROM {{ source('de_project', 'product_data') }} p  --Early filtering
)
 
   SELECT 
        se.SEARCH_TERMS,
        se.SEARCH_MODEL,
        se.PRODUCT_ID,
        p.product_name,
        se.total_searches,
        se.searches_with_pdp,
        se.searches_with_atc,
        se.searches_with_purchase,
        se.average_search_result_count,
        se.searches_by_registered_users,
        se.searches_by_un_registered_users,
        current_timestamp() AS dbt_loaded_at,
        'user_journey_transform_BS' AS dbt_source
    FROM search_events se
    JOIN valid_products p 
        ON se.product_id = p.product_id   
   
    WHERE
{% if is_incremental() %}
    dbt_loaded_at > (SELECT max(dbt_loaded_at) FROM {{ this }})
{% else %}
  TRUE   --INCASE  Full refresh dbt run 
{% endif %}
