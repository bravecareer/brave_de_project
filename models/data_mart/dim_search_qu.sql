{{ config(
   materialized='incremental',
   unique_key='search_data_id'
) }}

with source as (

   select * from  {{ ref ('stg_user_journey_qu') }}

),

search_data AS (
   SELECT
      
     {{dbt_utils.generate_surrogate_key(['search_model',
                  'search_type','search_feature',
                  'search_terms_type'])}} as search_data_id,
      search_model,  
      search_type,
      search_feature,
      search_terms_type
        
   FROM source
   GROUP BY search_model, 
            search_type, 
            search_feature, 
            search_terms_type
)


SELECT * FROM search_data