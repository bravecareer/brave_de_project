{{ config(
   materialized='incremental',
   unique_key= 'campaign_id'
) }}

with source as (

   select * from {{ref ('stg_user_journey_qu')}}

),

campaign_data AS (
   SELECT
      --ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS campaign_id,
      {{dbt_utils.generate_surrogate_key(['campaign_name', 
               'medium', 'source', 'content'])}} AS campaign_id,
      campaign_name,
      medium,
      source,
      content
     
   FROM source
   GROUP BY campaign_name, medium, source, content
)

SELECT * FROM campaign_data
