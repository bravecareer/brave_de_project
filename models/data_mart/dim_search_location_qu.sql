{{ config(
   materialized='incremental',
   unique_key='location_id'
) }}

with source as (

   select * from  {{ ref ('stg_user_journey_qu') }}

),

search_location_data AS (
   SELECT
       
      {{dbt_utils.generate_surrogate_key(['country','city',
                'region', 'zipcode'])}} as location_id, 
      country,
      city,
      region,
      zipcode

   FROM source
   GROUP BY city, country, region, zipcode
)

select * from search_location_data