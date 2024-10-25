{{ config(
   materialized='incremental',
   unique_key= ['user_id', 'search_event_id']
) }}


with user_journey as (
   select * from {{ ref('stg_user_journey_qu') }} 
),

valid_users as (

   select * from {{ ref('stg_user_data_qu') }}
   WHERE account_status = 'active'

),

products as (
   select * from {{ ref('stg_product_data_qu')}}   
),

search_data as (
   select * from {{ ref('dim_search_qu') }}
),

search_loc as (
   select * from {{ ref('dim_search_location_qu') }}
),

final AS (
   SELECT
  
      uj.search_event_id,
      uj.user_id,
      uj.product_id,
      sd.search_data_id,
      sl.location_id,

      uj.search_terms,
      uj.search_results_count,
      uj.search_timestamp, 
      
      vp.price,
      vp.rating,
      vp.sales_volume,
      vp.discount_percentage,
      vp.weight_grams,
      
      uj.has_qv,
      uj.has_pdp,
      uj.has_atc,
      uj.has_purchase

   FROM user_journey uj
   INNER JOIN search_data sd 
      ON uj.search_model = sd.search_model
      AND uj.search_type = sd.search_type
      AND uj.search_feature = sd.search_feature
      AND uj.search_terms_type = sd.search_terms_type
   INNER JOIN search_loc sl
      ON uj.country = sl.country
      AND uj.city = sl.city
      AND uj.region = sl.region
      AND uj.zipcode = sl.zipcode   

   INNER JOIN valid_users vu ON uj.user_id = vu.user_id
   LEFT JOIN products vp ON uj.product_id = vp.product_id
   
)


SELECT * FROM final