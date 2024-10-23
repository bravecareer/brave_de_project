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

final AS (
   SELECT
  
      uj.search_event_id,
      uj.user_id,
      uj.product_id,
      
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
   LEFT JOIN valid_users vu ON uj.user_id = vu.user_id
   LEFT JOIN products vp ON uj.product_id = vp.product_id
   
)


SELECT * FROM final