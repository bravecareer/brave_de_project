{{ config(
    materialized = 'incremental',
    unique_key = ['product_id', 'campaign_id', 'user_id']
)}}

WITH user_journey AS (

    select * from {{ref('stg_user_journey_qu') }}
),

valid_users as (

   select * from {{ ref('stg_user_data_qu') }}
   WHERE account_status = 'active'

),

product_data AS (

    select * from {{ ref('stg_product_data_qu') }} 
),

campaign_dim AS (
   SELECT * FROM {{ ref('dim_campaign_qu') }}
),

filteirng AS (
   SELECT
      
      p.price,
      uj.has_purchase,
      uj.has_atc,
      uj.has_pdp,
      uj.has_qv,
      uj.product_id,
      uj.user_id,
      cd.campaign_id -- Foreign key from dim_campaign
    
   FROM user_journey uj
   INNER JOIN campaign_dim cd
     ON uj.campaign_name = cd.campaign_name
    AND uj.medium = cd.medium
    AND uj.source = cd.source
    AND uj.content = cd.content

   INNER JOIN valid_users vu 
     ON uj.user_id = vu.user_id
   
   LEFT JOIN product_data p
     ON uj.product_id = p.product_id
   
   GROUP BY 
      cd.campaign_id,
      uj.product_id, 
      uj.user_id,
      p.price,
      uj.has_purchase,
      uj.has_atc,
      uj.has_pdp,
      uj.has_qv

),

final AS (
    SELECT
       
      campaign_id,
      product_id,
      price AS product_price,
      SUM(CASE WHEN has_purchase = TRUE THEN 1 ELSE 0 END) AS total_unit_sold,
      SUM(CASE WHEN has_atc = TRUE THEN 1 ELSE 0 END) AS total_atc,
      SUM(CASE WHEN has_qv = TRUE THEN 1 ELSE 0 END) AS total_qv,
      SUM(CASE WHEN has_pdp = TRUE THEN 1 ELSE 0 END) AS total_pdp,
      user_id

    FROM filteirng
    GROUP BY 
      campaign_id,
      product_id,
      user_id,
      price
      
)



SELECT * FROM final