{{ config(
   materialized='incremental',
   unique_key='user_id'
) }}

with source as (

   select * from {{ ref( 'stg_user_data_qu') }}
),


user_data AS (
   SELECT
      
      user_id,
      first_name,
      last_name,
      email,
      signup_date,
      preferred_language,
      dob,
      marketing_opt_in,
      account_status,
      loyalty_points_balance
   FROM source
   WHERE account_status = 'active'
)

SELECT * FROM user_data