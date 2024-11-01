{{ config(
   materialized='table',
   unique_key='user_id'
) }}

SELECT 
   user_id,
   first_name,
   last_name,
   email,
   signup_date,
   account_status
FROM {{ ref('stg_user_data') }}
WHERE account_status IS NOT NULL
