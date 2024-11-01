{{ config(
   materialized='incremental',
   unique_key='user_id'
) }}

WITH user_cleaned AS (
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
   FROM {{ source('de_project', 'user_data') }}
   WHERE user_id IS NOT NULL
     AND account_status IN ('active', 'inactive', 'banned')
)

SELECT * FROM user_cleaned
{% if is_incremental() %}
    WHERE user_id NOT IN (SELECT user_id FROM {{ this }})
{% endif %}
