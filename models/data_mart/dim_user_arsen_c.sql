{{ config(
   materialized='incremental',
   unique_key='user_id'
) }}

WITH user_data AS (
   SELECT
       u.user_id,
       u.first_name,
       u.last_name,
       CASE WHEN u.email not like '%_@__%.__%' THEN NULL ELSE u.email END AS email,
       TRY_CAST(u.signup_date AS DATE) AS signup_date,
       u.preferred_language,
       TRY_CAST(u.dob AS DATE) AS dob,
       u.marketing_opt_in,
       u.account_status,
       u.loyalty_points_balance
   FROM {{ source('de_project', 'user_data') }} u
   WHERE u.signup_date != 'signup_date'
)

SELECT * FROM user_data WHERE user_id IS NOT NULL