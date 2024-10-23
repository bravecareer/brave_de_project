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
       CAST(u.signup_date AS DATE) AS signup_date,
       u.preferred_language,
       CAST(u.dob AS DATE) AS dob,
       u.marketing_opt_in,
       u.account_status,
       CAST(u.loyalty_points_balance AS INTEGER) AS loyalty_points_balance
   FROM {{ source('de_project', 'user_data') }} u
   WHERE u.user_id != 'user_id'
   AND u.user_id IS NOT NULL
   AND (u.first_name IS NOT NULL OR u.last_name IS NOT NULL)
)

SELECT * FROM user_data