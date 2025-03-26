{{ config(
   materialized='table',
   unique_key='user_id'
) }}


WITH user_data AS (
   SELECT
       DISTINCT(u.user_id),
       CONCAT(u.first_name, ' ', u.last_name) AS full_name,
       CASE WHEN u.email not like '%_@__%.__%' THEN NULL ELSE u.email END AS email,
       u.signup_date,
       u.preferred_language,
       u.dob,
       u.marketing_opt_in,
       u.account_status,
       u.loyalty_points_balance
   FROM {{ ref('stg_user_data_ba') }} u
   WHERE u.user_id != 'user_id'
)


SELECT * FROM user_data