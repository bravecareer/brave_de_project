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
       TRY_CAST(u.signup_date AS date) AS signup_date,
       u.preferred_language,
       TRY_CAST(u.dob AS date) AS dob,
       TRY_CAST(u.marketing_opt_in AS boolean) AS marketing_opt_in,
       u.account_status,
       TRY_CAST(u.loyalty_points_balance AS int) AS loyalty_points_balance
   FROM {{ source('de_project', 'user_data') }} u
   WHERE u.account_status = 'active'
)


SELECT * FROM user_data
{% if is_incremental() %}
-- On incremental runs, only process new users allowing a few days for late-arriving facts
WHERE signup_date >= (SELECT DATEADD(day, -3, max(signup_date)) from {{ this }})
{% endif %}