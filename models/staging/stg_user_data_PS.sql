{{ config(
   materialized='incremental',
   unique_key='user_id'
) }}

WITH user_enriched AS (
   SELECT
       u.user_id,
       INITCAP(TRIM(u.first_name)) AS first_name,
       INITCAP(TRIM(u.last_name)) AS last_name,
       CASE WHEN u.email NOT LIKE '^[^@]+@[^@]+\.[^@]+$' THEN NULL ELSE LOWER(u.email) END AS email,
       TRY_CAST(u.signup_date AS DATE) AS signup_date,
       LOWER(u.preferred_language) AS preferred_language,
       TRY_CAST(u.dob AS DATE) AS dob,
       u.marketing_opt_in,
       u.account_status,
       TRY_CAST(u.loyalty_points_balance AS INTEGER) AS loyalty_points_balance
   FROM {{ source('de_project', 'user_data') }} u
   WHERE u.account_status IN ('active', 'inactive', 'banned')
     AND u.user_id IS NOT NULL
     AND LOWER(u.preferred_language) IN ('french', 'chinese', 'german', 'spanish', 'english')
     AND u.marketing_opt_in IN ('True', 'False')
)

SELECT * FROM user_enriched

{% if is_incremental() %}
    -- Ensure only new or updated rows are added
    WHERE user_id NOT IN (SELECT user_id FROM {{ this }})
{% endif %}
