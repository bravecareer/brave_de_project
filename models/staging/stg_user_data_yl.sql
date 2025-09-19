{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge'
) }}

WITH user_data AS (
  SELECT
      user_id,
      -- strip header-like values and normalize casing
      NULLIF(TRIM(first_name), 'first_name')               AS first_name,
      NULLIF(TRIM(last_name),  'last_name')                AS last_name,
      CASE WHEN email ILIKE '%_@__%.__%' THEN LOWER(email) END AS email,
      COALESCE(try_to_date(signup_date), CURRENT_DATE)         AS signup_date,
      LOWER(preferred_language)                            AS preferred_language,
      TRY_TO_DATE(dob)                                     AS dob,
      TRY_TO_BOOLEAN(marketing_opt_in)                     AS marketing_opt_in,
      LOWER(account_status)                                AS account_status,
      TRY_TO_NUMBER(loyalty_points_balance)                AS loyalty_points_balance
  FROM {{ source('de_project','user_data') }}
  WHERE user_id <> 'user_id'  -- drop header row if present
  {% if is_incremental() %}
    AND DATE(signup_date) >= CURRENT_DATE() - 2
  {% endif %}
),

cleaned_user AS (
  SELECT *
  FROM user_data
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY signup_date DESC NULLS LAST) = 1
)

SELECT *, '{{ run_started_at }}'::timestamp_tz AS dbt_processed_at FROM cleaned_user
