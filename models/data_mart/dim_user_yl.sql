{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge'
) }}


WITH user_data AS (
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
  FROM {{ ref('stg_user_data_yl') }}
  WHERE account_status = 'active'
  {% if is_incremental() %}
    -- Process only recent rows on incremental runs (2-day sliding window)
    AND DATE(signup_date) >= CURRENT_DATE() - 2
  {% endif %}
)


SELECT *, '{{ run_started_at }}'::timestamp_tz AS dbt_processed_at FROM user_data