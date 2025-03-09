{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

-- Get user data with basic information
WITH user_data AS (
    SELECT
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        u.signup_date,
        u.preferred_language,
        u.dob,
        u.marketing_opt_in,
        u.account_status,
        u.loyalty_points_balance
    FROM {{ ref('stg_user_data_tf') }} u
    {% if is_incremental() %}
    -- Only process new users in incremental runs
    -- Use TRY_CAST to handle potential data type issues
    WHERE TRY_CAST(u.signup_date AS DATE) >= CURRENT_DATE() - 7
    {% endif %}
)

SELECT * FROM user_data