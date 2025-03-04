{{ config(
    materialized='view',
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
)

SELECT * FROM user_data