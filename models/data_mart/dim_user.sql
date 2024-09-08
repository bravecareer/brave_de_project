{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

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
    FROM {{ source('de_project', 'user_data') }} u
    WHERE u.account_status = 'active'
)

SELECT * FROM user_data
