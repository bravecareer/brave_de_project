{{ config(
    materialized='table',
    unique_key='user_id'
) }}

WITH cleaned_user_data AS (
    SELECT
        u.user_id,
        u.first_name,
        u.last_name,
        CASE WHEN u.email NOT LIKE '%_@__%.__%' THEN NULL ELSE u.email END AS email,
        TRY_TO_DATE(u.signup_date, 'YYYY-MM-DD') AS signup_date,
        u.preferred_language,
        TRY_TO_DATE(u.dob, 'YYYY-MM-DD') AS dob,
        u.marketing_opt_in,
        u.account_status,
        TRY_CAST(u.loyalty_points_balance AS INTEGER) AS loyalty_points_balance
    FROM {{ source('de_project', 'user_data') }} u
    WHERE u.account_status = 'active' -- Filter for active accounts
)

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
FROM cleaned_user_data