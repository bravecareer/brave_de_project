{{ config(
    materialized='view',
    alias='user_data_transformed_gs',
    unique_key='USER_ID'
) }}

SELECT 
    u.USER_ID::VARCHAR(50) AS user_id,
    u.FIRST_NAME::VARCHAR(100) AS first_name,
    u.LAST_NAME::VARCHAR(100) AS last_name,
    CASE 
        WHEN u.EMAIL NOT LIKE '%_@__%.__%' THEN NULL 
        ELSE u.EMAIL 
    END::VARCHAR(255) AS email,
    COALESCE(TO_DATE(u.SIGNUP_DATE, 'YYYY-MM-DD'), NULL) AS signup_date,
    u.PREFERRED_LANGUAGE::VARCHAR(50) AS preferred_language,
    COALESCE(TO_DATE(u.DOB, 'YYYY-MM-DD'), NULL)::DATE AS date_of_birth,
    CASE 
        WHEN LOWER(u.MARKETING_OPT_IN) IN ('true', '1') THEN TRUE
        WHEN LOWER(u.MARKETING_OPT_IN) IN ('false', '0') THEN FALSE
        ELSE NULL
    END::BOOLEAN AS marketing_opt_in,
    u.ACCOUNT_STATUS::VARCHAR(50) AS account_status,
    COALESCE(u.LOYALTY_POINTS_BALANCE::INTEGER, 0) AS loyalty_points_balance,
    CURRENT_TIMESTAMP AS updated_at  -- New column to track updates

FROM 
    {{ source('de_project', 'user_data') }} u
WHERE 
    u.USER_ID IS NOT NULL  -- Filter to exclude rows where USER_ID is NULL
    AND u.USER_ID != 'user_id'  -- Skip the first row if 'user_id' is present as header
