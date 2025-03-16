{{ config(materialized='table', schema='PROJECT_TEST') }}

WITH raw_data AS (
    SELECT 
        USER_ID::VARCHAR(255) AS user_id,
        FIRST_NAME::VARCHAR(255) AS first_name,
        LAST_NAME::VARCHAR(255) AS last_name,
        EMAIL::VARCHAR(255) AS email,
        SIGNUP_DATE::VARCHAR(255) AS signup_date,
        PREFERRED_LANGUAGE::VARCHAR(255) AS preferred_language,
        DOB::VARCHAR(255) AS dob,
        MARKETING_OPT_IN::VARCHAR(255) AS marketing_opt_in,
        ACCOUNT_STATUS::VARCHAR(255) AS account_status,
        LOYALTY_POINTS_BALANCE::VARCHAR(255) AS loyalty_points_balance
    FROM {{ source('de_project', 'user_data') }}
    WHERE USER_ID IS NOT NULL
      AND EMAIL IS NOT NULL
      -- Email Validation: only accept emails that match the regex pattern.
      AND EMAIL REGEXP '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
),

cleaned AS (
    SELECT 
        TRIM(user_id) AS user_id,
        INITCAP(LOWER(TRIM(first_name))) AS first_name,
        INITCAP(LOWER(TRIM(last_name))) AS last_name,
        LOWER(TRIM(email)) AS email,
        TRY_CAST(TRIM(signup_date) AS DATE) AS signup_date,
        INITCAP(LOWER(TRIM(preferred_language))) AS preferred_language,
        TRY_CAST(TRIM(dob) AS DATE) AS dob,
        CASE 
          WHEN LOWER(TRIM(marketing_opt_in)) IN ('true', 'yes') THEN TRUE
          WHEN LOWER(TRIM(marketing_opt_in)) IN ('false', 'no') THEN FALSE
          ELSE FALSE
        END AS marketing_opt_in,
        CASE 
          WHEN LOWER(TRIM(account_status)) IN ('active', 'inactive', 'suspended')
          THEN INITCAP(LOWER(TRIM(account_status)))
          ELSE 'Unknown'
        END AS account_status,
        COALESCE(TRY_CAST(TRIM(loyalty_points_balance) AS NUMBER(38,2)), 0) AS loyalty_points_balance,
        -- Calculate FULL_NAME: if both first and last are missing, default to 'Unknown'
        CASE 
          WHEN TRIM(first_name) IS NULL AND TRIM(last_name) IS NULL THEN 'Unknown'
          ELSE CONCAT(
                COALESCE(INITCAP(LOWER(TRIM(first_name))), ''),
                ' ',
                COALESCE(INITCAP(LOWER(TRIM(last_name))), '')
               )
        END AS full_name,
        -- Additional Calculated Fields:
        DATEDIFF('year', TRY_CAST(TRIM(dob) AS DATE), CURRENT_DATE()) AS age_years,
        DATEDIFF('day', TRY_CAST(TRIM(signup_date) AS DATE), CURRENT_DATE()) AS days_since_signup,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM raw_data
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY load_timestamp DESC) AS rn
    FROM cleaned
)

SELECT *
FROM deduped
WHERE rn = 1
