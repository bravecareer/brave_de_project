{{ config(
    materialized='table',
    schema='PROJECT_TEST',
    cluster_by=['user_id'],
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT pk_user_id PRIMARY KEY (user_id)"
    ]
) }}

WITH source AS (
    SELECT 
        user_id,
        COALESCE(TRIM(first_name), 'Unknown') AS first_name,
        COALESCE(TRIM(last_name), 'Unknown') AS last_name,
        LOWER(TRIM(email)) AS email,
        INITCAP(TRIM(preferred_language)) AS preferred_language,
        INITCAP(TRIM(account_status)) AS account_status,
        CASE 
            WHEN LOWER(TRIM(marketing_opt_in)) IN ('true', 'yes', '1') THEN TRUE
            ELSE FALSE
        END AS marketing_opt_in,
        signup_date,
        age_years,
        days_since_signup,
        loyalty_points_balance
    FROM {{ ref('stg_user_data_sae') }}
    WHERE user_id IS NOT NULL
      AND email IS NOT NULL
),

transformed AS (
    SELECT
        user_id,
        INITCAP(first_name) AS first_name,
        INITCAP(last_name) AS last_name,
        CONCAT(INITCAP(first_name), ' ', INITCAP(last_name)) AS full_name,
        email,
        signup_date,
        preferred_language,
        account_status,
        marketing_opt_in,
        age_years,
        days_since_signup,
        loyalty_points_balance,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM source
)

SELECT *
FROM transformed






-- {{ config(materialized='table', schema='PROJECT_TEST', cluster_by=['user_id']) }}

-- WITH source AS (
--     SELECT 
--         user_id,
--         COALESCE(TRIM(first_name), 'Unknown') AS first_name,
--         COALESCE(TRIM(last_name), 'Unknown') AS last_name,
--         LOWER(TRIM(email)) AS email,
--         INITCAP(TRIM(preferred_language)) AS preferred_language,
--         INITCAP(TRIM(account_status)) AS account_status,
--         CASE 
--             WHEN LOWER(TRIM(marketing_opt_in)) IN ('true', 'yes', '1') THEN TRUE
--             ELSE FALSE
--         END AS marketing_opt_in,
--         signup_date,
--         age_years,
--         days_since_signup,
--         loyalty_points_balance
--     FROM {{ ref('stg_user_data_sae') }}
--     WHERE user_id IS NOT NULL
--       AND email IS NOT NULL
-- ),

-- transformed AS (
--     SELECT
--         user_id,
--         INITCAP(first_name) AS first_name,
--         INITCAP(last_name) AS last_name,
--         CONCAT(INITCAP(first_name), ' ', INITCAP(last_name)) AS full_name,
--         email,
--         signup_date,
--         preferred_language,
--         account_status,
--         marketing_opt_in,
--         age_years,
--         days_since_signup,
--         loyalty_points_balance,
--         CURRENT_TIMESTAMP() AS load_timestamp
--     FROM source
-- )

-- SELECT *
-- FROM transformed
