{{ config(
    materialized='view'
) }}

WITH source_data AS (
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
    FROM {{ source('de_project', 'user_data') }}
)

SELECT
    user_id,
    first_name,
    last_name,
    CASE 
        WHEN email NOT LIKE '%_@__%.__%' THEN NULL 
        ELSE email 
    END AS email,
    signup_date,
    preferred_language,
    dob,
    marketing_opt_in,
    account_status,
    loyalty_points_balance
FROM source_data