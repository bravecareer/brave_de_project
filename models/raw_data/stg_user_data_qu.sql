{{ 
    config(
    materialized='view'
) }}

with source as (

    select * from {{ source('de_project', 'user_data') }}

),

staging_data as (

    select
        user_id,
        
        -- Converting to short field
        CAST(first_name AS VARCHAR(64)) AS first_name,
        CAST(last_name AS VARCHAR(64)) AS last_name,
        CAST(preferred_language AS VARCHAR(64)) AS preferred_language,
        CAST(account_status AS VARCHAR(64)) AS account_status,

        -- Checking for invalid email id format 
        CASE 
            WHEN email not like '%_@__%.__%' THEN NULL 
            ELSE CAST(email AS VARCHAR(256)) 
        END AS email,

        -- Convert signup_date from VARCHAR to DATE
        {{convert_varchar_to_date('signup_date')}} AS signup_date,

        -- Convert dob from VARCHAR to DATE
        {{convert_varchar_to_date('dob')}} AS dob,

         -- Convert marketing_opt_in from VARCHAR to BOOLEAN
        CASE 
            WHEN marketing_opt_in = 'True' THEN TRUE
            WHEN marketing_opt_in = 'False' THEN FALSE
            ELSE False 
        END AS marketing_opt_in,

        -- Convert loyalty_points_balance from VARCHAR to NUMBER
        {{convert_varchar_to_num('loyalty_points_balance')}} AS loyalty_points_balance

    from source

)

select * from staging_data

