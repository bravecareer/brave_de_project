{{
    config(
        materialized='incremental',
        alias='dim_user_data_gs',
        unique_key='user_id'
    )
}}

SELECT 
    user_id,
    first_name,
    last_name,
    email,
    signup_date,
    preferred_language,
    date_of_birth,
    marketing_opt_in,
    account_status,
    loyalty_points_balance,
    updated_at
FROM {{ ref('view_user_data_transformed_gs') }}

{% if is_incremental() %}
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
