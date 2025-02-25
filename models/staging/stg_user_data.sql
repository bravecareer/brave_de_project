{{
    config(
        materialized='incremental',
        unique_key='user_id',
        incremental_strategy='delete+insert'
    )
}}

-- Staging layer for user data
with source as (
    select * from {{ source('de_project', 'user_data') }}
),

staged as (
    select
        -- User identifiers
        COALESCE(user_id, 'UNKNOWN') as user_id,
        
        -- Personal information
        COALESCE(first_name, 'UNKNOWN') as first_name,
        COALESCE(last_name, 'UNKNOWN') as last_name,
        COALESCE(email, 'UNKNOWN') as email,
        
        -- User preferences and dates
        COALESCE(signup_date, 'UNKNOWN') as signup_date,
        COALESCE(preferred_language, 'UNKNOWN') as preferred_language,
        COALESCE(dob, 'UNKNOWN') as dob,
        
        -- Account information
        COALESCE(marketing_opt_in, 'NO') as marketing_opt_in,
        COALESCE(account_status, 'INACTIVE') as account_status,
        COALESCE(loyalty_points_balance, '0') as loyalty_points_balance,
        
        -- Audit fields
        current_timestamp() as dbt_loaded_at,
        'stg_user' as dbt_source

    from source
)

select * from staged 