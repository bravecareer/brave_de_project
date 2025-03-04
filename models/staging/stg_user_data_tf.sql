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
        {{ default_value('user_id', "'UNKNOWN'") }} as user_id,
        
        -- Personal information
        {{ default_value('first_name', "'UNKNOWN'") }} as first_name,
        {{ default_value('last_name', "'UNKNOWN'") }} as last_name,
        {{ default_value('email', "'UNKNOWN'") }} as email,
        
        -- User preferences and dates
        {{ default_value('signup_date', "'UNKNOWN'") }} as signup_date,
        {{ default_value('preferred_language', "'UNKNOWN'") }} as preferred_language,
        {{ default_value('dob', "'UNKNOWN'") }} as dob,
        
        -- Account information
        {{ default_value('marketing_opt_in', "'NO'") }} as marketing_opt_in,
        {{ default_value('account_status', "'INACTIVE'") }} as account_status,
        {{ default_value('loyalty_points_balance', "'0'") }} as loyalty_points_balance,
        
        -- Audit fields
        current_timestamp() as dbt_loaded_at,
        'stg_user' as dbt_source

    from source
)

select * from staged