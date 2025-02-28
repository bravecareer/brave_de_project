{{
    config(
        materialized='view',
        unique_key='search_event_id'
    )
}}

-- Staging layer for user journey data
with source as (
    select * from {{ source('de_project', 'user_journey') }}
),

staged as (
    select
        -- Event identifiers
        {{ default_value('search_event_id', "'UNKNOWN'") }} as search_event_id,
        {{ default_value('cart_id', "'UNKNOWN'") }} as cart_id,
        {{ default_value('session_id', "'UNKNOWN'") }} as session_id,
        {{ default_value('search_request_id', "'UNKNOWN'") }} as search_request_id,
        
        -- Timestamps
        {{ default_value('timestamp', 'CURRENT_TIMESTAMP()') }} as event_timestamp,
        {{ default_value('collector_tstamp', 'CURRENT_TIMESTAMP()') }} as collector_timestamp,
        
        -- User identifiers and status
        {{ default_value('user_id', "'UNKNOWN'") }} as user_id,
        {{ default_value('app_id', "'UNKNOWN'") }} as app_id,
        {{ default_value('login_status', "'UNKNOWN'") }} as login_status,
        {{ default_value('registration_status', "'UNKNOWN'") }} as registration_status,
        
        -- Event flags
        {{ default_value('has_qv', 'FALSE') }} as has_qv,
        {{ default_value('has_pdp', 'FALSE') }} as has_pdp,
        {{ default_value('has_atc', 'FALSE') }} as has_atc,
        {{ default_value('has_purchase', 'FALSE') }} as has_purchase,
        
        -- Search details
        COALESCE(search_results_count, 0) as search_results_count,
        {{ default_value('search_terms', "'UNKNOWN'") }} as search_terms,
        {{ default_value('search_feature', "'UNKNOWN'") }} as search_feature,
        {{ default_value('search_terms_type', "'UNKNOWN'") }} as search_terms_type,
        {{ default_value('search_type', "'UNKNOWN'") }} as search_type,
        {{ default_value('search_model', "'UNKNOWN'") }} as search_model,
        
        -- User metrics
        {{ default_value('date_last_login', "'UNKNOWN'") }} as date_last_login,
        {{ default_value('date_last_purchase', 'CURRENT_DATE()') }} as date_last_purchase,
        COALESCE(lifetime_offline_orders_count, 0) as lifetime_offline_orders_count,
        COALESCE(lifetime_online_orders_count, 0) as lifetime_online_orders_count,
        
        -- Store information
        {{ default_value('grocery_home_store_id', "'UNKNOWN'") }} as grocery_home_store_id,
        {{ default_value('rx_home_store_id', "'UNKNOWN'") }} as rx_home_store_id,
        {{ default_value('auto_localized_store_id', "'UNKNOWN'") }} as auto_localized_store_id,
        CASE 
            WHEN selected_store_id IS NULL THEN -1
            WHEN selected_store_id < -1 THEN -1
            ELSE selected_store_id
        END as selected_store_id,
        
        -- Fulfillment details
        {{ default_value('fulfillment_type', "'UNKNOWN'") }} as fulfillment_type,
        {{ default_value('selected_timeslot_date', 'CURRENT_DATE()') }} as selected_timeslot_date,
        {{ default_value('selected_timeslot_time', "'UNKNOWN'") }} as selected_timeslot_time,
        {{ default_value('selected_timeslot_type', "'UNKNOWN'") }} as selected_timeslot_type,
        {{ default_value('shopping_mode', "'UNKNOWN'") }} as shopping_mode,
        
        -- Device information
        {{ default_value('device_class', "'UNKNOWN'") }} as device_class,
        COALESCE(br_viewwidth, 0) as br_viewwidth,
        COALESCE(br_viewheight, 0) as br_viewheight,
        COALESCE(dvce_screenwidth, 0) as dvce_screenwidth,
        COALESCE(dvce_screenheight, 0) as dvce_screenheight,
        COALESCE(doc_width, 0) as doc_width,
        COALESCE(doc_height, 0) as doc_height,
        
        -- Marketing information
        {{ default_value('mkt_medium', "'UNKNOWN'") }} as mkt_medium,
        {{ default_value('mkt_source', "'UNKNOWN'") }} as mkt_source,
        {{ default_value('mkt_content', "'UNKNOWN'") }} as mkt_content,
        {{ default_value('mkt_campaign', "'UNKNOWN'") }} as mkt_campaign,
        
        -- Localization
        {{ default_value('br_lang', "'UNKNOWN'") }} as br_lang,
        {{ default_value('page_language', "'UNKNOWN'") }} as page_language,
        
        -- Geographic information
        {{ default_value('geo_country', "'UNKNOWN'") }} as geo_country,
        {{ default_value('geo_region', "'UNKNOWN'") }} as geo_region,
        {{ default_value('geo_city', "'UNKNOWN'") }} as geo_city,
        {{ default_value('geo_zipcode', "'UNKNOWN'") }} as geo_zipcode,
        COALESCE(geo_latitude, 0) as geo_latitude,
        COALESCE(geo_longitude, 0) as geo_longitude,
        {{ default_value('geo_timezone', "'UNKNOWN'") }} as geo_timezone,
        
        -- Product information
        {{ default_value('product_id', "'UNKNOWN'") }} as product_id,
        
        -- Audit fields
        current_timestamp() as dbt_loaded_at,
        'stg_user_journey' as dbt_source

    from source
)

select * from staged 