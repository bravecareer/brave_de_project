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
        COALESCE(search_event_id, 'UNKNOWN') as search_event_id,
        COALESCE(cart_id, 'UNKNOWN') as cart_id,
        COALESCE(session_id, 'UNKNOWN') as session_id,
        COALESCE(search_request_id, 'UNKNOWN') as search_request_id,
        
        -- Timestamps
        COALESCE(timestamp, CURRENT_TIMESTAMP()) as event_timestamp,
        COALESCE(collector_tstamp, CURRENT_TIMESTAMP()) as collector_timestamp,
        
        -- User identifiers and status
        COALESCE(user_id, 'UNKNOWN') as user_id,
        COALESCE(app_id, 'UNKNOWN') as app_id,
        COALESCE(login_status, 'UNKNOWN') as login_status,
        COALESCE(registration_status, 'UNKNOWN') as registration_status,
        
        -- Event flags
        COALESCE(has_qv, FALSE) as has_qv,
        COALESCE(has_pdp, FALSE) as has_pdp,
        COALESCE(has_atc, FALSE) as has_atc,
        COALESCE(has_purchase, FALSE) as has_purchase,
        
        -- Search details
        COALESCE(search_results_count, 0) as search_results_count,
        COALESCE(search_terms, 'UNKNOWN') as search_terms,
        COALESCE(search_feature, 'UNKNOWN') as search_feature,
        COALESCE(search_terms_type, 'UNKNOWN') as search_terms_type,
        COALESCE(search_type, 'UNKNOWN') as search_type,
        COALESCE(search_model, 'UNKNOWN') as search_model,
        
        -- User metrics
        COALESCE(date_last_login, 'UNKNOWN') as date_last_login,
        COALESCE(date_last_purchase, CURRENT_DATE()) as date_last_purchase,
        COALESCE(lifetime_offline_orders_count, 0) as lifetime_offline_orders_count,
        COALESCE(lifetime_online_orders_count, 0) as lifetime_online_orders_count,
        
        -- Store information
        COALESCE(grocery_home_store_id, 'UNKNOWN') as grocery_home_store_id,
        COALESCE(rx_home_store_id, 'UNKNOWN') as rx_home_store_id,
        COALESCE(auto_localized_store_id, 'UNKNOWN') as auto_localized_store_id,
        COALESCE(selected_store_id, -1) as selected_store_id,
        
        -- Fulfillment details
        COALESCE(fulfillment_type, 'UNKNOWN') as fulfillment_type,
        COALESCE(selected_timeslot_date, CURRENT_DATE()) as selected_timeslot_date,
        COALESCE(selected_timeslot_time, 'UNKNOWN') as selected_timeslot_time,
        COALESCE(selected_timeslot_type, 'UNKNOWN') as selected_timeslot_type,
        COALESCE(shopping_mode, 'UNKNOWN') as shopping_mode,
        
        -- Device information
        COALESCE(device_class, 'UNKNOWN') as device_class,
        COALESCE(br_viewwidth, 0) as br_viewwidth,
        COALESCE(br_viewheight, 0) as br_viewheight,
        COALESCE(dvce_screenwidth, 0) as dvce_screenwidth,
        COALESCE(dvce_screenheight, 0) as dvce_screenheight,
        COALESCE(doc_width, 0) as doc_width,
        COALESCE(doc_height, 0) as doc_height,
        
        -- Marketing information
        COALESCE(mkt_medium, 'UNKNOWN') as mkt_medium,
        COALESCE(mkt_source, 'UNKNOWN') as mkt_source,
        COALESCE(mkt_content, 'UNKNOWN') as mkt_content,
        COALESCE(mkt_campaign, 'UNKNOWN') as mkt_campaign,
        
        -- Localization
        COALESCE(br_lang, 'UNKNOWN') as br_lang,
        COALESCE(page_language, 'UNKNOWN') as page_language,
        
        -- Geographic information
        COALESCE(geo_country, 'UNKNOWN') as geo_country,
        COALESCE(geo_region, 'UNKNOWN') as geo_region,
        COALESCE(geo_city, 'UNKNOWN') as geo_city,
        COALESCE(geo_zipcode, 'UNKNOWN') as geo_zipcode,
        COALESCE(geo_latitude, 0) as geo_latitude,
        COALESCE(geo_longitude, 0) as geo_longitude,
        COALESCE(geo_timezone, 'UNKNOWN') as geo_timezone,
        
        -- Product information
        COALESCE(product_id, 'UNKNOWN') as product_id,
        
        -- Audit fields
        current_timestamp() as dbt_loaded_at,
        'stg_user_journey' as dbt_source

    from source
)

select * from staged 