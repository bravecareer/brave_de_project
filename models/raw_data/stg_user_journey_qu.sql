{{ config(
    materialized='view'
) }}

with source as (

    select * from {{ source('de_project', 'user_journey') }}

),

staging_data as (

    select
        CAST(search_event_id AS VARCHAR(64)) AS search_event_id,
        has_qv,
        has_pdp,
        has_atc,
        has_purchase,
        impressions_with_attributions,
        cart_id,
        CAST(session_id AS VARCHAR(64)) AS session_id,
        search_request_id,
        search_results_count,
        CAST(search_terms AS VARCHAR(64)) AS search_terms,
        CAST(search_feature AS VARCHAR(64)) AS search_feature,
        CAST(search_terms_type AS VARCHAR(64)) AS search_terms_type,
        CAST(search_type AS VARCHAR(64)) AS search_type,
        aes_hash,
        date_last_purchase,
        login_status,
        CAST(user_id AS VARCHAR(64)) AS user_id,     
        sha256_hash,
        banner,  
        fulfillment_type,
        selected_timeslot_date,
        selected_timeslot_time,
        selected_timeslot_type,    
        br_viewwidth,
        br_viewheight,
        dvce_screenwidth,
        dvce_screenheight,
        doc_width,
        doc_height,
        CAST(mkt_medium AS VARCHAR(64)) AS medium,
        CAST(mkt_source AS VARCHAR(64)) AS source,
        CAST(mkt_content AS VARCHAR(64)) AS content,
        CAST(mkt_campaign AS VARCHAR(64)) AS campaign_name,     
        geo_latitude AS latitude,
        geo_longitude AS longitude,

        -- Reformat data
        CAST(REGEXP_REPLACE(app_id, '-prod$', '') AS VARCHAR(32)) AS app_id,

        -- Convert dob from VARCHAR to DATE
        {{convert_varchar_to_date('date_last_login')}} AS date_last_login,

        -- converting to interger, as count can only be integer
        CAST(lifetime_offline_orders_count AS NUMBER(38, 0)) AS lifetime_offline_orders_count,
        CAST(lifetime_online_orders_count AS NUMBER(38, 0)) AS lifetime_online_orders_count,

        -- converting id to integer number
        CAST(auto_localized_store_id AS NUMBER(38, 0)) AS auto_localized_store_id,
        CAST(rx_home_store_id AS NUMBER(38, 0)) AS rx_home_store_id,
        CAST(selected_store_id AS NUMBER(38, 0)) AS selected_store_id,
        CAST(grocery_home_store_id AS NUMBER(38, 0)) AS grocery_home_store_id,

        -- Truncate the field to necessary length
        CAST(registration_status AS VARCHAR(32)) AS registration_status,
        CAST(shopping_mode AS VARCHAR(32)) AS shopping_mode,
        CAST(device_class AS VARCHAR(32)) AS device_class,
        CAST(br_lang AS VARCHAR(32)) AS br_language,
        CAST(page_language AS VARCHAR(32)) AS page_language,
        CAST(geo_country AS VARCHAR(32)) AS country,
        CAST(geo_region AS VARCHAR(32)) AS region,
        CAST(geo_city AS VARCHAR(32)) AS city,
        CAST(geo_zipcode AS VARCHAR(32)) AS zipcode,
        CAST(search_model AS VARCHAR(32)) AS search_model,
        CAST(geo_timezone AS VARCHAR(32)) AS timezone,

        -- Extract product id from varchar string and cast to NUMBER
        {{convert_varchar_to_num('product_id')}} AS product_id, 

        -- Converting timestamp to
        to_timestamp(replace(timestamp, ' UTC', '')) AS search_timestamp,
        to_timestamp(replace(collector_tstamp, ' UTC', '')) AS collector_tstamp,
     

    from source

)

select * from staging_data