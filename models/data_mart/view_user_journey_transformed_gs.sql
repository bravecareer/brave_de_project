{{ config(
    materialized='view',
    unique_keys = ['user_id', 'search_event_id', 'product_id', 'timestamp'],
    alias='view_user_journey_transformed_gs',

) }}

WITH user_journey_clean AS (
    SELECT 
        search_event_id::VARCHAR(255) AS search_event_id,
        
        -- Handle UTC and null timestamps, ensure consistency with user_id and product_id
        CASE 
            WHEN timestamp IS NOT NULL THEN 
                -- If timestamp is in UTC, convert it to the desired timezone (e.g., 'America/New_York')
                CASE
                    WHEN timestamp::TEXT LIKE '%UTC%' THEN 
                        CONVERT_TIMEZONE('UTC', 'America/New_York', timestamp)
                    ELSE timestamp
                END
            ELSE NULL
        END AS timestamp,

        -- Ensure consistency: if timestamp is null, user_id and product_id should also be null
        CASE
            WHEN timestamp IS NULL THEN NULL
            ELSE user_id
        END AS user_id,
        
        CASE
            WHEN timestamp IS NULL THEN NULL
            ELSE product_id
        END AS product_id,

        -- Other columns remain unchanged
        app_id::VARCHAR(255) AS app_id,
        has_qv::BOOLEAN AS has_qv,
        has_pdp::BOOLEAN AS has_pdp,
        has_atc::BOOLEAN AS has_atc,
        has_purchase::BOOLEAN AS has_purchase,
        impressions_with_attributions::VARCHAR(255) AS impressions_with_attributions,
        cart_id::VARCHAR(255) AS cart_id,
        session_id::VARCHAR(255) AS session_id,
        search_request_id::VARCHAR(255) AS search_request_id,
        search_results_count::NUMBER AS search_results_count,
        search_terms::VARCHAR(255) AS search_terms,
        search_feature::VARCHAR(255) AS search_feature,
        search_terms_type::VARCHAR(255) AS search_terms_type,
        search_type::VARCHAR(255) AS search_type,
        lifetime_offline_orders_count::NUMBER AS lifetime_offline_orders_count,
        lifetime_online_orders_count::NUMBER AS lifetime_online_orders_count,
        login_status::VARCHAR(255) AS login_status,
        registration_status::VARCHAR(255) AS registration_status,
        shopping_mode::VARCHAR(255) AS shopping_mode,
        mkt_medium::VARCHAR(255) AS mkt_medium,
        mkt_source::VARCHAR(255) AS mkt_source,
        mkt_content::VARCHAR(255) AS mkt_content,
        mkt_campaign::VARCHAR(255) AS mkt_campaign,
        search_model::VARCHAR(255) AS search_model,
        
        -- Other columns like updated_at
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ source('de_project', 'user_journey') }}
    WHERE 
        search_event_id IS NOT NULL
        AND user_id IS NOT NULL
        AND product_id IS NOT NULL
        AND timestamp IS NOT NULL
)

SELECT * FROM user_journey_clean