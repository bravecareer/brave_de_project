{{ config(
    materialized='incremental',
    unique_key=['search_event_id', 'session_id', 'timestamp']
) }}

WITH user_journey AS (
    SELECT
        uj.search_event_id,
        uj.session_id,
        to_timestamp(replace(timestamp,' UTC','')) AS timestamp,
        uj.app_id,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        uj.search_feature,
        uj.date_last_purchase,
        uj.registration_status,
        uj.login_status,
        uj.banner,
        uj.mkt_medium,
        uj.mkt_source,
        uj.mkt_content,
        uj.mkt_campaign,
        uj.page_language,
        uj.geo_country,
        uj.geo_region,
        uj.geo_city,
        --uj.device_class,
        --uj.search_terms,
        --uj.geo_zipcode,
        --uj.geo_latitude,
        --uj.geo_longitude,
        --uj.geo_timezone,
        uj.search_model,
        uj.product_id
    FROM {{ source('de_project', 'user_journey') }} uj
),

product_data AS (
    SELECT
        p.product_id,
        p.product_name
    FROM {{ source('de_project', 'product_data') }} p
),

final AS (
    SELECT
        uj.search_event_id,
        uj.session_id,
        uj.timestamp,
        uj.app_id,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        uj.product_id,
        p.product_name,
        uj.search_feature,
        uj.date_last_purchase,
        uj.registration_status,
        uj.login_status,
        uj.banner,
        uj.mkt_medium,
        uj.mkt_source,
        uj.mkt_content,
        uj.mkt_campaign,
        uj.page_language,
        uj.geo_country,
        uj.geo_region,
        uj.geo_city,
        --uj.search_terms,
        --uj.device_class,
        --uj.geo_zipcode,
        --uj.geo_latitude,
        --uj.geo_longitude,
        --uj.geo_timezone,
        uj.search_model
   FROM user_journey uj
   LEFT JOIN product_data p ON uj.product_id = p.product_id
)

SELECT * FROM final