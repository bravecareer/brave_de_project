{{ config(
    materialized='view'
) }}

WITH raw_data AS (
    SELECT
        search_event_id,
        timestamp,
        session_id,
        mkt_campaign,
        mkt_source,
        mkt_medium,
        mkt_content,
        user_id,
        product_id,
        has_qv,
        has_pdp,
        has_atc,
        has_purchase,
        geo_country,
        geo_region,
        geo_city,
        geo_zipcode,
        device_class
    FROM {{ source('de_project', 'user_journey') }}
)

SELECT * FROM raw_data