/*
****************************************************************************************************
MODEL: stg_user_journey_kl
PURPOSE:
    - Cleanse and standardize raw user_journey data
****************************************************************************************************
*/

{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source AS (
    SELECT * FROM {{ source('de_project', 'user_journey') }}
),

type_standardization AS (
    SELECT
        -- Primary Keys
        CAST(USER_ID AS VARCHAR) AS user_id,
        CAST(PRODUCT_ID AS VARCHAR) AS product_id,
        CAST(SEARCH_EVENT_ID AS VARCHAR) AS search_event_id,
        CAST(SESSION_ID AS VARCHAR) AS session_id,
        CAST(CART_ID AS VARCHAR) AS cart_id,
        
        -- Timestamp Standardization
        TO_TIMESTAMP(
            CASE 
                WHEN TIMESTAMP LIKE '%UTC%' THEN REPLACE(TIMESTAMP, ' UTC', '')
                ELSE TIMESTAMP
            END
        ) AS event_timestamp,
        
        -- Boolean fields (根据实际列名)
        COALESCE(HAS_QV, FALSE) AS has_qv,
        COALESCE(HAS_PDP, FALSE) AS has_pdp,
        COALESCE(HAS_ATC, FALSE) AS has_atc,
        COALESCE(HAS_PURCHASE, FALSE) AS has_purchase,
        
        -- Context Fields
        SEARCH_TERMS AS search_terms,
        GEO_COUNTRY AS geo_country,
        GEO_ZIPCODE AS geo_zipcode,
        DEVICE_CLASS AS device_class,
        MKT_CAMPAIGN AS mkt_campaign,
        MKT_SOURCE AS mkt_source,
        
        -- Audit
        CURRENT_TIMESTAMP() AS loaded_at
        
    FROM source
),

quality_filtered AS (
    SELECT 
        *,
        CASE 
            WHEN user_id IS NULL THEN 'missing_user_id'
            WHEN product_id IS NULL THEN 'missing_product_id'
            WHEN event_timestamp IS NULL THEN 'invalid_timestamp'
            ELSE 'valid'
        END AS data_quality_flag
        
    FROM type_standardization
),

final AS (
    SELECT * EXCLUDE data_quality_flag
    FROM quality_filtered
    WHERE data_quality_flag = 'valid'
)

SELECT * FROM final