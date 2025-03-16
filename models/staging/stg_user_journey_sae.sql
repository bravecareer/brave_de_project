{{ config(materialized='table', schema='PROJECT_TEST', cluster_by=['search_event_id']) }}

WITH raw_data AS (
  SELECT 
    SEARCH_EVENT_ID::VARCHAR(255) AS search_event_id,
    TIMESTAMP AS event_timestamp,  -- primary event timestamp
    COLLECTOR_TSTAMP AS collector_timestamp,
    APP_ID::VARCHAR(255) AS app_id,
    HAS_QV::BOOLEAN AS has_qv,
    HAS_PDP::BOOLEAN AS has_pdp,
    HAS_ATC::BOOLEAN AS has_atc,
    HAS_PURCHASE::BOOLEAN AS has_purchase,
    TRIM(IMPRESSIONS_WITH_ATTRIBUTIONS) AS impressions_with_attributions,
    TRIM(CART_ID) AS cart_id,
    TRIM(SESSION_ID) AS session_id,
    TRIM(SEARCH_REQUEST_ID) AS search_request_id,
    SEARCH_RESULTS_COUNT::NUMBER(38,0) AS search_results_count,
    TRIM(SEARCH_TERMS) AS search_terms,
    TRIM(SEARCH_FEATURE) AS search_feature,
    TRIM(SEARCH_TERMS_TYPE) AS search_terms_type,
    TRIM(SEARCH_TYPE) AS search_type,
    TRIM(AES_HASH) AS aes_hash,
    TRIM(DATE_LAST_LOGIN) AS date_last_login,
    DATE_LAST_PURCHASE::DATE AS date_last_purchase,
    TRIM(GROCERY_HOME_STORE_ID) AS grocery_home_store_id,
    LIFETIME_OFFLINE_ORDERS_COUNT::NUMBER(38,1) AS lifetime_offline_orders_count,
    LIFETIME_ONLINE_ORDERS_COUNT::NUMBER(38,1) AS lifetime_online_orders_count,
    TRIM(LOGIN_STATUS) AS login_status,
    TRIM(USER_ID) AS user_id,
    TRIM(REGISTRATION_STATUS) AS registration_status,
    TRIM(RX_HOME_STORE_ID) AS rx_home_store_id,
    TRIM(SHA256_HASH) AS sha256_hash,
    TRIM(BANNER) AS banner,
    TRIM(AUTO_LOCALIZED_STORE_ID) AS auto_localized_store_id,
    TRIM(FULFILLMENT_TYPE) AS fulfillment_type,
    SELECTED_STORE_ID::NUMBER(38,1) AS selected_store_id,
    SELECTED_TIMESLOT_DATE::DATE AS selected_timeslot_date,
    TRIM(SELECTED_TIMESLOT_TIME) AS selected_timeslot_time,
    TRIM(SELECTED_TIMESLOT_TYPE) AS selected_timeslot_type,
    TRIM(SHOPPING_MODE) AS shopping_mode,
    TRIM(DEVICE_CLASS) AS device_class,
    BR_VIEWWIDTH::NUMBER(38,1) AS br_viewwidth,
    BR_VIEWHEIGHT::NUMBER(38,1) AS br_viewheight,
    DVCE_SCREENWIDTH::NUMBER(38,0) AS dvce_screenwidth,
    DVCE_SCREENHEIGHT::NUMBER(38,0) AS dvce_screenheight,
    DOC_WIDTH::NUMBER(38,1) AS doc_width,
    DOC_HEIGHT::NUMBER(38,1) AS doc_height,
    TRIM(MKT_MEDIUM) AS mkt_medium,
    TRIM(MKT_SOURCE) AS mkt_source,
    TRIM(MKT_CONTENT) AS mkt_content,
    TRIM(MKT_CAMPAIGN) AS mkt_campaign,
    TRIM(BR_LANG) AS br_lang,
    TRIM(PAGE_LANGUAGE) AS page_language,
    TRIM(GEO_COUNTRY) AS geo_country,
    TRIM(GEO_REGION) AS geo_region,
    TRIM(GEO_CITY) AS geo_city,
    TRIM(GEO_ZIPCODE) AS geo_zipcode,
    GEO_LATITUDE::NUMBER(38,4) AS geo_latitude,
    GEO_LONGITUDE::NUMBER(38,4) AS geo_longitude,
    TRIM(GEO_TIMEZONE) AS geo_timezone,
    TRIM(SEARCH_MODEL) AS search_model,
    TRIM(PRODUCT_ID) AS product_id
  FROM {{ source('de_project', 'user_journey') }}
  WHERE SEARCH_EVENT_ID IS NOT NULL
    AND TIMESTAMP IS NOT NULL
    -- Ensure numeric and geospatial data are valid
    AND SEARCH_RESULTS_COUNT >= 0
    AND (GEO_LATITUDE IS NULL OR (GEO_LATITUDE BETWEEN -90 AND 90))
    AND (GEO_LONGITUDE IS NULL OR (GEO_LONGITUDE BETWEEN -180 AND 180))
    AND TIMESTAMP <= COLLECTOR_TSTAMP
),

cleaned AS (
  SELECT
    search_event_id,
    event_timestamp,
    collector_timestamp,
    app_id,
    has_qv,
    has_pdp,
    has_atc,
    has_purchase,
    impressions_with_attributions,
    cart_id,
    session_id,
    search_request_id,
    search_results_count,
    INITCAP(LOWER(search_terms)) AS search_terms,
    INITCAP(LOWER(search_feature)) AS search_feature,
    INITCAP(LOWER(search_terms_type)) AS search_terms_type,
    INITCAP(LOWER(search_type)) AS search_type,
    aes_hash,
    TRY_CAST(date_last_login AS DATE) AS date_last_login,
    date_last_purchase,
    grocery_home_store_id,
    lifetime_offline_orders_count,
    lifetime_online_orders_count,
    INITCAP(LOWER(login_status)) AS login_status,
    user_id,
    INITCAP(LOWER(registration_status)) AS registration_status,
    rx_home_store_id,
    sha256_hash,
    banner,
    auto_localized_store_id,
    fulfillment_type,
    selected_store_id,
    selected_timeslot_date,
    TRIM(selected_timeslot_time) AS selected_timeslot_time,
    INITCAP(LOWER(selected_timeslot_type)) AS selected_timeslot_type,
    INITCAP(LOWER(shopping_mode)) AS shopping_mode,
    INITCAP(LOWER(device_class)) AS device_class,
    br_viewwidth,
    br_viewheight,
    dvce_screenwidth,
    dvce_screenheight,
    doc_width,
    doc_height,
    INITCAP(LOWER(mkt_medium)) AS mkt_medium,
    INITCAP(LOWER(mkt_source)) AS mkt_source,
    mkt_content,
    mkt_campaign,
    INITCAP(LOWER(br_lang)) AS br_lang,
    INITCAP(LOWER(page_language)) AS page_language,
    INITCAP(LOWER(geo_country)) AS geo_country,
    INITCAP(LOWER(geo_region)) AS geo_region,
    INITCAP(LOWER(geo_city)) AS geo_city,
    geo_zipcode,
    geo_latitude,
    geo_longitude,
    geo_timezone,
    INITCAP(LOWER(search_model)) AS search_model,
    product_id,
    CURRENT_TIMESTAMP() AS load_timestamp,
    -- Derive additional columns for analysis:
    CAST(event_timestamp AS DATE) AS search_date,
    TO_CHAR(event_timestamp, 'HH24:MI:SS') AS search_time,
    DATEDIFF('day', event_timestamp, CURRENT_TIMESTAMP()) AS event_age_days,
    DATEDIFF('day', date_last_purchase, CURRENT_DATE) AS days_since_last_purchase
  FROM raw_data
),

deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY search_event_id ORDER BY load_timestamp DESC) AS rn
  FROM cleaned
)

SELECT *
FROM deduped
WHERE rn = 1
