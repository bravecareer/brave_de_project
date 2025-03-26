/*SEARCH_EVENT_ID,
USER_ID,
PRODUCT_ID
TIMESTAMP,
COLLECTOR_TSTAMP,
HAS_ATC,
p.rating,
SESSION_ID,
HAS_PURCHASE,
	APP_ID,
	HAS_QV,
	HAS_PDP,
	
	
	IMPRESSIONS_WITH_ATTRIBUTIONS,
	CART_ID,
	
	SEARCH_REQUEST_ID,
	SEARCH_RESULTS_COUNT,
	SEARCH_TERMS,
	SEARCH_FEATURE,
	SEARCH_TERMS_TYPE,
	SEARCH_TYPE,
	AES_HASH,
	DATE_LAST_LOGIN,
	DATE_LAST_PURCHASE,
	GROCERY_HOME_STORE_ID,
	LIFETIME_OFFLINE_ORDERS_COUNT,
	LIFETIME_ONLINE_ORDERS_COUNT,
	LOGIN_STATUS,
	
	REGISTRATION_STATUS,
	RX_HOME_STORE_ID,
	SHA256_HASH,
	BANNER,
	AUTO_LOCALIZED_STORE_ID,
	FULFILLMENT_TYPE,
	SELECTED_STORE_ID,
	SELECTED_TIMESLOT_DATE,
	SELECTED_TIMESLOT_TIME,
	SELECTED_TIMESLOT_TYPE,
	SHOPPING_MODE,
	DEVICE_CLASS,
	BR_VIEWWIDTH,
	BR_VIEWHEIGHT,
	DVCE_SCREENWIDTH,
	DVCE_SCREENHEIGHT,
	DOC_WIDTH,
	DOC_HEIGHT,
	MKT_MEDIUM,
	MKT_SOURCE,
	MKT_CONTENT,
	MKT_CAMPAIGN,
	BR_LANG,
	PAGE_LANGUAGE,
	GEO_COUNTRY,
	GEO_REGION,
	GEO_CITY,
	GEO_ZIPCODE,
	GEO_LATITUDE,
	GEO_LONGITUDE,
	GEO_TIMEZONE,
	SEARCH_MODEL,
	

    SearchEventID	Primary key
Timestamp	Search timestamp
UserID	FK to DimUser
ProductID	FK to DimProduct
SessionID	FK to DimSession
SearchTerms	Query text
SearchFeature	Filter, voice, etc.
SearchType	Manual, Auto-suggest, etc.
SearchResultsCount	Returned results
HasQV	Viewed Quick View
HasPDP	Viewed Product Detail Page
HasATC	Added to Cart
HasPurchase	Purchase flag
CartID	FK to DimCart
SearchModel	Algorithm used
GeoKey	FK to DimGeo
DeviceKey	FK to DimDevice
MarketingKey	FK to DimMarketing*/

WITH base_events AS (
    SELECT
        be.search_event_id,
        be.timestamp,
        be.session_id,
        be.user_id,
        be.product_id,
        be.search_terms,
        be.search_feature,
        be.search_results_count,
        be.search_type,
        be.search_terms_type,
        be.search_model,
        be.has_qv,
        be.has_pdp,
        be.has_atc,
        be.has_purchase,
        be.cart_id,
        be.geo_country,
        be.geo_region,
        be.geo_city,
        be.device_class,
        be.mkt_campaign,
        be.mkt_medium,
        be.mkt_source,
        be.mkt_content
    FROM {{ ref('stg_user_journey_ba') }} be
),

-- Join with product data
product_enriched AS (
    SELECT
        pe.product_id,
        pe.product_name,
        pe.product_category,
        pe.price,
        pe.supplier_id,
        pe.rating,
        pe.discount_percentage
    FROM {{ ref('stg_product_data_ba') }} pe
),

-- Join with user data
user_enriched AS (
    SELECT
        ue.user_id,
        ue.signup_date,
        ue.loyalty_points_balance,
        ue.marketing_opt_in,
        ue.account_status
    FROM {{ ref('stg_user_data_ba') }} ue
)

SELECT
    be.search_event_id,
    be.timestamp,
    be.session_id,
    be.user_id,
    be.product_id,
    be.search_terms,
    be.search_feature,
    be.search_results_count,
    be.search_type,
    be.search_terms_type,
    be.search_model,
    be.has_qv,
    be.has_pdp,
    be.has_atc,
    be.has_purchase,
    be.cart_id,

    -- Geo + Device + Marketing
    be.geo_country,
    be.geo_region,
    be.geo_city,
    be.device_class,
    be.mkt_campaign,
    be.mkt_medium,
    be.mkt_source,
    be.mkt_content,

    -- Product Attributes
    pe.product_name,
    pe.product_category,
    pe.price,
    pe.rating,
    pe.discount_percentage,
    pe.supplier_id,

    -- User Attributes
    ue.signup_date,
    ue.loyalty_points_balance,
    ue.marketing_opt_in,
    ue.account_status

FROM base_events be
LEFT JOIN product_enriched pe ON be.product_id = pe.product_id
LEFT JOIN user_enriched ue ON be.user_id = ue.user_id


/*SELECT
  product_name,
  product_category,
  COUNT(*) AS total_searches,
  SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) AS total_atc,
  ROUND(SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 2) AS atc_rate
FROM {{ ref('fact_search_activity') }}
GROUP BY product_name, product_category
ORDER BY atc_rate DESC*/