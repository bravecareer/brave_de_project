{{ config(
   materialized='view',
   unique_key='search_event_id'
) }}


WITH user_journey AS (
   SELECT

        search_event_id, 
        user_id,
        product_id,
        to_timestamp(replace(timestamp,' UTC','')) AS timestamp,
        geo_country,
        geo_region,
        geo_city,
        geo_zipcode,
        geo_latitude,
        geo_longitude,
        geo_timezone,
        has_qv,
        has_pdp,
        has_atc,
        has_purchase,
        impressions_with_attributions,
        cart_id,
        session_id,
        mkt_medium,
        mkt_source,
        mkt_content,
        mkt_campaign,
        search_terms,
        search_results_count,
        search_type,
        device_class,
        app_id,
        CAST(date_last_purchase AS DATE) as date_last_purchase,
        LIFETIME_OFFLINE_ORDERS_COUNT,
	     LIFETIME_ONLINE_ORDERS_COUNT,
        registration_status
	
        
   FROM {{ source('de_project', 'user_journey') }} u
 )


SELECT * FROM user_journey