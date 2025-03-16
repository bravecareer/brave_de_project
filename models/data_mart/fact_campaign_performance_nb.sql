{{ config(
   materialized='incremental',
   unique_key= ['user_id', 'search_event_id', 'timestamp']
) }}

WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.mkt_campaign,
       uj.mkt_medium,
       uj.mkt_source,
       uj.mkt_content,
       uj.geo_country,
       uj.geo_region,
       uj.geo_city,
       uj.geo_zipcode,
       uj.session_id,
       to_timestamp(replace(uj.timestamp,' UTC','')) AS timestamp
   FROM {{ ref('stg_campaign_performance_nb') }} uj
),

valid_users AS (
   SELECT
       u.user_id
   FROM {{ ref('stg_user_data_nb') }} u
   WHERE u.account_status = 'active'
),

final AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.mkt_campaign,
       uj.mkt_medium,
       uj.mkt_source,
       uj.mkt_content,
       uj.geo_country,
       uj.geo_region,
       uj.geo_city,
       uj.geo_zipcode,
       uj.session_id,
       uj.timestamp
   FROM user_journey uj
   LEFT JOIN valid_users vu ON uj.user_id = vu.user_id
)

SELECT * FROM final
