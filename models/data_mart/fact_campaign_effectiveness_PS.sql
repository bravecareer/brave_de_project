{{ config(
   materialized='incremental',
   unique_key=['campaign_id', 'session_id', 'timestamp']
) }}

WITH campaign_interactions AS (
  SELECT
    uj.mkt_campaign AS campaign_id,
    uj.session_id AS session_id,
    uj.user_id AS user_id,
    uj.timestamp AS timestamp,
    COUNT(uj.session_id) AS total_interactions,
    SUM(CASE WHEN uj.has_purchase = TRUE THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN uj.has_atc = TRUE THEN 1 ELSE 0 END) AS has_atc,
    SUM(CASE WHEN uj.has_purchase = TRUE THEN 1 ELSE 0 END) AS has_purchase,
    uj.mkt_medium AS mkt_medium,
    uj.mkt_source AS mkt_source,
    uj.mkt_content AS mkt_content
  FROM {{ ref('stg_user_journey_PS') }} uj
  WHERE uj.mkt_campaign IS NOT NULL AND uj.session_id IS NOT NULL
  GROUP BY uj.mkt_campaign, uj.session_id, uj.user_id, uj.timestamp, uj.mkt_medium, uj.mkt_source, uj.mkt_content
),

valid_users AS (
  SELECT
    u.user_id,
    u.account_status
  FROM {{ ref('stg_user_data_PS') }} u
  WHERE u.account_status IN ('active', 'inactive', 'banned')
)

SELECT
  ci.campaign_id,
  ci.session_id,
  ci.user_id,
  vu.account_status,
  ci.timestamp,
  ci.total_interactions,
  ci.total_purchases,
  CASE WHEN ci.total_interactions > 0 THEN ci.total_purchases / ci.total_interactions ELSE 0 END AS conversion_rate,
  ci.has_atc,
  ci.has_purchase,
  ci.mkt_medium,
  ci.mkt_source,
  ci.mkt_content
FROM campaign_interactions ci
LEFT JOIN valid_users vu ON ci.user_id = vu.user_id

{% if is_incremental() %}
  WHERE ci.session_id NOT IN (SELECT session_id FROM {{ this }})
{% endif %}
