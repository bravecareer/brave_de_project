{{ config(
    materialized='table',
    unique_key='mkt_campaign'
) }}

WITH campaign_effectiveness AS (
    SELECT
        s.mkt_campaign,
        s.user_id,
        s.product_id,
        COUNT(s.search_event_id) AS impressions_with_attribution,
        SUM(CASE WHEN s.has_atc THEN 1 ELSE 0 END) AS atc_count,
        SUM(CASE WHEN s.has_purchase THEN 1 ELSE 0 END) AS purchase_count
    FROM {{ source('de_project', 'user_journey') }} s
    GROUP BY s.mkt_campaign, s.user_id, s.product_id
)

SELECT
    mkt_campaign,
    user_id,
    product_id,
    impressions_with_attribution,
    atc_count,
    purchase_count
FROM campaign_effectiveness