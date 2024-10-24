{{ config(
    materialized='incremental',
    unique_key=['campaign_id']
) }}

WITH user_journey AS (
    SELECT
        uj.search_event_id,
        uj.user_id,
        uj.has_atc,
        uj.has_purchase,
        uj.product_id,
        uj.mkt_campaign AS campaign_id
    FROM {{ source('de_project', 'user_journey') }} uj
    WHERE uj.mkt_campaign IS NOT NULL
),
campaign_performance AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT search_event_id) AS total_views,
        SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) AS total_atc_events,
        SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS total_purchases
    FROM user_journey
    GROUP BY campaign_id
),
deduplicated_campaign_performance AS (
    SELECT
        campaign_id,
        MAX(total_views) AS total_views,
        MAX(total_atc_events) AS total_atc_events,
        MAX(total_purchases) AS total_purchases
    FROM campaign_performance
    GROUP BY campaign_id
)

SELECT * FROM deduplicated_campaign_performance
