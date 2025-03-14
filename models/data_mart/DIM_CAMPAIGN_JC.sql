{{ config(
    materialized='table',
    unique_key='mkt_campaign'
) }}

WITH cleaned_campaign_data AS (
    SELECT
        c.mkt_campaign,
        c.mkt_medium,
        c.mkt_source,
        c.mkt_content
    FROM {{ source('de_project', 'user_journey') }} c
    WHERE c.mkt_campaign IS NOT NULL
)

SELECT
    mkt_campaign,
    mkt_medium,
    mkt_source,
    mkt_content
FROM cleaned_campaign_data