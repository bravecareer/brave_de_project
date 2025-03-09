{{ config(
   materialized='incremental',
   unique_key='campaign_id'
) }}

-- Dimension table for campaign details
-- Stores campaign attributes and configuration
WITH campaign_data AS (
   SELECT DISTINCT
       COALESCE(uj.mkt_campaign, 'Unknown') as campaign_id,
       COALESCE(uj.mkt_source, 'Unknown') as campaign_source,
       COALESCE(uj.mkt_medium, 'Unknown') as campaign_medium,
       COALESCE(uj.mkt_content, 'Unknown') as campaign_content,
       -- Geographic targeting information
       COALESCE(uj.geo_country, 'Unknown') as target_country,
       COALESCE(uj.geo_region, 'Unknown') as target_region,
       -- Device targeting
       COALESCE(uj.device_class, 'Unknown') as target_device,
       -- Language targeting
       COALESCE(uj.page_language, 'Unknown') as campaign_language,
       -- Additional campaign attributes would ideally come from a campaign management system
       -- For now, we'll use default values instead of NULL
       'Campaign ' || COALESCE(uj.mkt_campaign, 'Unknown') as campaign_name,
       MAX(uj.event_timestamp) as last_updated
   FROM {{ ref('stg_user_journey_tf') }} uj
   WHERE uj.mkt_campaign IS NOT NULL AND uj.mkt_campaign != 'UNKNOWN'
   {% if is_incremental() %}
   -- Only process new or updated campaigns in incremental runs
   AND uj.event_timestamp >= CURRENT_DATE() - 3
   {% endif %}
   GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT * FROM campaign_data
