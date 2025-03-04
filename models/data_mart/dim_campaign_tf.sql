{{ config(
   materialized='view',
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
       -- For now, we'll use placeholder fields that could be updated manually or through a separate process
       'Campaign ' || COALESCE(uj.mkt_campaign, 'Unknown') as campaign_name,
       CAST(NULL AS DECIMAL) as budget,
       CAST(NULL AS DATE) as start_date,
       CAST(NULL AS DATE) as end_date,
       CAST(NULL AS STRING) as campaign_objective,
       CAST(NULL AS STRING) as target_audience,
       CAST(NULL AS STRING) as campaign_type,
       CAST(NULL AS STRING) as campaign_status,
       current_timestamp() as last_updated
   FROM {{ ref('stg_user_journey_tf') }} uj
   WHERE uj.mkt_campaign IS NOT NULL AND uj.mkt_campaign != 'UNKNOWN'
)

SELECT * FROM campaign_data
