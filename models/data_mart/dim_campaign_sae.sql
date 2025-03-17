{{ config(
    materialized='table',
    schema='PROJECT_TEST',
    cluster_by=['campaign_id'],
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT pk_campaign_id PRIMARY KEY (campaign_id)"
    ]
) }}

WITH campaign_data AS (
  SELECT
    LOWER(TRIM(MKT_CAMPAIGN)) AS campaign_id,
    LOWER(TRIM(MKT_SOURCE)) AS campaign_source,
    LOWER(TRIM(MKT_MEDIUM)) AS campaign_medium,
    LOWER(TRIM(MKT_CONTENT)) AS campaign_content,
    UPPER(TRIM(GEO_COUNTRY)) AS target_country,
    UPPER(TRIM(GEO_REGION)) AS target_region,
    CASE 
      WHEN LOWER(DEVICE_CLASS) LIKE '%mobile%' THEN 'Mobile'
      ELSE 'Desktop'
    END AS target_device,
    LOWER(TRIM(BR_LANG)) AS campaign_language,
    LOAD_TIMESTAMP
  FROM {{ ref('stg_user_journey_sae') }}
  WHERE MKT_CAMPAIGN IS NOT NULL
),

aggregated AS (
  SELECT
    campaign_id,
    campaign_source,
    campaign_medium,
    campaign_content,
    target_country,
    target_region,
    target_device,
    campaign_language,
    CONCAT('Campaign ', INITCAP(campaign_id)) AS campaign_name,
    MAX(LOAD_TIMESTAMP) AS last_updated
  FROM campaign_data
  GROUP BY 
    campaign_id,
    campaign_source,
    campaign_medium,
    campaign_content,
    target_country,
    target_region,
    target_device,
    campaign_language
)

SELECT *
FROM aggregated
ORDER BY campaign_id





-- {{ config(materialized='table', schema='PROJECT_TEST', cluster_by=['campaign_id']) }}

-- WITH campaign_data AS (
--   SELECT
--     LOWER(TRIM(MKT_CAMPAIGN)) AS campaign_id,
--     LOWER(TRIM(MKT_SOURCE)) AS campaign_source,
--     LOWER(TRIM(MKT_MEDIUM)) AS campaign_medium,
--     LOWER(TRIM(MKT_CONTENT)) AS campaign_content,
--     UPPER(TRIM(GEO_COUNTRY)) AS target_country,
--     UPPER(TRIM(GEO_REGION)) AS target_region,
--     CASE 
--       WHEN LOWER(DEVICE_CLASS) LIKE '%mobile%' THEN 'Mobile'
--       ELSE 'Desktop'
--     END AS target_device,
--     LOWER(TRIM(BR_LANG)) AS campaign_language,
--     LOAD_TIMESTAMP
--   FROM {{ ref('stg_user_journey_sae') }}
--   WHERE MKT_CAMPAIGN IS NOT NULL
-- ),

-- aggregated AS (
--   SELECT
--     campaign_id,
--     campaign_source,
--     campaign_medium,
--     campaign_content,
--     target_country,
--     target_region,
--     target_device,
--     campaign_language,
--     CONCAT('Campaign ', INITCAP(campaign_id)) AS campaign_name,
--     MAX(LOAD_TIMESTAMP) AS last_updated
--   FROM campaign_data
--   GROUP BY 
--     campaign_id,
--     campaign_source,
--     campaign_medium,
--     campaign_content,
--     target_country,
--     target_region,
--     target_device,
--     campaign_language
-- )

-- SELECT *
-- FROM aggregated
-- ORDER BY campaign_id
