-- Check for duplicate campaign_ids
WITH duplicate_campaigns AS (
    SELECT
        campaign_id,
        COUNT(*) AS count
    FROM {{ ref('fact_campaign_effectiveness_om') }}
    GROUP BY campaign_id
    HAVING COUNT(*) > 1
)

SELECT campaign_id, count
FROM duplicate_campaigns
