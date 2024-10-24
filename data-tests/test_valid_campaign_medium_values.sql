-- Test for valid campaign medium values in fact_campaign_effectiveness_PS
SELECT
  DISTINCT mkt_medium
FROM {{ ref('fact_campaign_effectiveness_PS') }}
WHERE mkt_medium NOT IN ('paid search', 'email', 'social', 'display')
