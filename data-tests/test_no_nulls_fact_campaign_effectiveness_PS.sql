-- Test for no NULL values in critical columns for fact_campaign_effectiveness_PS
SELECT
  user_id,
  campaign_id,
  session_id
FROM {{ ref('fact_campaign_effectiveness_PS') }}
WHERE user_id IS NULL
  OR campaign_id IS NULL
  OR session_id IS NULL
