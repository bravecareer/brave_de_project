-- Test for valid user account status values in fact_campaign_effectiveness_PS
SELECT
  DISTINCT account_status
FROM {{ ref('fact_campaign_effectiveness_PS') }}
WHERE account_status NOT IN ('active', 'inactive', 'banned')
