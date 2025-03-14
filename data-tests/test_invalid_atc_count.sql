-- Test for invalid atc_count in campaign_effectiveness
WITH invalid_atc_count AS (
  SELECT
    mkt_campaign,
    user_id,
    product_id,
    atc_count
  FROM {{ ref('FACT_CAMPAIGN_EFFECTIVENESS_JC') }}
  WHERE atc_count < 0
)
SELECT * FROM invalid_atc_count