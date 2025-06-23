-- Ensure no duplicate values  are in for critical columns groups--

SELECT
m.product_id, 
m.mkt_campaign, 
m.mkt_content, 
m.mkt_medium, 
m.mkt_medium, 
m.mkt_source
FROM {{ref('fact_mkt_engagement_sh')}} m
GROUP BY m.product_id, m.mkt_campaign, m.mkt_content, m.mkt_medium, m.mkt_medium, m.mkt_source
HAVING COUNT(*) >1