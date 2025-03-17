{{ config(
   materialized='table',
   unique_key='mkt_campaign'
) }}

WITH campaign_effectiveness AS (
	SELECT
		uj.mkt_campaign,
		uj.mkt_content,
		uj.mkt_medium,
		uj.mkt_source,
		uj.geo_country,
		uj.geo_region,
		COUNT(uj.has_qv) AS views,
      COUNT(uj.has_pdp) AS detail_views,
      COUNT(uj.has_atc) AS atc_events,
      COUNT(uj.has_purchase) AS purchases,
      SUM(CASE WHEN uj.has_purchase THEN p.price ELSE 0 END) AS revenue
	  FROM {{ ref('stg_user_journey_bl') }} uj
	  LEFT JOIN {{ ref('dim_product_bl') }} p
	    ON uj.product_id = p.product_id
	 GROUP BY CUBE(uj.mkt_campaign, uj.mkt_content, uj.mkt_medium, uj.mkt_source, uj.geo_country, uj.geo_region)
)

SELECT * FROM campaign_effectiveness