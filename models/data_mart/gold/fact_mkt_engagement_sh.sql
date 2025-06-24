{{ config(
   materialized='incremental',
   unique_key= ['product_id']
) }}

SELECT
    m.product_id,
    COUNT(CASE WHEN m.has_atc THEN 1 END) AS ATCS,
    COUNT(CASE WHEN m.has_purchase THEN 1 END) AS quantity_sold,
    m.mkt_campaign,
    m.mkt_content,
    m.mkt_medium,
    m.mkt_source
    FROM {{ref('dim_campaign_sh')}} m INNER JOIN {{ref('raw_purchased_products_sh')}} p 
    ON m.product_id = p.product_id
    GROUP BY m.product_id, m.mkt_campaign, m.mkt_content, m.mkt_medium, m.mkt_medium, m.mkt_source
