{{ config(
   materialized='incremental',
   unique_key= ['product_id']
) }}

WITH mkt_data AS(

    SELECT  
    uj.product_id,
    uj.has_atc,
    uj.has_purchase,
    uj.mkt_campaign,
    uj.mkt_content,
    uj.mkt_medium,
    uj.mkt_source
    FROM {{ source('de_project', 'user_journey') }} uj
    WHERE --(uj.has_purchase = TRUE OR uj.has_atc =TRUE) 
             uj.timestamp >= CURRENT_DATE() - 5

),

mkt_data_by_product AS(
    SELECT
    m.product_id,
    COUNT(CASE WHEN m.has_atc THEN 1 END) AS ATCS,
    COUNT(CASE WHEN m.has_purchase THEN 1 END) AS quantity_sold,
    m.mkt_campaign,
    m.mkt_content,
    m.mkt_medium,
    m.mkt_source
    FROM mkt_data m LEFT JOIN {{source('de_project', 'product_data')}} p 
    ON m.product_id = p.product_id
    GROUP BY m.product_id, m.mkt_campaign, m.mkt_content, m.mkt_medium, m.mkt_medium, m.mkt_source
)



SELECT * from mkt_data_by_product

