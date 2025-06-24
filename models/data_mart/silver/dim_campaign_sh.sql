{{ config(
   materialized='view',
   unique_key='product_id'
) }}

SELECT
product_id,
mkt_campaign,
mkt_content,
mkt_medium,
mkt_source,
has_atc,
has_purchase,
timestamp
FROM {{ref ('raw_market_data_sh')}}