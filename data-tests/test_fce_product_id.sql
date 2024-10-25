-- This test checks if the product_id in the fact_campaign_effectiveness_qu
-- table is present in the stg_product_data_qu table and if the product_price
-- in the fact_campaign_effectiveness_qu table is the same as the price in the
-- stg_product_data_qu table.
-- The test will fail if the product_id is not present in the stg_product_data_qu
-- table or if the product_price in the fact_campaign_effectiveness_qu table is
-- not the same as the price in the stg_product_data_qu table.

SELECT
    fce.product_id,
    fce.product_price AS fce_price,
    spd.price AS spd_price
FROM {{ref ('fact_campaign_effectiveness_qu')}} fce
    
LEFT JOIN {{ref('stg_product_data_qu')}} spd
ON
    fce.product_id = spd.product_id
WHERE
    spd.product_id IS NULL
    OR fce.product_price != spd.price