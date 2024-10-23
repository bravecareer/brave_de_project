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