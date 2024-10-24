WITH product_interactions AS (
   SELECT
       uj.product_id,
       uj.timestamp,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase
   FROM {{ ref('stg_user_journey_PS') }} uj
   WHERE uj.product_id IS NOT NULL
),

valid_products AS (
   SELECT
       p.product_id,
       p.price,
       p.rating,
       p.weight_grams
   FROM {{ ref('stg_product_data_PS') }} p
)

-- Final aggregation step
SELECT
    pi.product_id,
    -- Aggregate key metrics
    SUM(CASE WHEN pi.has_qv = TRUE THEN 1 ELSE 0 END) AS total_qv_events,
    SUM(CASE WHEN pi.has_pdp = TRUE THEN 1 ELSE 0 END) AS total_pdp_views,
    SUM(CASE WHEN pi.has_atc = TRUE THEN 1 ELSE 0 END) AS total_atc_events,
    COUNT(CASE WHEN pi.has_purchase = TRUE THEN 1 END) AS quantity_sold,
    SUM(CASE WHEN pi.has_purchase = TRUE THEN vp.price ELSE 0 END) AS total_sales_amount,
    AVG(vp.rating) AS avg_rating,  -- Average rating of the product
    AVG(vp.weight_grams) AS avg_weight  -- Average weight of the product
FROM product_interactions pi
LEFT JOIN valid_products vp ON pi.product_id = vp.product_id  -- Join only on product_id
GROUP BY 
    pi.product_id
