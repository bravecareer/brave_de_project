WITH event_aggregates AS (
    SELECT
        product_id,
        COUNT(*) AS total_views,
        SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) AS total_atc_events,
        SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS total_purchases
    FROM {{ ref('stg_user_journey_ba') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.product_category,
    p.price,
    p.rating,
    p.discount_percentage,
    ea.total_views,
    ea.total_atc_events,
    ea.total_purchases,
    ROUND((ea.total_atc_events * 100.0) / NULLIF(ea.total_views, 0), 2) AS atc_rate_percentage,
    ROUND((ea.total_purchases * 100.0) / NULLIF(ea.total_views, 0), 2) AS purchase_rate_percentage
FROM event_aggregates ea
LEFT JOIN {{ ref('stg_product_data_ba') }} p ON ea.product_id = p.product_id