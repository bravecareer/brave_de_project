{{ config(
    materialized='incremental',
    unique_key=['product_id']
) }}

WITH product_performance AS (
    SELECT
        uj.product_id,
        COUNT(DISTINCT uj.search_event_id) AS total_views,
        SUM(CASE WHEN uj.has_atc THEN 1 ELSE 0 END) AS total_atc_events,
        SUM(CASE WHEN uj.has_purchase AND uj.has_atc THEN 1 ELSE 0 END) AS total_purchases, -- Ensure purchases are only counted if ATC event occurred
        MAX(p.price) AS price,
        MAX(p.rating) AS rating,
        SUM(p.sales_volume) AS sales_volume,
        MAX(p.discount_percentage) AS discount_percentage,
        MAX(p.weight_grams) AS weight_grams
    FROM
        brave_database.de_project.user_journey uj
    JOIN
        brave_database.de_project.product_data p
    ON
        uj.product_id = p.product_id
    GROUP BY
        uj.product_id
)

SELECT
    product_id,
    total_views,
    total_atc_events,
    total_purchases,
    price,
    rating,
    sales_volume,
    discount_percentage,
    weight_grams
FROM
    product_performance
