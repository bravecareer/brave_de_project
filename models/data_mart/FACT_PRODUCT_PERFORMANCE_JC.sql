{{ config(
    materialized='table',
    unique_key='product_id'
) }}

WITH product_performance AS (
    SELECT
        p.product_id,
        COUNT(s.search_event_id) AS views,
        SUM(CASE WHEN s.has_atc THEN 1 ELSE 0 END) AS atc_events,
        SUM(CASE WHEN s.has_purchase THEN 1 ELSE 0 END) AS purchases
    FROM {{ source('de_project', 'user_journey') }} s
    JOIN {{ source('de_project', 'product_data') }} p ON s.product_id = p.product_id
    GROUP BY p.product_id
)

SELECT DISTINCT
    product_id,
    views,
    atc_events,
    purchases
FROM product_performance