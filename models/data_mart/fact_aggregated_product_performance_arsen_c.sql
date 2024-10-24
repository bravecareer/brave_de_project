{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

WITH user_journey AS (
    SELECT
        uj.product_id,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase
    FROM {{ source('de_project', 'user_journey') }} uj
),

product_data AS (
    SELECT
        p.product_id,
        p.product_name
    FROM {{ source('de_project', 'product_data') }} p
),

final AS (
    SELECT
        uj.product_id,
        p.product_name,
        COUNT(uj.has_qv) AS qv_count,
        COUNT(uj.has_pdp) AS pdp_count,
        COUNT(uj.has_atc) AS atc_count,
        COUNT(uj.has_purchase) AS purchase_count
   FROM user_journey uj
   LEFT JOIN product_data p ON uj.product_id = p.product_id
   GROUP BY uj.product_id, p.product_name
)

SELECT * FROM final