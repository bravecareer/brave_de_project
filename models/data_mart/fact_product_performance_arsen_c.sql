{{ config(
    materialized='incremental',
    unique_key=['search_event_id', 'user_id', 'product_id']
) }}

WITH user_journey AS (
    SELECT
        uj.search_event_id,
        uj.user_id,
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
        uj.search_event_id,
        uj.user_id,
        uj.product_id,
        p.product_name,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase
   FROM user_journey uj
   LEFT JOIN product_data p ON uj.product_id = p.product_id
)

SELECT * FROM final