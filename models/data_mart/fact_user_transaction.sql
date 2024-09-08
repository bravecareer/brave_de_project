{{ config(
    materialized='incremental',
    unique_key='session_id'
) }}

WITH user_journey AS (
    SELECT
        uj.user_id,
        uj.product_id,
        uj.has_purchase,
        uj.session_id,
        uj.cart_id,
        uj.timestamp
    FROM {{ source('de_project', 'user_journey_data') }} uj
    WHERE uj.has_purchase = TRUE
),

transaction_data AS (
    SELECT
        uj.user_id,
        uj.product_id,
        MAX(uj.has_purchase) AS has_purchase, -- Using MAX() to include the field in the SELECT since it's always TRUE here
        COUNT(uj.has_purchase) AS quantity_sold, -- Counting the number of purchase events
        SUM(p.price) AS total_amount, -- Summing the price of the purchased products
        uj.timestamp,
        uj.session_id,
        uj.cart_id
    FROM user_journey uj
    JOIN {{ source('de_project', 'user_data') }} ud
      ON uj.user_id = ud.user_id
    JOIN {{ source('de_project', 'product_data') }} p
      ON uj.product_id = p.product_id
    GROUP BY uj.user_id, uj.product_id, uj.timestamp, uj.session_id, uj.cart_id
)

SELECT * FROM transaction_data
