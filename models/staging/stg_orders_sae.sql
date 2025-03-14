{{ config(materialized='table', schema='PROJECT_TEST') }}

WITH raw_data AS (
    SELECT 
        id::VARCHAR(36) AS id,
        customer::VARCHAR(36) AS customer,
        ordered_at,
        store_id::VARCHAR(36) AS store_id,
        subtotal::NUMBER AS subtotal,
        tax_paid::NUMBER AS tax_paid,
        order_total::NUMBER AS order_total
    FROM {{ ref('raw_orders') }}
    WHERE id IS NOT NULL
      AND customer IS NOT NULL
      AND ordered_at IS NOT NULL
      AND store_id IS NOT NULL
),

cleaned AS (
    SELECT
        REPLACE(REPLACE(id, '-', ''), ' ', '') AS order_id,
        REPLACE(REPLACE(customer, '-', ''), ' ', '') AS customer_id,
        REPLACE(REPLACE(store_id, '-', ''), ' ', '') AS store_id,
        subtotal,
        tax_paid,
        order_total,
        CAST(ordered_at AS DATE) AS order_date,
        TO_CHAR(ordered_at, 'HH24:MI:SS') AS order_time
    FROM raw_data
)

SELECT 
    order_id,
    customer_id,
    store_id,
    subtotal,
    tax_paid,
    order_total,
    order_date,
    order_time
FROM cleaned
WHERE order_total = subtotal + tax_paid
