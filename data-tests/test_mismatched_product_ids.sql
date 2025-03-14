-- Test for mismatched product_id between user_journey and product_data
WITH mismatched_product_ids AS (
  SELECT
    s.product_id AS user_journey_product_id,
    p.product_id AS product_data_product_id
  FROM {{ source('de_project', 'user_journey') }} s
  LEFT JOIN {{ source('de_project', 'product_data') }} p
    ON s.product_id = p.product_id
  WHERE p.product_id IS NULL
)
SELECT * FROM mismatched_product_ids