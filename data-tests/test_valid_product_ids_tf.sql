/*
    Test Name: Valid Product IDs Test
    Description: Validates that all product_ids in stg_user_journey_tf exist in stg_product_data_tf
    Note: This test ignores 'UNKNOWN' product_ids
*/

-- Find product_ids in stg_user_journey_tf that don't exist in stg_product_data_tf
WITH missing_product_ids AS (
    SELECT DISTINCT
      uj.product_id as invalid_product_id,
      'stg_user_journey_tf' as source_table
    FROM {{ ref('stg_user_journey_tf') }} uj
    LEFT JOIN {{ ref('stg_product_data_tf') }} p
    ON uj.product_id = p.product_id
    WHERE p.product_id IS NULL
      AND uj.product_id IS NOT NULL
      AND uj.product_id != 'UNKNOWN'
)

-- Return results
SELECT * FROM missing_product_ids
