-- data-tests/test_pipeline_watermarks_kl_coverage.sql
-- 确保所有source都被追踪

WITH expected_sources AS (
    SELECT 'user_journey' AS source_table
    UNION ALL SELECT 'product_data'
    UNION ALL SELECT 'user_data'
    UNION ALL SELECT 'inventory_data'
)

SELECT e.source_table
FROM expected_sources e
LEFT JOIN {{ ref('pipeline_watermarks_kl') }} w 
    ON e.source_table = w.source_table
WHERE w.source_table IS NULL