-- Test: Verify geo_key is unique
-- Expected: 0 rows

SELECT 
    geo_key,
    COUNT(*) AS duplicate_count
FROM {{ ref('dim_geography_kl') }}
GROUP BY geo_key
HAVING COUNT(*) > 1