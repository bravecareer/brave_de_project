-- Test: Verify geography key fields are not NULL
-- Expected: 0 rows

SELECT 
    geo_key,
    country,
    zipcode
FROM {{ ref('dim_geography_kl') }}
WHERE geo_key IS NULL
   OR country IS NULL