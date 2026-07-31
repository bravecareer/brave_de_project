{{ config(
    materialized='table',
    tags=['dimension', 'core']
) }}

WITH unique_locations AS (
    SELECT DISTINCT
        geo_country,
        geo_zipcode
    FROM {{ source('de_project', 'user_journey') }}
    WHERE geo_country IS NOT NULL
),

geography_dimension AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['geo_country', 'geo_zipcode']) }} AS geo_key,
        
        -- Geography Attributes
        geo_country AS country,
        geo_zipcode AS zipcode,
        
        -- Derived Attributes
        CASE 
            WHEN geo_country IN ('US', 'CA', 'MX') THEN 'North America'
            WHEN geo_country IN ('CN', 'JP', 'KR') THEN 'Asia'
            WHEN geo_country IN ('GB', 'DE', 'FR') THEN 'Europe'
            ELSE 'Other'
        END AS region,
        
        -- Audit
        CURRENT_TIMESTAMP() AS loaded_at
        
    FROM unique_locations
)

SELECT * FROM geography_dimension
ORDER BY country, zipcode