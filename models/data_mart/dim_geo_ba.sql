{{ config(
   materialized='table'
) }}

-- depends_on: {{ ref('stg_user_journey_ba') }}

WITH geo AS (
    SELECT 
        {{ 
        dbt_utils.generate_surrogate_key([
        'geo_latitude',
        'geo_longitude',
        ]) 
    }} AS geo_id,
        g.geo_country,
        geo_region,
        geo_city,
        geo_zipcode,
        geo_latitude,
        geo_longitude,
        geo_timezone
FROM {{ ref('stg_user_journey_ba') }} g
)

SELECT * FROM geo