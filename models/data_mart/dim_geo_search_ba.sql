/*GeoKey	Surrogate key
Country, Region, City, ZipCode	
Latitude, Longitude, TimeZone8*/

{{ config(
   materialized='table'
) }}

WITH geo AS (
    SELECT DISTINCT
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