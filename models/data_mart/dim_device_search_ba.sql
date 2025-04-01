{{ config(
   materialized='table'
) }}

-- depends_on: {{ ref('stg_user_journey_ba') }}

WITH device AS (
    SELECT DISTINCT
        d.device_class,
        d.dvce_screenwidth,
        d.dvce_screenheight
FROM {{ ref('stg_user_journey_ba') }} d
)

SELECT * FROM device