/*DeviceKey	Surrogate key
DeviceClass, BrowserLang, ViewHeight, ViewWidth, ScreenHeight, ScreenWidth, etc.
*/

{{ config(
   materialized='table'
) }}

WITH device AS (
    SELECT DISTINCT
        d.device_class,
        d.dvce_screenwidth,
        d.dvce_screenheight
FROM {{ ref('stg_user_journey_ba') }} d
)

SELECT * FROM device