{{ config(
   materialized='table'
) }}

-- depends_on: {{ ref('stg_user_journey_ba') }}

WITH marketing AS (
    SELECT DISTINCT
        m.mkt_campaign,
        m.mkt_medium,
        m.mkt_source,
        m.mkt_content
FROM {{ ref('stg_user_journey_ba') }} m
)

SELECT * FROM marketing
