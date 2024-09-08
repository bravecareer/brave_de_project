{{ config(
    materialized='incremental',
    unique_key='session_id'
) }}

WITH session_data AS (
    SELECT
        uj.session_id,
        uj.cart_id,
        uj.timestamp
    FROM {{ source('de_project', 'user_journey_data') }} uj
    WHERE uj.session_id IS NOT NULL
)

SELECT * FROM session_data
