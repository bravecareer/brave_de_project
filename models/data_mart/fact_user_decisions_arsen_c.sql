{{ config(
    materialized='incremental',
    unique_key=['user_id', 'product_id', 'search_event_id']
) }}

WITH user_journey AS (
    SELECT
        uj.user_id,
        uj.product_id,
        uj.search_event_id,
        to_timestamp(replace(timestamp,' UTC','')) AS timestamp,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        uj.search_results_count,
        uj.app_id,
        uj.search_terms,
        uj.search_feature,
        uj.login_status,
        uj.registration_status,
        uj.banner,
        uj.device_class,
        uj.search_model
    FROM {{ source('de_project', 'user_journey') }} uj
)

SELECT * FROM user_journey