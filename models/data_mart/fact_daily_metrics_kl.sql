{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date_key', 'geo_key', 'device_class', 'mkt_source', 'mkt_campaign']
) }}

SELECT
    -- Foreign keys
    d.date_key,
    g.geo_key,

    -- Degenerate dimensions
    uj.device_class,
    COALESCE(uj.mkt_source, 'organic') AS mkt_source,
    COALESCE(uj.mkt_campaign, 'none') AS mkt_campaign,

    -- User metrics
    COUNT(DISTINCT uj.user_id) AS daily_active_users,
    COUNT(DISTINCT CASE WHEN uj.has_qv THEN uj.user_id END) AS unique_qv_users,
    COUNT(DISTINCT CASE WHEN uj.has_purchase THEN uj.user_id END) AS unique_buyers,

    -- Event metrics
    SUM(CASE WHEN uj.has_qv THEN 1 ELSE 0 END) AS qv_count,
    SUM(CASE WHEN uj.has_pdp THEN 1 ELSE 0 END) AS pdp_view_count,
    SUM(CASE WHEN uj.has_atc THEN 1 ELSE 0 END) AS add_to_cart_count,
    SUM(CASE WHEN uj.has_purchase THEN 1 ELSE 0 END) AS purchase_count,

    COUNT(DISTINCT uj.session_id) AS session_count,

    -- Conversion metric
    DIV0(
        COUNT(DISTINCT CASE WHEN uj.has_purchase THEN uj.user_id END),
        COUNT(DISTINCT CASE WHEN uj.has_qv THEN uj.user_id END)
    ) AS qv_to_purchase_rate,

    -- Audit
    CURRENT_TIMESTAMP() AS loaded_at

FROM {{ ref('stg_user_journey_kl') }} uj
JOIN {{ ref('dim_date_kl') }} d
    ON CAST(uj.event_timestamp AS DATE) = d.full_date
JOIN {{ ref('dim_geography_kl') }} g
    ON uj.geo_country = g.country
    AND uj.geo_zipcode = g.zipcode

{% if is_incremental() %}
-- Reprocess the latest loaded daily partition so MERGE can update metrics
-- if source records for the latest date arrive or change after the prior run.
WHERE d.date_key >= (
    SELECT COALESCE(MAX(date_key), 0)
    FROM {{ this }}
)
{% endif %}

GROUP BY
    d.date_key,
    g.geo_key,
    uj.device_class,
    mkt_source,
    mkt_campaign
