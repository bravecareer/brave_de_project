-- Test: validate fact_daily_metrics_kl foreign keys and grain uniqueness
-- Expected result: 0 rows

WITH grain_duplicates AS (
    SELECT
        'duplicate_fact_grain' AS issue,
        date_key,
        geo_key,
        device_class,
        mkt_source,
        mkt_campaign,
        COUNT(*) AS issue_count
    FROM {{ ref('fact_daily_metrics_kl') }}
    GROUP BY
        date_key,
        geo_key,
        device_class,
        mkt_source,
        mkt_campaign
    HAVING COUNT(*) > 1
),

orphan_date_keys AS (
    SELECT
        'orphan_date_key' AS issue,
        f.date_key,
        f.geo_key,
        f.device_class,
        f.mkt_source,
        f.mkt_campaign,
        COUNT(*) AS issue_count
    FROM {{ ref('fact_daily_metrics_kl') }} f
    LEFT JOIN {{ ref('dim_date_kl') }} d
        ON f.date_key = d.date_key
    WHERE d.date_key IS NULL
    GROUP BY
        f.date_key,
        f.geo_key,
        f.device_class,
        f.mkt_source,
        f.mkt_campaign
),

orphan_geo_keys AS (
    SELECT
        'orphan_geo_key' AS issue,
        f.date_key,
        f.geo_key,
        f.device_class,
        f.mkt_source,
        f.mkt_campaign,
        COUNT(*) AS issue_count
    FROM {{ ref('fact_daily_metrics_kl') }} f
    LEFT JOIN {{ ref('dim_geography_kl') }} g
        ON f.geo_key = g.geo_key
    WHERE g.geo_key IS NULL
    GROUP BY
        f.date_key,
        f.geo_key,
        f.device_class,
        f.mkt_source,
        f.mkt_campaign
)

SELECT * FROM grain_duplicates
UNION ALL
SELECT * FROM orphan_date_keys
UNION ALL
SELECT * FROM orphan_geo_keys
