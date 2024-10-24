-- Ensure no duplicate unique keys
WITH duplicate_keys AS (
    SELECT
        user_id,
        search_event_id,
        timestamp,
        COUNT(*) AS count
    FROM {{ ref('fact_search_effectiveness_om') }}
    GROUP BY user_id, search_event_id, timestamp
    HAVING COUNT(*) > 1
),
-- Ensure valid user IDs
invalid_users AS (
    SELECT
        fe.user_id,
        fe.search_event_id,
        fe.timestamp,
        NULL AS product_id
    FROM {{ ref('fact_search_effectiveness_om') }} fe
    LEFT JOIN {{ source('de_project', 'user_data') }} ud
    ON fe.user_id = ud.user_id
    WHERE ud.user_id IS NULL
),
-- Ensure valid product IDs
invalid_products AS (
    SELECT
        fe.user_id,
        fe.search_event_id,
        fe.timestamp,
        fe.product_id
    FROM {{ ref('fact_search_effectiveness_om') }} fe
    LEFT JOIN {{ source('de_project', 'product_data') }} pd
    ON fe.product_id = pd.product_id
    WHERE pd.product_id IS NULL
)
-- Select results from each CTE
SELECT user_id, search_event_id, timestamp, NULL AS product_id FROM duplicate_keys
UNION ALL
SELECT user_id, search_event_id, timestamp, product_id FROM invalid_users
UNION ALL
SELECT user_id, search_event_id, timestamp, product_id FROM invalid_products
