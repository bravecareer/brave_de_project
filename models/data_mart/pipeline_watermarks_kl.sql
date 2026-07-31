/*
****************************************************************************************************
MODEL: pipeline_watermarks
PURPOSE: 
    - Track processing progress for each source table
    - Enable incremental processing by recording high watermark
    - Support pipeline resume from failure
GRAIN: One row per source table
USAGE:
    - Query this table to get last processed timestamp
    - Use in incremental models: WHERE timestamp > (SELECT high_watermark FROM pipeline_watermarks)
****************************************************************************************************
*/

{{ config(
    materialized='table',
    schema='pipeline_metadata',
    tags=['infrastructure', 'monitoring']
) }}

WITH user_journey_watermark AS (
    SELECT 
        'user_journey' AS source_table,
        'de_project' AS source_schema,
        MAX(TO_TIMESTAMP(REPLACE(timestamp, ' UTC', ''))) AS high_watermark,
        COUNT(*) AS total_rows_processed,
        COUNT(DISTINCT user_id) AS unique_users,
        CURRENT_TIMESTAMP() AS watermark_updated_at
    FROM {{ source('de_project', 'user_journey') }}
),

product_data_watermark AS (
    SELECT 
        'product_data' AS source_table,
        'de_project' AS source_schema,
        NULL AS high_watermark,  -- No timestamp in product_data
        COUNT(*) AS total_rows_processed,
        COUNT(DISTINCT product_id) AS unique_products,
        CURRENT_TIMESTAMP() AS watermark_updated_at
    FROM {{ source('de_project', 'product_data') }}
),

user_data_watermark AS (
    SELECT 
        'user_data' AS source_table,
        'de_project' AS source_schema,
        NULL AS high_watermark,  -- No timestamp in user_data
        COUNT(*) AS total_rows_processed,
        COUNT(DISTINCT user_id) AS unique_users,
        CURRENT_TIMESTAMP() AS watermark_updated_at
    FROM {{ source('de_project', 'user_data') }}
),

inventory_data_watermark AS (
    SELECT 
        'inventory_data' AS source_table,
        'de_project' AS source_schema,
        NULL AS high_watermark,  -- Check if inventory_data has timestamp
        COUNT(*) AS total_rows_processed,
        NULL AS unique_entities,
        CURRENT_TIMESTAMP() AS watermark_updated_at
    FROM {{ source('de_project', 'inventory_data') }}
),

all_watermarks AS (
    SELECT * FROM user_journey_watermark
    UNION ALL
    SELECT * FROM product_data_watermark
    UNION ALL
    SELECT * FROM user_data_watermark
    UNION ALL
    SELECT * FROM inventory_data_watermark
)

SELECT 
    source_table,
    source_schema,
    high_watermark,
    total_rows_processed,
    watermark_updated_at,
    -- Additional metadata
    '{{ run_started_at }}' AS dbt_run_timestamp,
    '{{ invocation_id }}' AS dbt_invocation_id
FROM all_watermarks
ORDER BY source_table