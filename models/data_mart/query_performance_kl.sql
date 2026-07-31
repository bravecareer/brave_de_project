/*
****************************************************************************************************
MODEL: query_performance
PURPOSE: 
    - Track query execution metrics for cost optimization
    - Identify slow queries and performance bottlenecks
    - Monitor warehouse usage and compute costs
GRAIN: One row per query execution
USAGE:
    - Performance optimization
    - Identifying queries to add incremental materialization
****************************************************************************************************
*/

{{ config(
    materialized='incremental',
    unique_key='query_id',
    schema='pipeline_metadata',
    tags=['infrastructure', 'monitoring', 'cost_analysis']
) }}

SELECT 
    -- Query Identification
    query_id,
    query_text,
    query_type,
    
    -- Database Context
    database_name,
    schema_name,
    
    -- User Context
    user_name,
    role_name,
    
    -- Warehouse
    warehouse_name,
    warehouse_size,
    
    -- Execution Metrics
    execution_status,
    error_code,
    error_message,
    
    -- Performance Metrics
    execution_time / 1000 AS execution_seconds,
    compilation_time / 1000 AS compilation_seconds,
    queued_provisioning_time / 1000 AS queue_seconds,
    
    -- Data Volume
    bytes_scanned / (1024*1024*1024) AS gb_scanned,
    bytes_written / (1024*1024*1024) AS gb_written,
    bytes_spilled_to_local_storage / (1024*1024*1024) AS gb_spilled_local,
    bytes_spilled_to_remote_storage / (1024*1024*1024) AS gb_spilled_remote,
    
    -- Partitioning
    partitions_scanned,
    partitions_total,
    
    -- Cost (approximate)
    credits_used_cloud_services,
    
    -- Timestamps
    start_time,
    end_time,
    
    -- Audit
    CURRENT_TIMESTAMP() AS loaded_at
    
FROM snowflake.account_usage.query_history

WHERE 1=1
    -- Filter to relevant queries only
    AND (
        query_text ILIKE '%brave_database%'  -- Our database
        OR query_text ILIKE '%de_project%'   -- Our schemas
        OR query_text ILIKE '%project_test%'
    )
    -- Exclude system queries
    AND query_type != 'SHOW'
    AND user_name != 'SYSTEM'
    
{% if is_incremental() %}
    -- Only process new queries
    AND start_time > (SELECT MAX(start_time) FROM {{ this }})
{% else %}
    -- Initial load: last 7 days
    AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
{% endif %}

ORDER BY start_time DESC