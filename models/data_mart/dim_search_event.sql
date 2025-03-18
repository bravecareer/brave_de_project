-- dim_search_event.sql: Table containing recent user search events, unique on search_event_id
{{ config(
    materialized='table',
    unique_key='search_event_id'
)}}

-- Define recent search event threshold (last 5 days)
WITH date_threshold AS (
    SELECT DATEADD(day, -5, CURRENT_DATE()) AS recent_search_date
),

-- Select recent search events ensuring valid IDs
recent_search_events AS (
    SELECT
        search_event_id,     -- Unique identifier for each search event
        user_id,             -- User identifier performing the search
        search_query,        -- Text query used by the user
        search_timestamp,    -- Timestamp of the search event
        product_id,          -- ID of primary product related to search
        results_returned,    -- Number of search results returned
        clicked_product_id   -- ID of the product clicked, if any
    FROM {{ source('de_project', 'search_event_data') }}
    WHERE search_timestamp >= (SELECT recent_search_date FROM date_threshold)
      AND search_event_id IS NOT NULL   -- Exclude events without valid ID
)

-- Final explicit selection from cleaned recent_search_events CTE
SELECT
    search_event_id,
    user_id,
    search_query,
    search_timestamp,
    product_id,
    results_returned,
    clicked_product_id
FROM recent_search_events
