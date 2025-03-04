/*
    Test Name: Search Metrics Consistency Test
    Description: Validates that search metrics follow logical rules:
    - All search events have valid search request IDs
    - All search events have non-negative search results counts
    
    Note: This test focuses on data quality for the search metrics fact table
    after the restructuring to a more normalized model.
*/

WITH search_metrics_check AS (
    SELECT 
        search_event_id,
        search_request_id,
        search_results_count,
        CASE 
            WHEN search_request_id IS NULL OR search_request_id = 'UNKNOWN' THEN 'Search request ID is missing or unknown'
            WHEN search_results_count < 0 THEN 'Search results count cannot be negative'
            ELSE NULL
        END as validation_error
    FROM {{ ref('fact_search_metrics_tf') }}
    WHERE search_request_id IS NULL 
       OR search_request_id = 'UNKNOWN'
       OR search_results_count < 0
)

-- Combine all results
SELECT * FROM search_metrics_check
WHERE validation_error IS NOT NULL
