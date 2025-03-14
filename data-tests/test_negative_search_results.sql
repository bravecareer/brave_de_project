WITH negative_search_results AS (
  SELECT
    search_event_id,
    search_results_count
  FROM {{ ref('FACT_SEARCH_EFFECTIVENESS_JC') }}
  WHERE search_results_count < 0
)
SELECT * FROM negative_search_results