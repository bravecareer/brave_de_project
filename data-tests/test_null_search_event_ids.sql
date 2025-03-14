WITH null_search_event_ids AS (
  SELECT
    search_event_id
  FROM {{ ref('FACT_SEARCH_EFFECTIVENESS_JC') }}
  WHERE search_event_id IS NULL
)
SELECT * FROM null_search_event_ids