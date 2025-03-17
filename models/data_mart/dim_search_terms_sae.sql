{{ config(materialized='table', schema='PROJECT_TEST', cluster_by=['search_term_id']) }}

WITH search_terms AS (
    SELECT 
        LOWER(TRIM(SEARCH_TERMS)) AS search_term,
        COUNT(*) AS total_searches,
        AVG(SEARCH_RESULTS_COUNT) AS avg_results_count,
        SUM(CASE WHEN HAS_ATC THEN 1 ELSE 0 END) AS atc_count,
        SUM(CASE WHEN HAS_PURCHASE THEN 1 ELSE 0 END) AS purchase_count
    FROM {{ ref('stg_user_journey_sae') }}
    WHERE SEARCH_TERMS IS NOT NULL 
      AND TRIM(SEARCH_TERMS) <> ''
    GROUP BY LOWER(TRIM(SEARCH_TERMS))
),

categorized_terms AS (
    SELECT 
        search_term,
        total_searches AS search_popularity,
        avg_results_count,
        atc_count::FLOAT / NULLIF(total_searches, 0) AS atc_rate,
        purchase_count::FLOAT / NULLIF(total_searches, 0) AS purchase_rate,
        CASE 
            WHEN purchase_count > 0 THEN 'Buying'
            WHEN atc_count > 0 THEN 'Considering'
            WHEN search_term ILIKE '%explore%' THEN 'Browsing'
            ELSE 'Other'
        END AS intent_category
    FROM search_terms
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY search_term) AS search_term_id,
    search_term,
    search_popularity,
    ROUND(avg_results_count, 2) AS avg_results_count,
    ROUND(atc_rate, 4) AS atc_rate,
    ROUND(purchase_rate, 4) AS purchase_rate,
    intent_category
FROM categorized_terms
