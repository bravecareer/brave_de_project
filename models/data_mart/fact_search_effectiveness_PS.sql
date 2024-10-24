{{ config(
   materialized='incremental',
   unique_key=['user_id', 'search_event_id', 'timestamp']
) }}

WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.search_terms,
       uj.search_results_count,
       uj.search_type,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.timestamp,
       uj.session_id,
       uj.search_terms_type, -- Include SEARCH_TERMS_TYPE
       uj.search_feature     -- Include SEARCH_FEATURE
   FROM {{ ref('stg_user_journey_PS') }} uj
   WHERE uj.search_event_id IS NOT NULL
),

valid_users AS (
   SELECT
       u.user_id,
       u.account_status
   FROM {{ ref('stg_user_data_PS') }} u
   WHERE u.account_status IN ('active', 'inactive', 'banned')
),

valid_products AS (
   SELECT
       p.product_id,
       p.product_name
   FROM {{ ref('stg_product_data_PS') }} p
),

final AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.search_terms,
       uj.search_results_count,
       uj.search_type,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.timestamp,
       uj.session_id,
       vp.product_name,
       uj.search_terms_type, -- Include SEARCH_TERMS_TYPE in final selection
       uj.search_feature     -- Include SEARCH_FEATURE in final selection
   FROM user_journey uj
   LEFT JOIN valid_users vu ON uj.user_id = vu.user_id
   LEFT JOIN valid_products vp ON uj.product_id = vp.product_id
)

SELECT * FROM final

{% if is_incremental() %}
    WHERE search_event_id NOT IN (SELECT search_event_id FROM {{ this }})
{% endif %}
