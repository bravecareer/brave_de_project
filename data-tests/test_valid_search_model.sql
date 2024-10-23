select search_model
from {{ ref('fact_search_effectiveness_michael_w') }}
where search_model not in ('Model A', 'Model B', 'Model C', 'Model D', 'Model E', 'Model F')