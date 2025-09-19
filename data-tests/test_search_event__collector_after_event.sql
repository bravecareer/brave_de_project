select *
from {{ ref('dim_search_event_yl') }}
where collector_ts < event_ts