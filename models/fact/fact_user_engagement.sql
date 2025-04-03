select
    user_id,
    product_id,
    search_event_id,
    timestamp,
    has_qv,
    has_pdp,
    has_atc,
    has_purchase,
    session_id
from {{ ref('stg_users') }}
