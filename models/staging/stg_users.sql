with source as (
    select * from {{ source('raw', 'user_events') }}
)

select
    cast(user_id as string) as user_id,
    cast(product_id as string) as product_id,
    search_event_id,
    timestamp,
    has_qv,
    has_pdp,
    has_atc,
    has_purchase,
    session_id
from source
