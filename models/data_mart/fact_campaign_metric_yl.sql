-- Grain: one row per (campaign_sk, session_id) using LAST-TOUCH attribution
{{ config(
    materialized='incremental',
    unique_key=['campaign_sk','session_id'],
    incremental_strategy='merge'
) }}

-- Pull everything once and derive both metrics + campaign key
with events as (
  select
    session_id,
    user_id,
    event_ts,
    product_id,
    iff(has_atc,1,0)      as atc,
    iff(has_purchase,1,0) as purchase,
    {{ dbt_utils.generate_surrogate_key([
      'mkt_campaign',
      'mkt_source',
      'mkt_medium',
      'mkt_content'
    ]) }} as campaign_sk
  from {{ ref('stg_user_journey_yl') }}
  {% if is_incremental() %}
    where date(collector_ts) >= CURRENT_DATE() - 2
  {% endif %}
),

events_priced as (
  select
    e.*,
    coalesce(p.price, 0) as unit_price
  from events e
  left join {{ ref('dim_product_yl') }} p
    on e.product_id = p.product_id
),

-- Compute session-level metrics via window functions, then pick ONE campaign per session (last-touch).
bridge_with_metrics as (
  select
    session_id,
    user_id,
    campaign_sk,
    'last_touch'::varchar as attribution_model,
    event_ts              as attributed_ts,
    -- session-level metrics across ALL events in the session
    max(atc)      over (partition by session_id) as atc_session,
    max(purchase) over (partition by session_id) as purchase_session,
    sum(case when purchase = 1 then unit_price else 0 end)
        over (partition by session_id)           as revenue_session
  from events_priced
  qualify row_number() over (
    partition by session_id
    order by event_ts desc
  ) = 1
)

select
  campaign_sk,
  session_id,          
  user_id,
  attribution_model,
  attributed_ts,
  cast(attributed_ts as date) as attributed_date,
  atc_session       as atc,
  purchase_session  as purchase,
  revenue_session   as revenue,
  '{{ run_started_at }}'::timestamp_tz as dbt_processed_at
from bridge_with_metrics
