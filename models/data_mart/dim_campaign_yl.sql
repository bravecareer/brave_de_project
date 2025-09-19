{{ config(
    materialized='incremental',
    unique_key='campaign_sk',
    incremental_strategy='merge'
) }}

with sj as (
  select
    mkt_campaign,
    mkt_source,
    mkt_medium,
    mkt_content,
    event_ts
  from {{ ref('stg_user_journey_yl') }}
  {% if is_incremental() %}
    where TO_DATE(collector_ts) >= CURRENT_DATE() - 2
  {% endif %}
),

agg as (
  select
    mkt_campaign,
    mkt_source,
    mkt_medium,
    mkt_content,
    max(event_ts) as last_seen_at
  from sj
  group by 1,2,3,4
)

select
    {{ dbt_utils.generate_surrogate_key([
      'mkt_campaign',
      'mkt_source',
      'mkt_medium',
      'mkt_content'
    ]) }} as campaign_sk,
  mkt_campaign,
  mkt_source,
  mkt_medium,
  mkt_content,
  last_seen_at,
  '{{ run_started_at }}'::timestamp_tz as dbt_processed_at
from agg
