{{ config(
    materialized='incremental',
    unique_key='journey_event_sk',
    incremental_strategy='merge'
) }}

with src as (
  select *
  from {{ source('de_project','user_journey') }}
  {% if is_incremental() %}
    where date(collector_tstamp) >= current_date() - 2
  {% endif %}
),

parsed as (
  select
    timestamp        ::timestamp_ntz as raw_event_ts,
    collector_tstamp ::timestamp_ntz as raw_collector_ts,
    *
  from src
),

clean as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'session_id','search_event_id','product_id',
      "to_varchar(coalesce(raw_event_ts, raw_collector_ts))"
    ]) }} as journey_event_sk,

    -- enforce ordering but keep the final names
    case
      when raw_event_ts is null then raw_collector_ts
      when raw_collector_ts is null then raw_event_ts
      else least(raw_event_ts, raw_collector_ts)
    end as event_ts,

    case
      when raw_event_ts is null then raw_collector_ts
      when raw_collector_ts is null then raw_event_ts
      else greatest(raw_event_ts, raw_collector_ts)
    end as collector_ts,

    coalesce(nullif(lower(trim(mkt_campaign)), ''), '(none)') as mkt_campaign,
    coalesce(nullif(lower(trim(mkt_source)),   ''), '(none)') as mkt_source,
    coalesce(nullif(lower(trim(mkt_medium)),   ''), '(none)') as mkt_medium,
    coalesce(nullif(lower(trim(mkt_content)),  ''), '(none)') as mkt_content,

    nullif(trim(user_id), '')    as user_id,
    nullif(trim(session_id), '') as session_id,
    cart_id,
    search_event_id,
    product_id,
    try_to_number(search_results_count) as search_results_count,
    search_terms,
    search_type,
    has_qv,
    has_pdp,
    has_atc,
    has_purchase,

    '{{ run_started_at }}'::timestamp_tz          as dbt_processed_at
  from parsed
)

select * from clean







    

