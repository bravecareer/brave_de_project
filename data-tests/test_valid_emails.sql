with invalid_emails as (
  select
    user_id,
    email
  from {{ ref('dim_user_data_gs') }}
  where email not like '%_@__%.__%'
)
select * from invalid_emails
