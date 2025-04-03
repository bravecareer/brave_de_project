-- tests/test_invalid_sessions.sql

select *
from {{ ref('fact_user_engagement') }}
where session_id is null
