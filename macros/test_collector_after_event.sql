{% test collector_after_event(model, column_name, event_col) %}
-- column_name is the column under test (collector_ts)
select *
from {{ model }}
where {{ column_name }} < {{ event_col }}
{% endtest %}

