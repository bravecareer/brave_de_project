{% macro cast_as_float(column) %}
    CAST(REPLACE({{ column }}, ',', '.') AS FLOAT)
{% endmacro %}

{% macro try_to_timestamp(column) %}
    TRY_TO_TIMESTAMP(TO_VARCHAR({{ column }}, 'YYYY-MM-DD'))
{% endmacro %}
