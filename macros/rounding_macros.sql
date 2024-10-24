{% macro round_to_decimal(column, decimal_places) %}
    CAST(ROUND(CAST(REPLACE({{ column }}, ',', '.') AS FLOAT), {{ decimal_places }}) AS FLOAT)
{% endmacro %}
