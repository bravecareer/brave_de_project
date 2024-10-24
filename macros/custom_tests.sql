-- Macro to test if a column has only non-negative values
{% macro test_not_negative(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 0
{% endmacro %}

-- Macro to test if a column has only numeric values
{% macro test_numeric(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE TRY_CAST({{ column_name }} AS DOUBLE) IS NULL
    AND {{ column_name }} IS NOT NULL
{% endmacro %}
