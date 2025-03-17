{% macro test_expression_is_true(dim_user_sae, column_name, expression) %}
    {% set sql %}
        WITH validation AS (
            SELECT *
            FROM {{ model }}
            WHERE NOT ({{ expression }})
        )
        SELECT count(*) AS failures FROM validation
    {% endset %}
    {{ return(sql) }}
{% endmacro %}
