{% macro test_expression_is_true(model, column_name, expression) %}
    {#-
      This custom test macro runs the supplied expression against the given model.
      It returns a query that counts the number of rows where the expression is false.
      In dbt tests, a non-zero count indicates a test failure.
    -#}
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
