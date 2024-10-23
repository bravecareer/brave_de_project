{% macro convert_varchar_to_num(column_name) %}
    IFF(
        {{ column_name }} IS NULL OR TRIM({{ column_name }}) = '', 
        0, 
        TRY_CAST(REGEXP_REPLACE({{ column_name }}, '[^0-9.]', '') AS NUMBER)
    )
{% endmacro %}