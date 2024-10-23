{% macro convert_varchar_to_date(column_name) %}
    IFF(
        {{ column_name }} IS NULL OR TRIM({{ column_name }}) = '', 
        NULL, 
        IFF(
            REGEXP_LIKE(TRIM({{ column_name }}), '^\d{4}-\d{2}-\d{2}$'), 
            TRY_CAST(TRIM({{ column_name }}) AS DATE), 
            NULL
        )
    )
{% endmacro %}