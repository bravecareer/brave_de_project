{% macro extract_num_with_comma(column_name, precision) %}
    IFF(
        {{ column_name }} IS NULL OR TRIM({{ column_name }}) = '', 
        0, 
        TRY_CAST(REPLACE(REGEXP_REPLACE(TRIM({{ column_name }}), '[^0-9,]', ''), ',', '.') AS NUMBER(38, {{ precision }}))
    )
{% endmacro %}