{# 
    Macro for safely handling numeric values, dealing with NULL and invalid values
    Parameters:
        column_name: Name of the column
        min_value: Minimum allowed value, default 0
        max_value: Maximum allowed value, default none
    Example usage:
        {{ safe_numeric('price') }}
        {{ safe_numeric('discount_percentage', min_value=0, max_value=100) }}
#}
{% macro safe_numeric(column_name, min_value=0, max_value=none) %}
    CASE 
        WHEN {{ column_name }} IS NULL THEN {{ min_value }}
        WHEN {{ column_name }} < {{ min_value }} THEN {{ min_value }}
        {% if max_value is not none %}
        WHEN {{ column_name }} > {{ max_value }} THEN {{ max_value }}
        {% endif %}
        ELSE {{ column_name }}
    END
{% endmacro %}

{# 
    Macro for handling default values
    Parameters:
        column_name: Name of the column
        default_value: Default value to use when NULL
    Example usage:
        {{ default_value('user_id', "'UNKNOWN'") }}
        {{ default_value('search_results_count', 0) }}
#}
{% macro default_value(column_name, default_value) %}
    COALESCE({{ column_name }}, {{ default_value }}) as {{ column_name }}
{% endmacro %}
