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
    COALESCE({{ column_name }}, {{ default_value }})
{% endmacro %}
