{% macro cast_columns(column_casts) %}
    {% for column, cast_type in column_casts.items() %}
        CAST({{ column }} AS {{ cast_type }}) AS {{ column }},
    {% endfor %}
{% endmacro %}
