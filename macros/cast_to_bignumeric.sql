{% macro cast_to_bignumeric(col_name) %}
    cast(coalesce({{ col_name }}, 0) as BIGNUMERIC)
{% endmacro %}