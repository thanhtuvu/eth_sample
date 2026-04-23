{% macro show_env() %}
    {{ log("target.name: " ~ target.name, info=True) }}
    {{ log("target.schema: " ~ target.schema, info=True) }}
    {{ log("target.database: " ~ target.database, info=True) }}
{% endmacro %}

-- run: dbt run-operation show_env
-- or run: select 
--   '{{ target.name }}' as env,
--   '{{ target.schema }}' as dataset,
--   '{{ target.database }}' as project