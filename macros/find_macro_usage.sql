
-- macros/find_macro_usage.sql
{% macro find_macro_usage(macro_name) %}

{% set ns = namespace(found=false) %}

{% for node_id, node in graph.nodes.items() %}
    {% for dep in node.depends_on.macros %}
        {% if macro_name in dep %}
            {{ log("  → " ~ node.original_file_path, info=True) }}
            {% set ns.found = true %}
        {% endif %}
    {% endfor %}
{% endfor %}

{% if not ns.found %}
    {{ log("  → not used in any model", info=True) }}
{% endif %}

{% endmacro %}