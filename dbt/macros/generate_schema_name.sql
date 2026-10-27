{# Use +schema as the dataset name as is (silver, gold), not "<target>_<schema>". #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.dataset }}
{%- endmacro %}
