{% macro sql_list(values) -%}
    {%- for value in values -%}
        '{{ value }}'{%- if not loop.last %}, {% endif -%}
    {%- endfor -%}
{%- endmacro %}
