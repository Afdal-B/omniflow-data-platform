{% macro unnest_json_object(relation_alias, json_column) -%}
    {{ return(adapter.dispatch('unnest_json_object')(relation_alias, json_column)) }}
{%- endmacro %}

{% macro default__unnest_json_object(relation_alias, json_column) -%}
    cross join lateral jsonb_each_text({{ relation_alias }}.{{ json_column }}) as kv(json_key, json_value)
{%- endmacro %}

{% macro snowflake__unnest_json_object(relation_alias, json_column) -%}
    , lateral (
        select key as json_key, value::string as json_value
        from table(flatten(input => {{ relation_alias }}.{{ json_column }}))
    ) as kv
{%- endmacro %}
