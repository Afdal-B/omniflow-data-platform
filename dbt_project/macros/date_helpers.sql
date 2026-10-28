{% macro iso_day_of_week(date_expr) -%}
    {{ return(adapter.dispatch('iso_day_of_week')(date_expr)) }}
{%- endmacro %}

{% macro default__iso_day_of_week(date_expr) -%}
    cast(extract(isodow from {{ date_expr }}) as integer)
{%- endmacro %}

{% macro snowflake__iso_day_of_week(date_expr) -%}
    dayofweekiso({{ date_expr }})
{%- endmacro %}

{% macro day_name(date_expr) -%}
    {{ return(adapter.dispatch('day_name')(date_expr)) }}
{%- endmacro %}

{% macro default__day_name(date_expr) -%}
    to_char({{ date_expr }}, 'Dy')
{%- endmacro %}

{% macro snowflake__day_name(date_expr) -%}
    dayname({{ date_expr }})
{%- endmacro %}

{% macro month_name(date_expr) -%}
    {{ return(adapter.dispatch('month_name')(date_expr)) }}
{%- endmacro %}

{% macro default__month_name(date_expr) -%}
    to_char({{ date_expr }}, 'Mon')
{%- endmacro %}

{% macro snowflake__month_name(date_expr) -%}
    monthname({{ date_expr }})
{%- endmacro %}
