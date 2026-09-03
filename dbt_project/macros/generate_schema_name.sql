{% macro generate_schema_name(custom_schema_name, node) -%}

    {#- In dev, prefix with the target schema (personal sandbox) so multiple
        developers don't collide. In prod/ci/airflow, use the custom schema
        exactly as configured in dbt_project.yml (e.g. MARTS_LEDGER, not
        PROD_marts_ledger) — those three targets all point at schemas
        snowflake/00_setup_database_warehouse.sql actually created, and
        DBT_TRANSFORMER has no CREATE SCHEMA grant (see macros/create_schema.sql),
        so a prefixed name here isn't just wrong, it's unrunnable. -#}

    {%- set default_schema = target.schema -%}

    {%- if target.name in ('prod', 'ci', 'airflow') -%}
        {{ custom_schema_name | trim }}
    {%- elif custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
