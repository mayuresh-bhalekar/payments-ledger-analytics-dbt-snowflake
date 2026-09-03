{% macro create_schema(relation) %}
    {#- Overrides dbt-core's default create_schema macro to a no-op.
        Schemas are provisioned once via snowflake/00_setup_database_warehouse.sql
        as part of the least-privilege access model in snowflake/01_grants.sql —
        DBT_TRANSFORMER is deliberately not granted CREATE SCHEMA, so schema
        creation is an infra-as-code step, not something a model run can do
        implicitly. Without this override, `dbt run`/`dbt build` issues a
        `create schema if not exists` for every schema it touches regardless
        of whether it already exists, which fails under that grant. -#}
{% endmacro %}
