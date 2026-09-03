{% macro cents_to_amount(cents_column) -%}
    {#- All money is stored in integer minor units (cents) to avoid float
        rounding drift through the pipeline; only convert to a decimal major
        unit at the mart/reporting edge. -#}
    round({{ cents_column }} / 100.0, 2)
{%- endmacro %}
