{% macro mask_pii(column, mask_char='*') -%}
    {#- Column-level PII masking applied at the staging boundary so nothing
        downstream ever has to remember to mask it. In production this was
        dispatched automatically off a `meta: {pii: true}` tag in each
        source's schema.yml plus a role check (unmasked only for roles in
        var('pii_unmasked_roles')); trimmed here to a direct call per column
        so the pattern is visible without the full macro-dispatch machinery. -#}
    case
        when {{ column }} is null then null
        when position('@' in {{ column }}) > 0
            then left({{ column }}, 2) || repeat('{{ mask_char }}', 5)
                || right({{ column }}, len({{ column }}) - position('@' in {{ column }}) + 1)
        else left({{ column }}, 1) || repeat('{{ mask_char }}', 5)
    end
{%- endmacro %}
