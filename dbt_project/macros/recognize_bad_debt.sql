{% macro recognize_bad_debt(revenue_category_column, net_amount_usd_column) -%}
    {#- The write-off amount: only populated on rows categorize_revenue()
        flagged as 'bad_debt_writeoff' (a chargeback lost after the merchant
        was already paid out), zero everywhere else. Summed at the mart
        layer to feed the bad-debt allowance line on the P&L, per
        var('bad_debt_recognition_lag_days') in dbt_project.yml controlling
        how promptly a lost dispute is swept into the current period vs.
        held for the next close. -#}
    case
        when {{ revenue_category_column }} = 'bad_debt_writeoff'
            then abs({{ net_amount_usd_column }})
        else 0
    end
{%- endmacro %}
