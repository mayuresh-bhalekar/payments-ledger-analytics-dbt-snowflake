{% macro categorize_revenue(transaction_type_column, dispute_status_column, settlement_status_column) -%}
    {#- Encodes the revenue-recognition policy in one place instead of every
        mart re-deriving it: a captured charge is gross revenue, a refund
        reverses it, a lost dispute on an already-settled charge is a
        bad-debt write-off (not a plain refund — the payout already left the
        ledger, see recognize_bad_debt.sql), and an open dispute is held
        pending, not yet recognized either way. -#}
    case
        when {{ dispute_status_column }} = 'lost' and {{ settlement_status_column }} = 'settled'
            then 'bad_debt_writeoff'
        when {{ dispute_status_column }} = 'open'
            then 'pending_dispute_hold'
        when {{ transaction_type_column }} = 'refund'
            then 'revenue_reversal'
        when {{ transaction_type_column }} = 'charge' and {{ settlement_status_column }} = 'settled'
            then 'gross_revenue'
        else 'unrecognized'
    end
{%- endmacro %}
