{% macro fx_gain_loss(txn_amount_usd, settled_amount_usd) -%}
    {#- Accounting logic: FX gain/loss is the delta between what a
        transaction was worth in USD at the moment it was authorized
        (transaction-date spot rate) and what actually landed in USD at
        settlement (settlement-date spot rate, and — for a processor like
        Adyen that settles in a currency different from the charge currency,
        e.g. GBP charged -> EUR settled -> USD reported — the processor's
        own conversion too). Positive = the merchant received more USD than
        the transaction was booked at; negative = FX erosion. Booked as its
        own ledger entry so revenue recognition never silently absorbs FX
        movement. -#}
    round(({{ settled_amount_usd }}) - ({{ txn_amount_usd }}), 2)
{%- endmacro %}
