-- A dispute can never claim more than the transaction it's disputing was
-- worth — a violation almost always means the dispute was joined to the
-- wrong transaction_id upstream.

-- dispute_amount is booked in the transaction's native currency, same as
-- gross_amount_usd is for the USD-only disputes in this seed set; a
-- multi-currency version would convert both sides to USD before comparing.
select
    d.dispute_id,
    d.transaction_id,
    d.dispute_amount,
    t.gross_amount_usd as transaction_gross_amount
from {{ ref('fct_disputes') }} d
inner join {{ ref('fct_transactions') }} t on t.transaction_id = d.transaction_id
where d.dispute_amount > abs(t.gross_amount_usd) + 0.01
