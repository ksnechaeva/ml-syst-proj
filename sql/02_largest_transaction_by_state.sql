SELECT
    toString(state) AS state,
    argMax(category, tuple(amt, unix_time, trans_num)) AS category,
    max(amt) AS max_amount
FROM transactions.card_transactions
GROUP BY state
ORDER BY state
FORMAT CSVWithNames;
