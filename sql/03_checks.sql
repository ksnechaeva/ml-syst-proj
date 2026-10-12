SELECT count() AS loaded_rows FROM transactions.card_transactions;
SELECT database, table, is_currently_used
FROM system.projections
WHERE database = 'transactions' AND table = 'card_transactions';
