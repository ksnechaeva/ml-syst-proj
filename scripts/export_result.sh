#!/usr/bin/env sh
set -eu

mkdir -p results
docker compose exec -T clickhouse clickhouse-client \
  --queries-file /dev/stdin < sql/02_largest_transaction_by_state.sql \
  > results/largest_transaction_category_by_state.csv
echo "Wrote results/largest_transaction_category_by_state.csv"
