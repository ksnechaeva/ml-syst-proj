#!/usr/bin/env sh
set -eu

expected_rows="${EXPECTED_ROWS:-5}"
attempt=0
while [ "$attempt" -lt 30 ]; do
  actual_rows="$(docker compose exec -T clickhouse clickhouse-client --query \
    'SELECT count() FROM transactions.card_transactions')"
  if [ "$actual_rows" -ge "$expected_rows" ]; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 2
done

if [ "$actual_rows" -lt "$expected_rows" ]; then
  echo "Expected at least $expected_rows rows, got $actual_rows" >&2
  exit 1
fi

./scripts/export_result.sh
states="$(tail -n +2 results/largest_transaction_category_by_state.csv | wc -l | tr -d ' ')"
test "$states" -eq 3
echo "Smoke test passed: $actual_rows rows, $states states"
