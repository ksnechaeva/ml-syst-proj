.PHONY: up produce result test down reset

up:
	docker compose up -d kafka clickhouse

produce:
	docker compose run --rm producer

result:
	./scripts/export_result.sh

test:
	./scripts/smoke_test.sh

down:
	docker compose down

reset:
	docker compose down -v
