# Kafka → ClickHouse: предыдущее домашнее задание

Это моё предыдущее задание. Я оставила его в этом же репозитории, но вынесла
старый Docker Compose в файл `docker-compose.hw2.yml`, чтобы оно не конфликтовало
с новым проектом на Airflow.

В этой версии Python читает CSV построчно и публикует транзакции в Kafka.
ClickHouse читает сообщения через таблицу с `ENGINE = Kafka`, а materialized view
переносит их в постоянную `MergeTree`-таблицу. Итоговый запрос находит категорию
самой крупной транзакции в каждом штате.

## Файлы задания

- `producer.py` — потоковый CSV → Kafka producer;
- `sql/01_tables.sql` — таблицы Kafka, materialized view и постоянная таблица;
- `sql/02_largest_transaction_by_state.sql` — итоговый запрос;
- `results/largest_transaction_category_by_state.csv` — сохранённый результат;
- `data/sample_transactions.csv` — маленький набор для быстрой проверки;
- `docker-compose.hw2.yml` — Kafka, ClickHouse и producer.

## Быстрый запуск

Если новое задание уже запущено, сначала его нужно остановить обычной командой
`docker compose down`. Тома удалять не обязательно.

Для команд прошлого задания я задаю отдельный Compose-файл и отдельное имя
проекта:

```bash
export COMPOSE_FILE=docker-compose.hw2.yml
export COMPOSE_PROJECT_NAME=ml-syst-proj-hw2

docker compose up -d kafka clickhouse
docker compose run --rm producer
./scripts/smoke_test.sh
```

Результат можно открыть так:

```bash
cat results/largest_transaction_category_by_state.csv
```

Проверка количества загруженных строк:

```bash
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count() FROM transactions.card_transactions"
```

Остановить именно это задание:

```bash
docker compose down
unset COMPOSE_FILE COMPOSE_PROJECT_NAME
```

Для полного удаления данных прошлого задания вместо `docker compose down` можно
выполнить `docker compose down -v`.

## Запуск со своими CSV

```bash
CSV_FILE=./data/train.csv docker compose run --rm producer
CSV_FILE=./data/test.csv docker compose run --rm producer
./scripts/export_result.sh
```

Producer не загружает весь файл в память. Он понимает как названия колонок из
выданного датасета (`transaction_time`, `merch`, `cat_id`, `amount`, `us_state`,
`target`), так и распространённую схему (`trans_date_trans_time`, `merchant`,
`category`, `amt`, `state`, `is_fraud`). Лишний индексный столбец игнорируется.

## Как идут данные

1. Producer преобразует строку CSV в JSON и отправляет её в топик `transactions`.
2. Kafka-таблица ClickHouse читает сообщения как поток и сама их не хранит.
3. Materialized view записывает сообщения в `transactions.card_transactions`.
4. Запрос использует `argMax(category, tuple(amt, unix_time, trans_num))`, чтобы
   детерминированно выбрать категорию максимальной транзакции.

В постоянной таблице используются `LowCardinality`, узкие числовые типы, codecs,
месячные partitions и aggregate projection `max_transaction_by_state`.

Проверить использование projection:

```bash
docker compose exec clickhouse clickhouse-client --query \
  "EXPLAIN indexes=1, optimize=1 SELECT state, argMax(category, tuple(amt, unix_time, trans_num)), max(amt) FROM transactions.card_transactions GROUP BY state"
```

В плане должна быть строка `ReadFromMergeTree (max_transaction_by_state)`.
