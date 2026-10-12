# Kafka → ClickHouse: транзакции по штатам

Учебный ETL-проект: Python читает CSV построчно и публикует транзакции в
Kafka; ClickHouse забирает сообщения через таблицу `ENGINE = Kafka`, а
materialized view сохраняет их в оптимизированную `MergeTree`-таблицу. Итоговый
запрос возвращает категорию самой крупной транзакции в каждом штате.

## Что находится в репозитории

- `producer.py` — отдельный потоковый CSV → Kafka producer;
- `sql/01_tables.sql` — DDL Kafka-таблицы, materialized view и постоянной таблицы;
- `sql/02_largest_transaction_by_state.sql` — аналитический запрос;
- `results/largest_transaction_category_by_state.csv` — результат по полному
  набору `train.csv` + `test.csv` (1 048 575 транзакций, 51 штат/территория);
- `docker-compose.yml` — Kafka (KRaft, без ZooKeeper), ClickHouse и producer;
- `data/sample_transactions.csv` — минимальный набор для smoke test.

## Быстрый запуск

Нужны Docker с Compose v2 и примерно 3 ГБ свободной памяти.

```bash
cp .env.example .env
docker compose up -d kafka clickhouse
docker compose run --rm producer
./scripts/smoke_test.sh
```

Открыть результат:

```bash
cat results/largest_transaction_category_by_state.csv
```

Проверить число строк вручную:

```bash
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count() FROM transactions.card_transactions"
```

Остановить контейнеры: `docker compose down`. Полностью удалить данные и начать
заново: `docker compose down -v`.

## Запуск с Kaggle CSV

Скачайте файлы по выданной в задании приватной ссылке (Kaggle требует войти и
принять приглашение). Для конкретного набора из задания загрузите оба файла:

```bash
CSV_FILE=./data/train.csv docker compose run --rm producer
CSV_FILE=./data/test.csv docker compose run --rm producer
./scripts/export_result.sh
```

`train.csv` содержит 786 431 строку, `test.csv` — 262 144. В `test.csv` нет
целевой колонки `target`; producer подставляет `is_fraud = 0`, поскольку для
аналитического запроса эта колонка не используется.

Producer не загружает файл целиком в память. Он понимает как исходные названия
этого соревнования (`transaction_time`, `merch`, `cat_id`, `amount`, `us_state`,
`target`), так и распространённую схему (`trans_date_trans_time`, `merchant`,
`category`, `amt`, `state`, `is_fraud`). Для первой схемы он сам вычисляет
`unix_time` и стабильный идентификатор строки. Регистр заголовков не важен,
лишний индексный столбец игнорируется. Повторный запуск повторно отправит все
строки; для чистого учебного прогона сначала выполните `make reset && make up`.

## Как движутся данные

1. Producer преобразует строку CSV в JSON и отправляет её в топик
   `transactions`. Ключом служит штат, поэтому сообщения одного штата попадают в
   одну Kafka partition.
2. `transactions.transactions_kafka` читает JSON как поток. Данные в таблице
   Kafka не хранятся.
3. Materialized view автоматически вставляет сообщения в
   `transactions.card_transactions`.
4. Запрос использует `argMax(category, tuple(amt, unix_time, trans_num))`.
   `argMax` возвращает категорию той строки, у которой максимален второй
   аргумент. `unix_time` и `trans_num` дают детерминированный выбор при равных
   суммах.

## Оптимизация ClickHouse

Постоянная таблица использует:

- `LowCardinality` для повторяющихся `state`, `category`, `merchant` и других
  строк — словарное кодирование уменьшает объём;
- узкие числовые типы (`UInt8`, `UInt32`) и `Decimal(12,2)` для денег;
- `ZSTD`/`Delta` codecs для дополнительного сжатия;
- месячные partitions и `ORDER BY (state, category, amt, ...)`;
- aggregate projection `max_transaction_by_state`, которая хранит заранее
  агрегированный результат по штатам. ClickHouse может автоматически подменить
  полный scan проекции при совпадающем запросе.

После добавления projection к уже заполненной таблице её нужно построить:

```sql
ALTER TABLE transactions.card_transactions
MATERIALIZE PROJECTION max_transaction_by_state;
```

В этом проекте projection создаётся до загрузки, поэтому новые части строятся
сразу. Проверить план и использование projection можно так:

```bash
docker compose exec clickhouse clickhouse-client --query \
  "EXPLAIN indexes=1, optimize=1 SELECT state, argMax(category, tuple(amt, unix_time, trans_num)), max(amt) FROM transactions.card_transactions GROUP BY state"
```

В плане должна появиться строка
`ReadFromMergeTree (max_transaction_by_state)` — это подтверждает, что читается
агрегированная projection, а не все исходные строки.

## Важные замечания

- CSV с персональными данными исключены из Git через `.gitignore`; sample
  синтетический.
- Kafka даёт доставку как минимум один раз. Для production-защиты от дублей
  можно использовать `ReplacingMergeTree` с версией и уникальным `trans_num`.
- `state` здесь — штат владельца карты, поскольку именно так поле определено в
  исходном датасете. Если преподаватель подразумевает штат магазина, понадобится
  другое поле источника.
