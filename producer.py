#!/usr/bin/env python3
"""Stream a credit-card transaction CSV to Kafka as JSONEachRow messages."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


FIELDS = (
    "trans_date_trans_time", "cc_num", "merchant", "category", "amt",
    "first", "last", "gender", "street", "city", "state", "zip",
    "lat", "long", "city_pop", "job", "dob", "trans_num", "unix_time",
    "merch_lat", "merch_long", "is_fraud",
)
ALIASES = {
    "amount": "amt",
    "cat_id": "category",
    "jobs": "job",
    "lon": "long",
    "merch": "merchant",
    "merchant_lat": "merch_lat",
    "merchant_lon": "merch_long",
    "name_1": "first",
    "name_2": "last",
    "one_city": "city",
    "population_city": "city_pop",
    "post_code": "zip",
    "target": "is_fraud",
    "trans_date_trans_time": "trans_date_trans_time",
    "transaction_time": "trans_date_trans_time",
    "transaction_date_time": "trans_date_trans_time",
    "us_state": "state",
}
NUMERIC_DEFAULTS = {
    "amt": 0.0, "zip": 0, "lat": 0.0, "long": 0.0, "city_pop": 0,
    "unix_time": 0, "merch_lat": 0.0, "merch_long": 0.0, "is_fraud": 0,
}


def normalize_header(name: str) -> str:
    normalized = name.strip().lower().replace(" ", "_")
    return ALIASES.get(normalized, normalized)


def normalize_row(row: dict[str, str], source_id: str = "row") -> dict[str, object]:
    source = {normalize_header(k): (v or "").strip() for k, v in row.items() if k}
    missing = [name for name in ("state", "category", "amt") if not source.get(name)]
    if missing:
        raise ValueError(f"required values are empty: {', '.join(missing)}")

    if not source.get("trans_num"):
        source["trans_num"] = source_id
    if not source.get("unix_time") and source.get("trans_date_trans_time"):
        try:
            timestamp = datetime.fromisoformat(source["trans_date_trans_time"])
            source["unix_time"] = str(int(timestamp.replace(tzinfo=timezone.utc).timestamp()))
        except ValueError as exc:
            raise ValueError(
                f"invalid transaction timestamp {source['trans_date_trans_time']!r}"
            ) from exc

    result: dict[str, object] = {}
    for field in FIELDS:
        value = source.get(field, "")
        if field in NUMERIC_DEFAULTS:
            default = NUMERIC_DEFAULTS[field]
            if value == "":
                result[field] = default
                continue
            try:
                result[field] = int(float(value)) if isinstance(default, int) else float(value)
            except ValueError as exc:
                raise ValueError(f"invalid number {field}={value!r}") from exc
        else:
            result[field] = value
    return result


def ensure_topic(bootstrap: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
    try:
        futures[topic].result()
        print(f"Created Kafka topic {topic!r}")
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default=os.getenv("CSV_FILE", "data/sample_transactions.csv"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.is_file():
        parser.error(f"CSV file does not exist: {csv_path}")

    for attempt in range(30):
        try:
            ensure_topic(args.bootstrap_servers, args.topic)
            break
        except Exception:
            if attempt == 29:
                raise
            time.sleep(2)

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "csv-transaction-producer",
        "enable.idempotence": True,
        "compression.type": "snappy",
    })
    sent = 0
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise ValueError("CSV has no header")
            normalized = {normalize_header(x) for x in reader.fieldnames}
            missing = {"state", "category", "amt"} - normalized
            if missing:
                raise ValueError(f"CSV misses required columns: {', '.join(sorted(missing))}")

            for line_number, row in enumerate(reader, start=2):
                try:
                    event = normalize_row(row, f"{csv_path.name}:{line_number}")
                except ValueError as exc:
                    raise ValueError(f"line {line_number}: {exc}") from exc
                producer.produce(
                    args.topic,
                    key=str(event["state"]).encode(),
                    value=json.dumps(event, ensure_ascii=False).encode(),
                    on_delivery=lambda error, _message: print(error, file=sys.stderr) if error else None,
                )
                producer.poll(0)
                sent += 1
                if sent % 100_000 == 0:
                    print(f"Queued {sent:,} rows")
    finally:
        remaining = producer.flush(30)

    if remaining:
        raise RuntimeError(f"Kafka delivery timed out for {remaining} messages")
    print(f"Published {sent:,} rows from {csv_path} to {args.topic!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
