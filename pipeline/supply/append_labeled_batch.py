"""Дозапись подготовленного месячного батча в размеченную витрину."""

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline.db import copy_labeled_frame, get_conn, month_end_exclusive

logger = logging.getLogger(__name__)


def append_labeled_batch(batch_info: dict) -> dict:
    """Атомарно заменяет целевой месяц строками подготовленного батча.

    Читает путь и месяц из batch_info. В одной транзакции удаляет уже записанный
    целевой месяц и загружает parquet заново. Поэтому retry после сбоя не создаёт
    дублей и не оставляет частично записанный месяц. Возвращает метаданные батча.
    """
    batch_path = Path(batch_info["batch_path"])
    if not batch_path.exists():
        raise FileNotFoundError(f"Нет файла батча для загрузки: {batch_path}")

    frame = pd.read_parquet(batch_path)
    batch_month = date.fromisoformat(batch_info["batch_month"])
    batch_end = month_end_exclusive(batch_month)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM labeled_transactions WHERE transaction_time >= %s AND transaction_time < %s",
                (batch_month, batch_end),
            )
        copy_labeled_frame(conn, frame, commit=False)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info(
        "Месяц %s добавлен в витрину, строк: %s",
        batch_info["batch_month"],
        len(frame),
    )
    return {
        "batch_path": batch_info["batch_path"],
        "batch_month": batch_info["batch_month"],
        "rows": int(len(frame)),
    }
