"""Extract a reproducible profile of the labeled transaction showcase."""

import json
from datetime import datetime, timezone

from pipeline.db import get_conn
from pipeline.settings import REPORTS_DIR


def extract_profile() -> dict:
    """Read aggregate statistics from Postgres and write a JSON snapshot.

    Only SELECT queries are executed. The timestamped snapshot is immutable,
    therefore retrying the task cannot damage the source dataset.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) AS rows,
                       count(*) FILTER (WHERE transaction_time IS NULL) AS null_time,
                       count(*) FILTER (WHERE amount IS NULL OR amount < 0) AS bad_amount,
                       count(*) FILTER (WHERE target IS NULL OR target NOT IN (0, 1)) AS bad_target,
                       count(*) FILTER (WHERE lat IS NULL OR lon IS NULL OR merchant_lat IS NULL OR merchant_lon IS NULL
                                           OR lat NOT BETWEEN -90 AND 90 OR lon NOT BETWEEN -180 AND 180
                                           OR merchant_lat NOT BETWEEN -90 AND 90 OR merchant_lon NOT BETWEEN -180 AND 180) AS bad_coordinates,
                       count(*) FILTER (WHERE merch IS NULL OR btrim(merch) = '' OR cat_id IS NULL OR btrim(cat_id) = '') AS blank_categories,
                       count(DISTINCT date_trunc('month', transaction_time)) AS months,
                       min(transaction_time), max(transaction_time),
                       count(*) FILTER (WHERE target = 1) AS fraud_rows
                FROM labeled_transactions
                """
            )
            row = cur.fetchone()
    finally:
        conn.close()

    generated_at = datetime.now(timezone.utc)
    profile = dict(zip(
        ["rows", "null_time", "bad_amount", "bad_target", "bad_coordinates",
         "blank_categories", "months", "min_time", "max_time", "fraud_rows"], row
    ))
    for key in ("min_time", "max_time"):
        profile[key] = profile[key].isoformat() if profile[key] else None
    snapshot = REPORTS_DIR / f"profile_{generated_at:%Y%m%dT%H%M%SZ}.json"
    snapshot.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"profile_path": str(snapshot), "generated_at": generated_at.isoformat()}
