"""Quality gate between training and production publication."""

import json
import logging
from pathlib import Path

from pipeline.settings import MODEL_METRIC_TOLERANCE, MODELS_DIR

logger = logging.getLogger(__name__)


def _metric_is_not_worse(candidate, current) -> bool:
    """Treat an unavailable metric as neutral; otherwise compare with tolerance."""
    if candidate is None or current is None:
        return True
    return float(candidate) + MODEL_METRIC_TOLERANCE >= float(current)


def evaluate_candidate(train_info: dict) -> dict:
    """Compare candidate metrics with the currently published model.

    Reads ``latest.txt`` and its metrics JSON from the shared model registry.
    Writes nothing, so Airflow retries cannot alter production state.  The
    returned ``approved`` flag is consumed by the publication task.
    """
    result = dict(train_info)
    latest_path = MODELS_DIR / "latest.txt"
    if not latest_path.exists():
        result.update(approved=True, gate_reason="model registry is empty")
        return result

    current_version = latest_path.read_text(encoding="utf-8").strip()
    current_metrics_path = MODELS_DIR / f"my_catboost_{current_version}.metrics.json"
    if not current_metrics_path.exists():
        result.update(approved=True, gate_reason=f"baseline {current_version} has no metrics")
        return result

    current = json.loads(current_metrics_path.read_text(encoding="utf-8"))
    checks = {
        "roc_auc": _metric_is_not_worse(result.get("roc_auc"), current.get("roc_auc")),
        "f1": _metric_is_not_worse(result.get("f1"), current.get("f1")),
    }
    approved = all(checks.values())
    result.update(
        approved=approved,
        compared_with=current_version,
        previous_roc_auc=current.get("roc_auc"),
        previous_f1=current.get("f1"),
        gate_reason="all metrics are not worse" if approved else f"regression: {checks}",
    )
    logger.info("Candidate %s approved=%s: %s", result["version"], approved, result["gate_reason"])
    return result
