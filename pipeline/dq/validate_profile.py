"""Evaluate explicit data-quality rules against an extracted profile."""

import json
from pathlib import Path


def validate_profile(profile_info: dict) -> dict:
    """Read a JSON profile and return pass/fail results for seven rules.

    This pure transformation has no side effects. Repeated executions produce
    the same checks for the same profile snapshot.
    """
    profile = json.loads(Path(profile_info["profile_path"]).read_text(encoding="utf-8"))
    rows = int(profile["rows"])
    fraud_share = (int(profile["fraud_rows"]) / rows) if rows else 0.0
    checks = [
        {"rule": "dataset_not_empty", "passed": rows > 0, "value": rows, "expected": "> 0"},
        {"rule": "transaction_time_not_null", "passed": profile["null_time"] == 0, "value": profile["null_time"], "expected": "0"},
        {"rule": "amount_non_negative", "passed": profile["bad_amount"] == 0, "value": profile["bad_amount"], "expected": "0 invalid rows"},
        {"rule": "binary_target", "passed": profile["bad_target"] == 0, "value": profile["bad_target"], "expected": "target in {0,1}"},
        {"rule": "valid_coordinates", "passed": profile["bad_coordinates"] == 0, "value": profile["bad_coordinates"], "expected": "0 invalid rows"},
        {"rule": "categories_not_blank", "passed": profile["blank_categories"] == 0, "value": profile["blank_categories"], "expected": "0 blank rows"},
        {"rule": "both_classes_present", "passed": 0 < fraud_share < 1, "value": round(fraud_share, 6), "expected": "0 < fraud share < 1"},
    ]
    return {**profile_info, "profile": profile, "checks": checks, "passed": all(item["passed"] for item in checks)}
