"""Render data-quality checks as a standalone HTML report."""

import html
import json
import os
from pathlib import Path

from pipeline.settings import REPORTS_DIR


def publish_report(validation: dict) -> str:
    """Atomically write the DQ result to timestamped and latest HTML files.

    Reads validation from XCom and writes only report artifacts. Temporary-file
    replacement makes retries safe: readers see either the old or full new report.
    The task fails after publishing when any blocking rule failed.
    """
    stamp = validation["generated_at"].replace("-", "").replace(":", "").split(".")[0]
    rows = "".join(
        f"<tr><td>{html.escape(item['rule'])}</td><td>{'PASS' if item['passed'] else 'FAIL'}</td>"
        f"<td>{html.escape(str(item['value']))}</td><td>{html.escape(item['expected'])}</td></tr>"
        for item in validation["checks"]
    )
    profile_json = html.escape(json.dumps(validation["profile"], ensure_ascii=False, indent=2))
    document = f"""<!doctype html><html lang='ru'><head><meta charset='utf-8'>
<title>Data Quality report</title><style>body{{font:16px system-ui;max-width:1000px;margin:40px auto}}
table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #bbb;padding:8px;text-align:left}}
th{{background:#eee}}pre{{background:#f5f5f5;padding:16px;overflow:auto}}</style></head><body>
<h1>Data Quality: {'PASS' if validation['passed'] else 'FAIL'}</h1>
<p>Generated UTC: {html.escape(validation['generated_at'])}</p>
<table><thead><tr><th>Rule</th><th>Status</th><th>Value</th><th>Expected</th></tr></thead><tbody>{rows}</tbody></table>
<h2>Dataset profile</h2><pre>{profile_json}</pre></body></html>"""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = REPORTS_DIR / f"dq_report_{stamp}.html"
    temp = REPORTS_DIR / f".{report.name}.tmp"
    temp.write_text(document, encoding="utf-8")
    os.replace(temp, report)
    latest_temp = REPORTS_DIR / ".latest.html.tmp"
    latest_temp.write_text(document, encoding="utf-8")
    os.replace(latest_temp, REPORTS_DIR / "latest.html")
    if not validation["passed"]:
        failed = [item["rule"] for item in validation["checks"] if not item["passed"]]
        raise ValueError(f"Data Quality failed: {failed}. Report: {report}")
    return str(report)
