"""Scheduled data-quality DAG; business logic lives in pipeline.dq."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.dq.extract_profile import extract_profile
from pipeline.dq.publish_report import publish_report
from pipeline.dq.validate_profile import validate_profile


@dag(
    dag_id="data_quality",
    description="Проверяет размеченную витрину и публикует HTML-отчёт",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "data-quality"],
)
def data_quality():
    """Connect profile extraction, validation and report publication tasks."""
    profile = task(extract_profile, task_id="extract_profile")()
    checked = task(validate_profile, task_id="validate_profile")(profile)
    task(publish_report, task_id="publish_html_report")(checked)


data_quality()
