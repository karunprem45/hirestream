"""Airflow DAG: dedupe raw postings with Spark, load to BigQuery, then check
the load. Runs every 6 hours.

Set PROJECT_DIR to where this repo lives on your machine."""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = os.getenv("HIRESTREAM_DIR", str(Path(__file__).resolve().parents[1]))
RAW = os.getenv("RAW_INPUT", "data/sample_postings.csv")
CLEAN = os.getenv("CLEAN_OUTPUT", "output/clean_postings")
REPORTS = os.getenv("REPORT_DIR", "output/reports")


def check_load():
    """Fail the run if BigQuery has fewer rows than the dedupe step produced."""
    reports = Path(PROJECT_DIR, REPORTS)
    dedupe = json.loads(sorted(reports.glob("dedupe_*.json"))[-1].read_text())
    bq = json.loads(sorted(reports.glob("bigquery_*.json"))[-1].read_text())
    expected = dedupe["after_cross_source_dedupe"]
    if bq["rows_in_table"] != expected:
        raise ValueError(f"BigQuery has {bq['rows_in_table']} rows, expected {expected}")
    print(f"row count check passed: {expected} rows")


with DAG(
    dag_id="hirestream",
    start_date=datetime(2026, 11, 1),
    schedule="0 */6 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["hirestream"],
) as dag:
    dedupe = BashOperator(
        task_id="spark_dedupe",
        bash_command=f"cd {PROJECT_DIR} && python jobs/dedupe_postings.py "
                     f"--input {RAW} --output {CLEAN} --report-dir {REPORTS}",
    )
    load = BashOperator(
        task_id="load_bigquery",
        bash_command=f"cd {PROJECT_DIR} && python jobs/load_bigquery.py "
                     f"--input {CLEAN} --report-dir {REPORTS}",
    )
    check = PythonOperator(task_id="check_row_counts", python_callable=check_load)

    dedupe >> load >> check
