# Results log (fill in from real runs only)

Copy numbers straight from the JSON files in `output/reports/`. Do not round
up, estimate, or use numbers from the fake sample data.

| What | Value | Where it comes from |
|---|---|---|
| Raw rows in (real job-posting data) | | `dedupe_*.json` -> rows_in |
| Duplicates removed | | `dedupe_*.json` -> duplicates_removed |
| Spark job time (seconds) | | `dedupe_*.json` -> seconds |
| Rows in BigQuery | | `bigquery_*.json` -> rows_in_table |
| BigQuery load time (seconds) | | `bigquery_*.json` -> seconds |
| Bytes processed, partition-filtered query vs full scan | | BigQuery console, query 2 with and without the WHERE |
| Successful Airflow runs / total runs | | Airflow UI, DAG runs page |
| Rows in Snowflake (optional) | | `load_snowflake.py` output |
