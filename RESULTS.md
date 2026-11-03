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

## Local test run (sample data, 2026-11-03)

This run checks that the pipeline logic works. It uses the generated file from
`scripts/make_sample_data.py` (570 rows with 70 planted duplicates), not real
job-posting data, so these numbers do not belong in the table above.

Environment: Apple M5, Python 3.10.1, PySpark 3.5.1 (local mode), OpenJDK 17.0.21.

**Unit tests:** `pytest -q` -> 4 passed in 6.01s

**Spark dedupe** (`output/reports/dedupe_20261103_155126.json`):

```json
{
  "rows_in": 570,
  "after_source_id_dedupe": 530,
  "after_cross_source_dedupe": 500,
  "duplicates_removed": 70,
  "seconds": 3.14,
  "finished_at": "2026-11-03T20:51:26.618905+00:00"
}
```

| Step | Rows | Removed |
|---|---|---|
| Raw input | 570 | |
| After same-source dedupe (source + source_id) | 530 | 40 |
| After cross-source dedupe (title + company + location) | 500 | 30 |

All 70 planted duplicates were removed and the 500 originals were kept.
The clean output covers postings from 2026-10-01 to 2026-10-31, split by
source: adzuna 147, usajobs 120, jooble 112, remotive 110, other-board 11.

**Not run yet:** BigQuery load, Snowflake load, Airflow schedule (they need
cloud credentials in `.env`).
