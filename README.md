# HireStream

Takes raw job postings from job-board APIs, removes duplicates with PySpark,
loads the clean data into BigQuery (and optionally Snowflake), and runs on
an Airflow schedule every 6 hours with a row-count check after each load.

```
raw postings -> spark_dedupe -> load_bigquery -> check_row_counts
                     |                |
              dedupe report      load report      (output/reports/*.json)
```

## 1. Setup (about 30 minutes)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config.example.env .env      # fill in your values
```

PySpark needs Java 11 or 17. Check with `java -version`.

## 2. Test locally with fake data first

```bash
python scripts/make_sample_data.py
pytest -q
python jobs/dedupe_postings.py --input data/sample_postings.csv \
    --output output/clean_postings --report-dir output/reports
```

The sample file has 570 rows with 70 planted duplicates, so the report should
show 500 rows after dedupe. If it does, the logic works. These are test
numbers only; do not put them on a resume.

## 3. Point it at real job-posting data

Export raw postings from any job-board API (before any dedupe) to CSV, JSON or
Parquet with these columns:

`source, source_id, title, company, location, salary_min, salary_max, posted_at, skills`

`skills` is one string separated by semicolons. If your column names differ,
rename them in the export or in `normalize()`. Set `RAW_INPUT` in `.env`.

## 4. BigQuery (free sandbox)

1. Go to the BigQuery console and create a project. The sandbox needs no card.
2. Create a service account with the BigQuery Admin role, download its JSON
   key, and set `GOOGLE_APPLICATION_CREDENTIALS` and `GCP_PROJECT` in `.env`.
3. Run:
   ```bash
   set -a && source .env && set +a
   python jobs/load_bigquery.py --input output/clean_postings
   ```
4. Run the queries in `sql/bigquery_analysis.sql`. For query 2, note the bytes
   processed with and without the `WHERE` on `posted_date`. That difference is
   what partitioning saves you.

## 5. Airflow

```bash
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
export AIRFLOW_HOME=~/airflow
export HIRESTREAM_DIR=$(pwd)
mkdir -p ~/airflow/dags && cp dags/hirestream_dag.py ~/airflow/dags/
airflow standalone
```

Open localhost:8080, turn on `hirestream`, and trigger a run. Leave
it on for a few days so you have a real run history.

## 6. Snowflake (optional)

Start a free 30-day trial, fill in the Snowflake values in `.env`, then:

```bash
python jobs/load_snowflake.py --input output/clean_postings
```

## 7. Record results

Fill in `RESULTS.md` from the JSON reports and the Airflow UI.
