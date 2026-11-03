"""Load the clean Parquet output into BigQuery, partitioned by posted_date.
The free BigQuery sandbox works for this; no billing account needed.

Writes a JSON report with rows in the table and load time."""
import argparse
import glob
import json
import os
import time
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="folder of clean Parquet files")
    ap.add_argument("--project", default=os.getenv("GCP_PROJECT"))
    ap.add_argument("--dataset", default=os.getenv("BQ_DATASET", "hirestream"))
    ap.add_argument("--table", default=os.getenv("BQ_TABLE", "postings"))
    ap.add_argument("--report-dir", default="output/reports")
    args = ap.parse_args()

    files = sorted(glob.glob(f"{args.input}/*.parquet"))
    if not files:
        raise SystemExit(f"no parquet files in {args.input}")

    client = bigquery.Client(project=args.project)
    client.create_dataset(args.dataset, exists_ok=True)
    table_id = f"{args.project}.{args.dataset}.{args.table}"

    t0 = time.time()
    for i, f in enumerate(files):
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            # First file replaces the table, the rest append to it.
            write_disposition=(bigquery.WriteDisposition.WRITE_TRUNCATE if i == 0
                               else bigquery.WriteDisposition.WRITE_APPEND),
            time_partitioning=bigquery.TimePartitioning(field="posted_date"),
        )
        with open(f, "rb") as fh:
            client.load_table_from_file(fh, table_id, job_config=cfg).result()

    table = client.get_table(table_id)
    report = {
        "table": table_id,
        "rows_in_table": table.num_rows,
        "files_loaded": len(files),
        "seconds": round(time.time() - t0, 2),
    }
    Path(args.report_dir).mkdir(parents=True, exist_ok=True)
    out = Path(args.report_dir) / f"bigquery_{datetime.now():%Y%m%d_%H%M%S}.json"
    out.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
