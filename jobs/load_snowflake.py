"""Optional: load the same clean Parquet into Snowflake (free 30-day trial).
Reads credentials from environment variables in .env."""
import argparse
import glob
import json
import os
import time

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    args = ap.parse_args()

    df = pd.concat(pd.read_parquet(f) for f in sorted(glob.glob(f"{args.input}/*.parquet")))
    df["skills"] = df["skills"].apply(lambda s: ";".join(s))  # arrays as text
    df.columns = [c.upper() for c in df.columns]

    t0 = time.time()
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "HIRESTREAM"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )
    ok, _, nrows, _ = write_pandas(conn, df, os.environ.get("SNOWFLAKE_TABLE", "POSTINGS"),
                                   auto_create_table=True, overwrite=True)
    conn.close()
    print(json.dumps({"success": ok, "rows_loaded": nrows,
                      "seconds": round(time.time() - t0, 2)}, indent=2))


if __name__ == "__main__":
    main()
