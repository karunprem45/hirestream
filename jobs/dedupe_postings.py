"""Clean and deduplicate raw job postings with PySpark.

Two passes:
  1. Same source + same source_id: keep the newest copy.
  2. Same job on different sources: match on normalized title + company +
     location, keep the newest copy.

Writes clean Parquet plus a JSON run report with real counts and timing.
The report numbers are what you can put on a resume.
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    if path.endswith(".csv"):
        return spark.read.option("header", True).option("multiLine", True).csv(path)
    if path.endswith((".json", ".jsonl")):
        return spark.read.json(path)
    return spark.read.parquet(path)


def normalize(df: DataFrame) -> DataFrame:
    def clean(c):
        return F.lower(F.trim(F.regexp_replace(F.col(c), r"\s+", " ")))

    return (
        df.withColumn("title_norm", clean("title"))
        .withColumn("company_norm", clean("company"))
        .withColumn("location_norm", clean("location"))
        .withColumn("posted_at", F.to_timestamp("posted_at"))
        .withColumn("salary_min", F.col("salary_min").cast("double"))
        .withColumn("salary_max", F.col("salary_max").cast("double"))
        .withColumn("skills", F.split(F.lower(F.coalesce(F.col("skills"), F.lit(""))), ";"))
        .withColumn("skills", F.expr("filter(skills, s -> s != '')"))
    )


def keep_newest(df: DataFrame, keys: list) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("posted_at").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def dedupe(df: DataFrame):
    """Return (clean_df, counts). counts holds row counts after each step."""
    counts = {"rows_in": df.count()}
    step1 = keep_newest(df, ["source", "source_id"])
    counts["after_source_id_dedupe"] = step1.count()
    step2 = keep_newest(step1, ["title_norm", "company_norm", "location_norm"])
    counts["after_cross_source_dedupe"] = step2.count()
    counts["duplicates_removed"] = counts["rows_in"] - counts["after_cross_source_dedupe"]
    clean = step2.withColumn("posted_date", F.to_date("posted_at"))
    return clean, counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--report-dir", default="output/reports")
    args = ap.parse_args()

    t0 = time.time()
    spark = SparkSession.builder.appName("hirestream-dedupe").getOrCreate()
    raw = normalize(read_raw(spark, args.input))
    clean, counts = dedupe(raw)
    clean.drop("title_norm", "company_norm", "location_norm") \
        .write.mode("overwrite").parquet(args.output)
    counts["seconds"] = round(time.time() - t0, 2)
    counts["finished_at"] = datetime.now(timezone.utc).isoformat()

    Path(args.report_dir).mkdir(parents=True, exist_ok=True)
    report = Path(args.report_dir) / f"dedupe_{datetime.now():%Y%m%d_%H%M%S}.json"
    report.write_text(json.dumps(counts, indent=2))
    print(json.dumps(counts, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
