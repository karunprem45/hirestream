import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
from dedupe_postings import dedupe, normalize  # noqa: E402

COLS = ["source", "source_id", "title", "company", "location",
        "salary_min", "salary_max", "posted_at", "skills"]


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield s
    s.stop()


def run(spark, rows):
    df = normalize(spark.createDataFrame(rows, COLS))
    return dedupe(df)


def test_same_source_id_keeps_newest(spark):
    rows = [
        ("a", "1", "Data Engineer", "Acme", "Boston", "1", "2", "2026-10-01T00:00:00", "sql"),
        ("a", "1", "Data Engineer", "Acme", "Boston", "1", "2", "2026-10-02T00:00:00", "sql"),
    ]
    clean, counts = run(spark, rows)
    assert counts["after_source_id_dedupe"] == 1
    assert clean.first()["posted_at"].day == 2


def test_cross_source_match_ignores_case_and_spaces(spark):
    rows = [
        ("a", "1", "Data Engineer", "Acme", "Boston", "1", "2", "2026-10-01T00:00:00", "sql"),
        ("b", "9", "  DATA   ENGINEER ", "acme", "boston", "1", "2", "2026-10-03T00:00:00", "sql"),
    ]
    clean, counts = run(spark, rows)
    assert counts["after_cross_source_dedupe"] == 1
    assert clean.first()["source"] == "b"


def test_different_jobs_are_kept(spark):
    rows = [
        ("a", "1", "Data Engineer", "Acme", "Boston", "1", "2", "2026-10-01T00:00:00", "sql"),
        ("a", "2", "ML Engineer", "Acme", "Boston", "1", "2", "2026-10-01T00:00:00", "python"),
    ]
    _, counts = run(spark, rows)
    assert counts["duplicates_removed"] == 0


def test_skills_split_into_array(spark):
    rows = [("a", "1", "DE", "Acme", "Boston", "1", "2", "2026-10-01T00:00:00", "SQL;Spark;")]
    clean, _ = run(spark, rows)
    assert clean.first()["skills"] == ["sql", "spark"]
