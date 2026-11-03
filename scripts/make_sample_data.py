"""Make a small fake postings file so you can test the pipeline before
pointing it at real job-posting data. These rows are made up. Never put
numbers from this file on a resume."""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(7)
TITLES = ["Data Engineer", "ML Engineer", "Data Analyst", "Software Engineer"]
COMPANIES = ["Acme Corp", "Globex", "Initech", "Umbrella"]
CITIES = ["Boston, MA", "San Mateo, CA", "Austin, TX", "Remote"]
SKILLS = ["python", "sql", "spark", "airflow", "aws", "docker", "pandas", "tableau"]
SOURCES = ["adzuna", "usajobs", "remotive", "jooble"]


def main(n=500, out="data/sample_postings.csv"):
    rows = []
    start = datetime(2026, 10, 1)
    for i in range(n):
        rows.append({
            "source": random.choice(SOURCES),
            "source_id": f"id-{i}",
            "title": random.choice(TITLES),
            "company": f"{random.choice(COMPANIES)} {i}",
            "location": random.choice(CITIES),
            "salary_min": random.choice(["", 80000, 95000, 110000]),
            "salary_max": random.choice(["", 120000, 140000, 160000]),
            "posted_at": (start + timedelta(hours=random.randint(0, 720))).isoformat(),
            "skills": ";".join(random.sample(SKILLS, k=3)),
        })
    originals = list(rows)
    # Same posting pulled twice from one source (same source_id).
    rows += [dict(r) for r in random.sample(originals, 40)]
    # Same job listed on another board, with messy casing and spacing.
    for r in random.sample(originals, 30):
        d = dict(r)
        d["source"] = "other-board"
        d["source_id"] = "x-" + r["source_id"]
        d["title"] = "  " + r["title"].upper() + " "
        rows.append(d)
    random.shuffle(rows)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} rows to {out}")


if __name__ == "__main__":
    main()
