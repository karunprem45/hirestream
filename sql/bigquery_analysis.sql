-- Replace your-gcp-project-id with your project. Run in the BigQuery console.
-- Note the "bytes processed" each query shows: a good interview talking point.

-- 1. Top 15 skills by number of postings
SELECT skill, COUNT(*) AS postings
FROM `your-gcp-project-id.hirestream.postings`, UNNEST(skills) AS skill
GROUP BY skill
ORDER BY postings DESC
LIMIT 15;

-- 2. Postings per day (reads only the partitions in the date range)
SELECT posted_date, COUNT(*) AS postings
FROM `your-gcp-project-id.hirestream.postings`
WHERE posted_date BETWEEN '2026-10-01' AND '2026-10-31'
GROUP BY posted_date
ORDER BY posted_date;

-- 3. Median advertised max salary by skill (skills with 30+ salaried postings)
SELECT skill,
       COUNT(*) AS n,
       APPROX_QUANTILES(salary_max, 2)[OFFSET(1)] AS median_salary_max
FROM `your-gcp-project-id.hirestream.postings`, UNNEST(skills) AS skill
WHERE salary_max IS NOT NULL
GROUP BY skill
HAVING n >= 30
ORDER BY median_salary_max DESC;
