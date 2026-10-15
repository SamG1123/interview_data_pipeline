# Interview Question Data Pipeline

An end-to-end data engineering project that transforms raw interview
questions into curated datasets and makes them available for analytics
using Apache Spark, AWS Glue, Amazon S3, and Amazon Athena.

## Architecture

Raw CSV
  -> PySpark ETL
  -> Data validation and deduplication
  -> Curated Parquet datasets
  -> Amazon S3
  -> AWS Glue Data Catalog
  -> Amazon Athena SQL analytics

## Tech Stack

- Python
- PySpark
- Apache Parquet
- AWS Glue
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- pytest
- Git and GitHub

## Dataset

The pipeline processes interview questions associated with companies,
technical domains, job roles, and difficulty levels.

The source CSV is maintained locally and is not included in this
repository.

## Data Model

### fact_questions

| Column | Description |
|---|---|
| question_id | Question identifier |
| company_id | Company dimension key |
| domain_id | Domain dimension key |
| question | Interview question text |
| job_role | Associated job role |
| difficulty | Question difficulty |
| domain | Interview question domain |

### dim_company

| Column | Description |
|---|---|
| company_id | Company identifier |
| company_name | Normalized company name |

### dim_domain

| Column | Description |
|---|---|
| domain_id | Domain identifier |
| domain | Interview domain name |

## Verified Pipeline Results

The AWS ETL job completed successfully, and the following table counts
were verified through Athena.

| Dataset | Rows |
|---|---:|
| fact_questions | 6,653 |
| dim_company | 113 |
| dim_domain | 674 |

The pipeline also produces a rejected-record dataset for records that
fail validation.

## Analytics

SQL queries are available in the `sql/` directory.

- `company_question_counts.sql`: ranks companies by distinct question count.
- `domain_question_counts.sql`: ranks domains by distinct question count.
- `company_data_quality.sql`: measures unknown job roles per company.

Example Athena query:

```sql
SELECT
    c.company_name,
    COUNT(DISTINCT f.question_id) AS question_count
FROM fact_questions AS f
JOIN dim_company AS c
    ON f.company_id = c.company_id
GROUP BY c.company_name
ORDER BY question_count DESC
LIMIT 10;
