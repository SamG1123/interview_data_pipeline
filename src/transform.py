
import json
import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

# Allow importing validate.py when running this file directly.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from validate import check_schema, split_valid


SOURCE_QUESTION = "QUESTIONS ASKED BY INTERVIEW PANEL (COMPANY WISE)"
SOURCE_COMPANY = "Company Name"
SOURCE_JOB_ROLE = "Job Role"
SOURCE_DOMAIN = "Domain"


def clean_text(column):
    """Lowercase text, trim it, and collapse repeated whitespace."""
    return F.regexp_replace(
        F.lower(F.trim(column.cast("string"))),
        r"\s+",
        " ",
    )


def question_key(column):
    """Build a normalized key for question deduplication."""
    text = clean_text(column)
    return F.trim(F.regexp_replace(text, r"[^a-z0-9 ]", ""))


def company_key(column):
    """Normalize common company-name suffixes."""
    company = clean_text(column)
    company = F.regexp_replace(
        company, r"\.(com|in|org)$", ""
    )
    company = F.regexp_replace(
        company, r"[,.]?\s+(inc|ltd|llc|pvt|corp)\.?$", ""
    )
    return F.trim(company)


def run(input_path: str, out_dir: str, rejected_dir: str) -> dict:
    spark = (
        SparkSession.builder
        .appName("interview-question-pipeline")
        .getOrCreate()
    )

    try:
        # Read the source without modifying the original CSV.
        raw = (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .option("multiLine", True)
            .option("escape", '"')
            .csv(input_path)
        )

        # Map the real source schema to our pipeline schema.
        required_source = [
            SOURCE_QUESTION,
            SOURCE_COMPANY,
            SOURCE_DOMAIN,
        ]
        missing = [c for c in required_source if c not in raw.columns]

        if missing:
            raise ValueError(
                f"Missing source columns: {missing}. "
                f"Found columns: {raw.columns}"
            )

        mapped = raw.select(
            F.col(SOURCE_QUESTION).alias("question"),
            F.col(SOURCE_COMPANY).alias("company"),
            F.col(SOURCE_DOMAIN).alias("domain"),
            (
                F.col(SOURCE_JOB_ROLE).cast("string")
                if SOURCE_JOB_ROLE in raw.columns
                else F.lit(None).cast("string")
            ).alias("job_role"),
        )

        # Add optional difficulty so downstream queries have a stable schema.
        mapped = mapped.withColumn("difficulty", F.lit("unknown"))

        # Measure input and split valid/rejected rows.
        rows_in = mapped.count()
        valid, rejected = split_valid(mapped)

        rejected.write.mode("overwrite").parquet(rejected_dir)

        # Normalize fields and build the deduplication key.
        normalized = (
            valid
            .withColumn("question", F.trim(F.col("question")))
            .withColumn("company", company_key(F.col("company")))
            .withColumn("domain", clean_text(F.col("domain")))
            .withColumn(
                "job_role",
                F.when(
                    F.col("job_role").isNull()
                    | (F.trim(F.col("job_role")) == ""),
                    F.lit("unknown"),
                ).otherwise(F.trim(F.col("job_role"))),
            )
            .withColumn("difficulty", F.lit("unknown"))
            .withColumn("_question_key", question_key(F.col("question")))
        )

        # Remove rows where normalization leaves an unusable key.
        normalized = normalized.filter(
            (F.col("company") != "")
            & (F.col("_question_key") != "")
        )

        # Keep one row per normalized company-question pair.
        deduped = (
            normalized
            .dropDuplicates(["company", "_question_key"])
            .cache()
        )

        # Build company and domain dimensions.
        dim_company = (
            deduped.select(F.col("company").alias("company_name"))
            .distinct()
            .withColumn(
                "company_id",
                F.row_number().over(
                    Window.orderBy("company_name")
                ),
            )
            .select("company_id", "company_name")
        )

        dim_domain = (
            deduped.select("domain")
            .distinct()
            .withColumn(
                "domain_id",
                F.row_number().over(Window.orderBy("domain")),
            )
            .select("domain_id", "domain")
        )

        # Build the fact table.
        fact = (
            deduped.alias("q")
            .join(
                dim_company.alias("c"),
                F.col("q.company") == F.col("c.company_name"),
            )
            .join(
                dim_domain.alias("d"),
                F.col("q.domain") == F.col("d.domain"),
            )
            .select(
                F.sha2(
                    F.concat_ws(
                        "|",
                        F.col("q.company"),
                        F.col("q._question_key"),
                    ),
                    256,
                ).alias("question_id"),
                F.col("c.company_id"),
                F.col("d.domain_id"),
                F.col("q.question"),
                F.col("q.job_role"),
                F.col("q.difficulty"),
                F.col("q.domain"),
            )
        )

        # Write curated outputs.
        # Domain partitioning can create many small folders; we will
        # assess cardinality and file sizes before the AWS deployment.
        fact.write.mode("overwrite").parquet(
            f"{out_dir}/fact_questions"
        )
        dim_company.coalesce(1).write.mode("overwrite").parquet(
            f"{out_dir}/dim_company"
        )
        dim_domain.coalesce(1).write.mode("overwrite").parquet(
            f"{out_dir}/dim_domain"
        )

        rows_rejected = rejected.count()
        rows_after_dedupe = deduped.count()

        summary = {
            "rows_in": rows_in,
            "rows_rejected": rows_rejected,
            "pct_rejected": round(
                100.0 * rows_rejected / rows_in, 2
            ) if rows_in else 0.0,
            "rows_after_dedupe": rows_after_dedupe,
            "rows_removed_after_validation": (
                rows_in - rows_rejected - rows_after_dedupe
            ),
            "companies": dim_company.count(),
            "domains": dim_domain.count(),
        }

        print(json.dumps(summary, indent=2))
        return summary

    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(
            "Usage: python src/transform.py "
            "<input_csv> <curated_output_dir> <rejected_output_dir>"
        )

    run(sys.argv[1], sys.argv[2], sys.argv[3])
