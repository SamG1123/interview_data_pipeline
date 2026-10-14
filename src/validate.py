
from pyspark.sql import DataFrame, functions as F

MIN_QUESTION_LEN = 15


def _blank(column: str):
    """Return a condition for null or whitespace-only values."""
    return (
        F.col(column).isNull()
        | (F.trim(F.col(column).cast("string")) == "")
    )


def check_schema(df: DataFrame) -> None:
    """Ensure the mapped dataframe contains required columns."""
    required = ["question", "company", "domain"]
    missing = [column for column in required if column not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def split_valid(df: DataFrame):
    """
    Return (valid_df, rejected_df).

    Each rejected record receives one reject_reason.
    Missing required values take precedence over question length.
    """
    check_schema(df)

    reason = (
        F.when(_blank("question"), "missing_question")
        .when(
            F.length(F.trim(F.col("question"))) < MIN_QUESTION_LEN,
            "question_too_short",
        )
        .when(_blank("company"), "missing_company")
        .when(_blank("domain"), "missing_domain")
    )

    flagged = df.withColumn("reject_reason", reason)

    valid = (
        flagged
        .filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
    )

    rejected = flagged.filter(F.col("reject_reason").isNotNull())

    return valid, rejected
