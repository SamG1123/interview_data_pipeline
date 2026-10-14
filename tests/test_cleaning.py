
import pathlib
import sys

import pytest
from pyspark.sql import SparkSession, functions as F

sys.path.append(
    str(pathlib.Path(__file__).resolve().parent.parent / "src")
)

from transform import company_key, question_key
from validate import split_valid


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("tests")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def test_company_key_normalizes(spark):
    df = spark.createDataFrame(
        [("Amazon.com",), (" AMAZON ",), ("Acme Inc.",)],
        ["company"],
    )
    out = [
        r[0]
        for r in df.select(company_key(F.col("company"))).collect()
    ]
    assert out == ["amazon", "amazon", "acme"]


def test_question_key_ignores_case_spacing_and_punctuation(spark):
    df = spark.createDataFrame(
        [("What is a Deadlock?",), ("what is a deadlock",)],
        ["q"],
    )
    keys = [
        r[0]
        for r in df.select(question_key(F.col("q"))).collect()
    ]
    assert keys[0] == keys[1]


def test_split_valid_flags_each_reason(spark):
    rows = [
        ("What is a deadlock and how do you avoid it?", "amazon", "os"),
        ("", "amazon", "os"),
        ("short", "amazon", "os"),
        ("What is a mutex in detail?", None, "os"),
        ("What is a semaphore in detail?", "amazon", None),
    ]
    df = spark.createDataFrame(
        rows,
        "question string, company string, domain string",
    )

    valid, rejected = split_valid(df)

    assert valid.count() == 1
    reasons = {
        r["reject_reason"] for r in rejected.collect()
    }
    assert reasons == {
        "missing_question",
        "question_too_short",
        "missing_company",
        "missing_domain",
    }
