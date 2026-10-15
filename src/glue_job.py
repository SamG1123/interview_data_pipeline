
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "INPUT_PATH",
        "CURATED_PATH",
        "REJECTED_PATH",
    ],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

from transform import run

summary = run(
    args["INPUT_PATH"],
    args["CURATED_PATH"],
    args["REJECTED_PATH"],
    spark=spark,
)

print("Glue ETL summary:", summary)

job.commit()

