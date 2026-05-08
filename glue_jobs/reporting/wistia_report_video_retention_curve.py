import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "timeline_path",
        "output_path"
    ]
)

spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

df = spark.read.parquet(args["timeline_path"])

df_report = (
    df
    .select(
        "media_id",
        "load_date",
        "timeline_index",
        "engagement_pct_of_start",
        "rewatch_pct_of_peak"
    )
)

df_report.write.mode("overwrite").parquet(args["output_path"])