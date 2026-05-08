import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "gold_reporting_path",
        "output_path"
    ]
)

spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

df = spark.read.parquet(args["gold_reporting_path"])

df_report = (
    df
    .select(
        "media_key",
        "media_id",
        "media_name",
        "media_type",
        "status",
        "is_active",
        "duration_seconds",
        "folder_name",
        "channel_key",
        "campaign_key",
        "load_date",
        "engagement_score",
        "engagement_dropoff_pct",
        "avg_engagement_count",
        "avg_rewatch_count",
        "peak_rewatch_count"
    )
    .withColumn(
        "engagement_category",
        F.when(F.col("engagement_score") >= 0.7, "High")
        .when(F.col("engagement_score") >= 0.4, "Medium")
        .otherwise("Low")
    )
)

df_report.write.mode("overwrite").parquet(args["output_path"])