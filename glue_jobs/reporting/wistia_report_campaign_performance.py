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
    .groupBy("campaign_key", "channel_key", "folder_name")
    .agg(
        F.countDistinct("media_id").alias("video_count"),
        F.avg("engagement_score").alias("avg_engagement_score"),
        F.avg("engagement_dropoff_pct").alias("avg_dropoff_pct"),
        F.sum("peak_rewatch_count").alias("total_rewatch_count")
    )
)

df_report.write.mode("overwrite").parquet(args["output_path"])