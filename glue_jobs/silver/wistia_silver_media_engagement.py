import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bronze_path",
        "silver_summary_path",
        "silver_timeline_path"
    ]
)

JOB_NAME = args["JOB_NAME"]
BRONZE_PATH = args["bronze_path"].rstrip("/")
SILVER_SUMMARY_PATH = args["silver_summary_path"].rstrip("/")
SILVER_TIMELINE_PATH = args["silver_timeline_path"].rstrip("/")

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()

print(f"Reading bronze from: {BRONZE_PATH}")

df_bronze = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(BRONZE_PATH)
)

print("Bronze schema:")
df_bronze.printSchema()

bronze_count = df_bronze.count()
print(f"Bronze row count: {bronze_count}")

if bronze_count == 0:
    raise Exception(f"No bronze records found under {BRONZE_PATH}")

df_bronze = (
    df_bronze
    .filter(F.col("source") == "wistia_stats_media_engagement")
    .filter(F.col("raw_record").isNotNull())
)

filtered_count = df_bronze.count()
print(f"Filtered bronze row count: {filtered_count}")

if filtered_count == 0:
    raise Exception("No valid wistia_stats_media_engagement rows found after filtering")

df_summary = (
    df_bronze
    .select(
        F.col("media_id").cast("string").alias("media_id"),
        F.col("load_date").cast("string").alias("load_date"),
        F.col("run_id").cast("string").alias("run_id"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
        F.col("raw_record.engagement").cast("double").alias("engagement_score"),
        F.size(F.col("raw_record.engagement_data")).alias("timeline_points"),
        F.array_max(F.col("raw_record.engagement_data")).cast("long").alias("max_engagement_count"),
        F.array_max(F.col("raw_record.rewatch_data")).cast("long").alias("max_rewatch_count")
    )
    .dropDuplicates(["media_id", "run_id"])
)

summary_count = df_summary.count()
print(f"Summary row count: {summary_count}")

if summary_count == 0:
    raise Exception("Summary dataframe is empty")

df_timeline = (
    df_bronze
    .select(
        F.col("media_id").cast("string").alias("media_id"),
        F.col("load_date").cast("string").alias("load_date"),
        F.col("run_id").cast("string").alias("run_id"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
        F.col("raw_record.engagement_data").alias("engagement_data"),
        F.col("raw_record.rewatch_data").alias("rewatch_data")
    )
    .withColumn("timeline_points", F.size(F.col("engagement_data")))
    .withColumn("rewatch_points", F.size(F.col("rewatch_data")))
    .filter(F.col("timeline_points") > 0)
    .filter(F.col("timeline_points") == F.col("rewatch_points"))
    .select(
        "media_id",
        "load_date",
        "run_id",
        "ingested_at",
        F.posexplode(F.col("engagement_data")).alias("timeline_index", "engagement_count"),
        F.col("rewatch_data")
    )
    .withColumn("rewatch_count", F.col("rewatch_data")[F.col("timeline_index")])
    .drop("rewatch_data")
)

timeline_count = df_timeline.count()
print(f"Timeline row count: {timeline_count}")

if timeline_count == 0:
    raise Exception("Timeline dataframe is empty")

# Write summary
(
    df_summary.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("load_date")
    .save(SILVER_SUMMARY_PATH)
)

print(f"Wrote summary parquet to {SILVER_SUMMARY_PATH}")

# Write timeline
(
    df_timeline.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("load_date")
    .save(SILVER_TIMELINE_PATH)
)

print(f"Wrote timeline parquet to {SILVER_TIMELINE_PATH}")

spark.stop()