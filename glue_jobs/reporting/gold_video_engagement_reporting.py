import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------------
# Args
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "gold_fact_media_engagement_path",
        "dim_media_path",
        "gold_video_engagement_reporting_path"
    ]
)

JOB_NAME = args["JOB_NAME"]
GOLD_FACT_MEDIA_ENGAGEMENT_PATH = args["gold_fact_media_engagement_path"].rstrip("/")
DIM_MEDIA_PATH = args["dim_media_path"].rstrip("/")
GOLD_VIDEO_ENGAGEMENT_REPORTING_PATH = args["gold_video_engagement_reporting_path"].rstrip("/")

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()

# --------------------------------------------------
# Read inputs
# --------------------------------------------------
df_fact = spark.read.parquet(GOLD_FACT_MEDIA_ENGAGEMENT_PATH)
df_dim_media = spark.read.parquet(DIM_MEDIA_PATH)

print("Fact schema:")
df_fact.printSchema()

print("Dim media schema:")
df_dim_media.printSchema()

print(f"Fact count: {df_fact.count()}")
print(f"Dim media count: {df_dim_media.count()}")

# --------------------------------------------------
# Normalize types
# --------------------------------------------------
df_fact = df_fact.withColumn("media_id", F.col("media_id").cast("string"))
df_dim_media = df_dim_media.withColumn("media_id", F.col("media_id").cast("string"))

# --------------------------------------------------
# Optional dedupe for dim_media
# Keep latest record per media_id
# --------------------------------------------------
dim_window = Window.partitionBy("media_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("ingested_at_utc").desc_nulls_last()
)

df_dim_media_latest = (
    df_dim_media
    .withColumn("rn", F.row_number().over(dim_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print(f"Dim media latest count: {df_dim_media_latest.count()}")

# --------------------------------------------------
# Join fact to dim_media
# --------------------------------------------------
df_reporting = (
    df_fact.alias("f")
    .join(
        df_dim_media_latest.alias("d"),
        on="media_id",
        how="left"
    )
    .select(
        # Dimension columns
        F.col("d.media_key"),
        F.col("f.media_id"),
        F.col("d.media_hashed_id"),
        F.col("d.media_name"),
        F.col("d.media_type"),
        F.col("d.status"),
        F.col("d.is_active"),
        F.col("d.duration_seconds"),
        F.col("d.project_id"),
        F.col("d.folder_id"),
        F.col("d.folder_hashed_id"),
        F.col("d.folder_name"),
        F.col("d.channel_key"),
        F.col("d.campaign_key"),
        F.col("d.media_created_date"),
        F.col("d.media_updated_date"),

        # Fact columns
        F.col("f.load_date"),
        F.col("f.engagement_score"),
        F.col("f.timeline_points"),
        F.col("f.start_engagement_count"),
        F.col("f.max_engagement_count"),
        F.col("f.max_rewatch_count"),
        F.col("f.avg_engagement_count"),
        F.col("f.avg_rewatch_count"),
        F.col("f.engagement_dropoff_pct"),
        F.col("f.peak_rewatch_index"),
        F.col("f.peak_rewatch_count"),

        # Audit columns
        F.col("f.run_id").alias("fact_run_id"),
        F.col("f.ingested_at").alias("fact_ingested_at"),
        F.col("d.run_id").alias("dim_run_id"),
        F.col("d.ingested_at_utc").alias("dim_ingested_at_utc")
    )
)

reporting_count = df_reporting.count()
print(f"Reporting count: {reporting_count}")

if reporting_count == 0:
    raise Exception("gold_video_engagement_reporting is empty after join")

df_reporting.show(10, truncate=False)

# --------------------------------------------------
# Write output
# --------------------------------------------------
(
    df_reporting.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("load_date")
    .save(GOLD_VIDEO_ENGAGEMENT_REPORTING_PATH)
)

print(f"Wrote reporting table to {GOLD_VIDEO_ENGAGEMENT_REPORTING_PATH}")

spark.stop()