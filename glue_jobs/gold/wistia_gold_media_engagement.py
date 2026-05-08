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
        "silver_summary_path",
        "silver_timeline_path",
        "gold_fact_summary_path",
        "gold_fact_timeline_path"
    ]
)

JOB_NAME = args["JOB_NAME"]
SILVER_SUMMARY_PATH = args["silver_summary_path"].rstrip("/")
SILVER_TIMELINE_PATH = args["silver_timeline_path"].rstrip("/")
GOLD_FACT_SUMMARY_PATH = args["gold_fact_summary_path"].rstrip("/")
GOLD_FACT_TIMELINE_PATH = args["gold_fact_timeline_path"].rstrip("/")

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()

# --------------------------------------------------
# Read Silver parquet
# Do NOT use recursiveFileLookup here because
# Spark needs to infer partition columns like load_date
# --------------------------------------------------
df_summary = spark.read.parquet(SILVER_SUMMARY_PATH)
df_timeline = spark.read.parquet(SILVER_TIMELINE_PATH)

print("Summary schema:")
df_summary.printSchema()

print("Timeline schema:")
df_timeline.printSchema()

print(f"Summary count: {df_summary.count()}")
print(f"Timeline count: {df_timeline.count()}")

# --------------------------------------------------
# Start engagement count = engagement at timeline_index 0
# --------------------------------------------------
start_engagement_df = (
    df_timeline
    .filter(F.col("timeline_index") == 0)
    .select(
        "media_id",
        "load_date",
        "run_id",
        F.col("engagement_count").alias("start_engagement_count")
    )
)

# --------------------------------------------------
# Timeline-level aggregates
# --------------------------------------------------
timeline_agg = (
    df_timeline
    .groupBy("media_id", "load_date", "run_id")
    .agg(
        F.avg("engagement_count").alias("avg_engagement_count"),
        F.avg("rewatch_count").alias("avg_rewatch_count"),
        F.max("rewatch_count").alias("peak_rewatch_count")
    )
)

# --------------------------------------------------
# Peak rewatch index
# --------------------------------------------------
rewatch_peak_window = Window.partitionBy("media_id", "load_date", "run_id").orderBy(
    F.col("rewatch_count").desc(),
    F.col("timeline_index").asc()
)

peak_rewatch_index_df = (
    df_timeline
    .withColumn("rn", F.row_number().over(rewatch_peak_window))
    .filter(F.col("rn") == 1)
    .select(
        "media_id",
        "load_date",
        "run_id",
        F.col("timeline_index").alias("peak_rewatch_index")
    )
)

# --------------------------------------------------
# Gold Fact Summary
# --------------------------------------------------
df_gold_summary = (
    df_summary.alias("s")
    .join(
        timeline_agg.alias("t"),
        on=["media_id", "load_date", "run_id"],
        how="left"
    )
    .join(
        start_engagement_df.alias("se"),
        on=["media_id", "load_date", "run_id"],
        how="left"
    )
    .join(
        peak_rewatch_index_df.alias("p"),
        on=["media_id", "load_date", "run_id"],
        how="left"
    )
    .withColumn(
        "engagement_dropoff_pct",
        F.when(
            F.col("start_engagement_count") > 0,
            (F.col("start_engagement_count") - F.col("avg_engagement_count")) / F.col("start_engagement_count")
        ).otherwise(F.lit(None))
    )
    .select(
        F.col("media_id"),
        F.col("load_date"),
        F.col("engagement_score"),
        F.col("timeline_points"),
        F.col("start_engagement_count"),
        F.col("max_engagement_count"),
        F.col("max_rewatch_count"),
        F.round(F.col("avg_engagement_count"), 2).alias("avg_engagement_count"),
        F.round(F.col("avg_rewatch_count"), 2).alias("avg_rewatch_count"),
        F.round(F.col("engagement_dropoff_pct"), 6).alias("engagement_dropoff_pct"),
        F.col("peak_rewatch_index"),
        F.col("peak_rewatch_count"),
        F.col("run_id"),
        F.col("ingested_at")
    )
)

print(f"Gold summary count: {df_gold_summary.count()}")
df_gold_summary.show(5, truncate=False)

# --------------------------------------------------
# Gold Fact Timeline
# --------------------------------------------------
start_window = Window.partitionBy("media_id", "load_date", "run_id")
peak_window = Window.partitionBy("media_id", "load_date", "run_id")

df_gold_timeline = (
    df_timeline
    .withColumn(
        "start_engagement_count",
        F.max(
            F.when(F.col("timeline_index") == 0, F.col("engagement_count"))
        ).over(start_window)
    )
    .withColumn(
        "peak_rewatch_count",
        F.max("rewatch_count").over(peak_window)
    )
    .withColumn(
        "engagement_pct_of_start",
        F.when(
            F.col("start_engagement_count") > 0,
            F.col("engagement_count") / F.col("start_engagement_count")
        ).otherwise(F.lit(None))
    )
    .withColumn(
        "rewatch_pct_of_peak",
        F.when(
            F.col("peak_rewatch_count") > 0,
            F.col("rewatch_count") / F.col("peak_rewatch_count")
        ).otherwise(F.lit(None))
    )
    .select(
        "media_id",
        "load_date",
        "timeline_index",
        "engagement_count",
        "rewatch_count",
        F.round("engagement_pct_of_start", 6).alias("engagement_pct_of_start"),
        F.round("rewatch_pct_of_peak", 6).alias("rewatch_pct_of_peak"),
        "run_id",
        "ingested_at"
    )
)

print(f"Gold timeline count: {df_gold_timeline.count()}")
df_gold_timeline.show(10, truncate=False)

# --------------------------------------------------
# Write Gold Summary
# --------------------------------------------------
(
    df_gold_summary.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("load_date")
    .save(GOLD_FACT_SUMMARY_PATH)
)

print(f"Wrote gold summary to {GOLD_FACT_SUMMARY_PATH}")

# --------------------------------------------------
# Write Gold Timeline
# --------------------------------------------------
(
    df_gold_timeline.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("load_date")
    .save(GOLD_FACT_TIMELINE_PATH)
)

print(f"Wrote gold timeline to {GOLD_FACT_TIMELINE_PATH}")

spark.stop()