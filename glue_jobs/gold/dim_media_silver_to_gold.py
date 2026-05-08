import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job


# -----------------------------------------------------------------------------
# Arguments
# -----------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "silver_path",
        "gold_path"
    ]
)

JOB_NAME = args["JOB_NAME"]
SILVER_PATH = args["silver_path"]
GOLD_PATH = args["gold_path"]


# -----------------------------------------------------------------------------
# Spark / Glue setup
# -----------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# -----------------------------------------------------------------------------
# Read silver metadata
# -----------------------------------------------------------------------------
df_raw = (
    spark.read
    .format("parquet")
    .load(SILVER_PATH)
    .withColumn("_source_file", F.input_file_name())
)

print(f"SILVER_PATH = {SILVER_PATH}")
print(f"GOLD_PATH = {GOLD_PATH}")
print("=== RAW SILVER SCHEMA ===")
df_raw.printSchema()

raw_count = df_raw.count()
print(f"RAW COUNT = {raw_count}")

df_raw.select(
    "media_hashed_id",
    "media_id",
    "media_name",
    "media_type",
    "status",
    "load_date",
    "_source_file"
).show(10, truncate=False)


# -----------------------------------------------------------------------------
# Ensure load_date exists and is populated
# -----------------------------------------------------------------------------
# Silver already has load_date, but this makes the gold job resilient in case
# the column is blank/null for any reason.
df_raw = df_raw.withColumn(
    "derived_load_date",
    F.regexp_extract(
        F.col("_source_file"),
        r"load_date=([0-9]{4}-[0-9]{2}-[0-9]{2})",
        1
    )
).withColumn(
    "load_date",
    F.when(
        F.col("load_date").isNull() | (F.col("load_date") == ""),
        F.col("derived_load_date")
    ).otherwise(F.col("load_date"))
)

null_load_date_raw_count = df_raw.filter(F.col("load_date").isNull() | (F.col("load_date") == "")).count()
print(f"NULL_OR_BLANK LOAD_DATE IN RAW COUNT = {null_load_date_raw_count}")


# -----------------------------------------------------------------------------
# Basic filtering
# -----------------------------------------------------------------------------
df = (
    df_raw
    .filter(F.col("media_hashed_id").isNotNull())
    .filter(F.col("media_name").isNotNull())
)

filtered_count = df.count()
print(f"FILTERED COUNT = {filtered_count}")

df.select(
    "media_hashed_id",
    "media_id",
    "media_name",
    "media_type",
    "status",
    "load_date"
).show(10, truncate=False)


# -----------------------------------------------------------------------------
# Deduplicate at gold level
# Keep latest version per media_hashed_id
# -----------------------------------------------------------------------------
w = Window.partitionBy("media_hashed_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("ingested_at_utc").desc_nulls_last(),
    F.col("load_date").desc_nulls_last()
)

df_latest = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn", "_source_file", "derived_load_date")
)

latest_count = df_latest.count()
print(f"LATEST COUNT = {latest_count}")


# -----------------------------------------------------------------------------
# Build conformed gold dimension
# -----------------------------------------------------------------------------
df_gold = (
    df_latest
    .withColumn("media_key", F.sha2(F.col("media_hashed_id"), 256))
    .withColumn("is_active", F.when(F.col("status") == F.lit("ready"), F.lit(True)).otherwise(F.lit(False)))
    .withColumn("record_source", F.lit("wistia"))
    .withColumn("channel_key", F.lit(None).cast("string"))
    .withColumn("campaign_key", F.lit(None).cast("string"))
    .withColumn("folder_id", F.lit(None).cast("string"))
    .withColumn("folder_hashed_id", F.lit(None).cast("string"))
    .withColumn("folder_name", F.lit(None).cast("string"))
    .withColumn("archived", F.lit(None).cast("boolean"))
    .withColumn("media_created_date", F.to_date("created_at"))
    .withColumn("media_updated_date", F.to_date("updated_at"))
)

df_gold = df_gold.select(
    "media_key",
    "media_hashed_id",
    "media_id",
    "media_name",
    "media_type",
    "status",
    "is_active",
    "description",
    "duration_seconds",
    "project_id",
    "folder_id",
    "folder_hashed_id",
    "folder_name",
    "archived",
    "thumbnail_url",
    "embed_url",
    "seo_description",
    "asset_count",
    "channel_key",
    "campaign_key",
    "media_created_date",
    "media_updated_date",
    "created_at",
    "updated_at",
    "record_source",
    "run_id",
    "ingested_at_utc",
    "load_date"
)

gold_count = df_gold.count()
print(f"GOLD COUNT = {gold_count}")

null_media_key_count = df_gold.filter(F.col("media_key").isNull()).count()
print(f"NULL MEDIA_KEY COUNT = {null_media_key_count}")

null_load_date_count = df_gold.filter(F.col("load_date").isNull()).count()
print(f"NULL LOAD_DATE COUNT = {null_load_date_count}")

blank_load_date_count = df_gold.filter(F.col("load_date") == "").count()
print(f"BLANK LOAD_DATE COUNT = {blank_load_date_count}")

distinct_load_dates = [row["load_date"] for row in df_gold.select("load_date").distinct().collect()]
print(f"DISTINCT LOAD_DATES = {distinct_load_dates}")

df_gold.select(
    "media_key",
    "media_hashed_id",
    "media_name",
    "media_type",
    "status",
    "load_date"
).show(10, truncate=False)

print(f"WRITING TO GOLD PATH = {GOLD_PATH}")


# -----------------------------------------------------------------------------
# Write gold
# -----------------------------------------------------------------------------
(
    df_gold.write
    .mode("overwrite")
    .partitionBy("load_date")
    .format("parquet")
    .save(GOLD_PATH)
)

print("WRITE COMPLETED SUCCESSFULLY")

job.commit()