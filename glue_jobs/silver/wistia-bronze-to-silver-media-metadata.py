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
        "bronze_path",
        "silver_path"
    ]
)

JOB_NAME = args["JOB_NAME"]
BRONZE_PATH = args["bronze_path"]
SILVER_PATH = args["silver_path"]


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
# Read bronze metadata
# -----------------------------------------------------------------------------
df_raw = (
    spark.read
    .option("multiLine", "false")
    .json(BRONZE_PATH)
    .withColumn("_source_file", F.input_file_name())
    .withColumn(
        "load_date",
        F.regexp_extract(
            F.col("_source_file"),
            r"load_date=([0-9]{4}-[0-9]{2}-[0-9]{2})",
            1
        )
    )
)

print(f"BRONZE_PATH = {BRONZE_PATH}")
print(f"SILVER_PATH = {SILVER_PATH}")
print("=== RAW SCHEMA ===")
df_raw.printSchema()

raw_count = df_raw.count()
print(f"RAW COUNT = {raw_count}")

df_raw.select(
    "media_hashed_id",
    "media_id",
    "name",
    "run_id",
    "load_date",
    "_source_file"
).show(10, truncate=False)


# -----------------------------------------------------------------------------
# Standardize and cast
# -----------------------------------------------------------------------------
df = (
    df_raw
    .withColumnRenamed("name", "media_name")
    .withColumnRenamed("type", "media_type")
    .withColumn("media_id", F.col("media_id").cast("string"))
    .withColumn("media_hashed_id", F.col("media_hashed_id").cast("string"))
    .withColumn("project_id", F.col("project_id").cast("string"))
    .withColumn("media_name", F.col("media_name").cast("string"))
    .withColumn("media_type", F.col("media_type").cast("string"))
    .withColumn("status", F.col("status").cast("string"))
    .withColumn("description", F.col("description").cast("string"))
    .withColumn("thumbnail_url", F.col("thumbnail_url").cast("string"))
    .withColumn("embed_url", F.col("embed_url").cast("string"))
    .withColumn("seo_description", F.col("seo_description").cast("string"))
    .withColumn("asset_count", F.col("asset_count").cast("int"))
    .withColumn("duration_seconds", F.col("duration").cast("double"))
    .withColumn("ingested_at_utc", F.to_timestamp("ingested_at_utc"))
    .withColumn("run_id", F.col("run_id").cast("string"))
    .withColumn("created_at_raw", F.col("created_at"))
    .withColumn("updated_at_raw", F.col("updated_at"))
)

# Handle timestamps whether they are ISO timestamps or unix-ish values
df = (
    df
    .withColumn(
        "created_at",
        F.coalesce(
            F.to_timestamp("created_at_raw"),
            F.to_timestamp(F.from_unixtime(F.col("created_at_raw").cast("bigint")))
        )
    )
    .withColumn(
        "updated_at",
        F.coalesce(
            F.to_timestamp("updated_at_raw"),
            F.to_timestamp(F.from_unixtime(F.col("updated_at_raw").cast("bigint")))
        )
    )
)

standardized_count = df.count()
print(f"STANDARDIZED COUNT = {standardized_count}")

df.select(
    "media_hashed_id",
    "media_id",
    "media_name",
    "media_type",
    "status",
    "load_date"
).show(10, truncate=False)


# -----------------------------------------------------------------------------
# Filter bad rows
# -----------------------------------------------------------------------------
df_filtered = df.filter(F.col("media_hashed_id").isNotNull())

filtered_count = df_filtered.count()
print(f"FILTERED COUNT = {filtered_count}")


# -----------------------------------------------------------------------------
# Deduplicate
# Keep most recent record per media_hashed_id
# -----------------------------------------------------------------------------
w = Window.partitionBy("media_hashed_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("ingested_at_utc").desc_nulls_last()
)

df_latest = (
    df_filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn", "duration", "created_at_raw", "updated_at_raw", "raw_payload", "_source_file")
)

latest_count = df_latest.count()
print(f"LATEST COUNT = {latest_count}")


# -----------------------------------------------------------------------------
# Final silver shape
# -----------------------------------------------------------------------------
df_silver = df_latest.select(
    "media_hashed_id",
    "media_id",
    "project_id",
    "media_name",
    "media_type",
    "status",
    "description",
    "duration_seconds",
    "created_at",
    "updated_at",
    "thumbnail_url",
    "embed_url",
    "seo_description",
    "asset_count",
    "run_id",
    "ingested_at_utc",
    "load_date"
)

silver_count = df_silver.count()
print(f"SILVER COUNT = {silver_count}")

null_load_date_count = df_silver.filter(F.col("load_date").isNull()).count()
print(f"NULL LOAD_DATE COUNT = {null_load_date_count}")

blank_load_date_count = df_silver.filter(F.col("load_date") == "").count()
print(f"BLANK LOAD_DATE COUNT = {blank_load_date_count}")

distinct_load_dates = [row["load_date"] for row in df_silver.select("load_date").distinct().collect()]
print(f"DISTINCT LOAD_DATES = {distinct_load_dates}")

df_silver.select(
    "media_hashed_id",
    "media_name",
    "media_type",
    "status",
    "load_date"
).show(10, truncate=False)

print(f"WRITING TO SILVER PATH = {SILVER_PATH}")


# -----------------------------------------------------------------------------
# Write silver
# -----------------------------------------------------------------------------
(
    df_silver.write
    .mode("overwrite")
    .partitionBy("load_date")
    .format("parquet")
    .save(SILVER_PATH)
)

print("WRITE COMPLETED SUCCESSFULLY")

job.commit()