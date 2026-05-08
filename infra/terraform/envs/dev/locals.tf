locals {
  name_prefix = "${var.project_name}-${var.environment}"

  data_lake_bucket_name = var.data_lake_bucket_name != "" ? var.data_lake_bucket_name : "${local.name_prefix}-data-lake-${data.aws_caller_identity.current.account_id}"
  artifact_bucket_name  = var.artifact_bucket_name != "" ? var.artifact_bucket_name : "${local.name_prefix}-artifacts-${data.aws_caller_identity.current.account_id}"

  watermark_table_name = "wistia_pipeline_watermarks"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  lambda_functions = {
    "wistia-lambda-metadata-api-extractor" = {
      source_dir  = "${path.module}/../../../../lambdas/metadata_extractor"
      handler     = "lambda_function.lambda_handler"
      description = "Extracts Wistia media metadata to S3 Bronze."
      env = {
        SECRET_NAME   = var.wistia_secret_name
        BRONZE_BUCKET = aws_s3_bucket.data_lake.bucket
        BRONZE_PREFIX = "bronze/wistia"
        LOG_LEVEL     = "INFO"
        ONLY_VIDEO    = "true"
      }
    }
    "media_engagement_lambda_function" = {
      source_dir  = "${path.module}/../../../../lambdas/media_engagement_extractor"
      handler     = "lambda_function.lambda_handler"
      description = "Extracts Wistia media engagement data to S3 Bronze."
      env = {
        SECRET_NAME      = var.wistia_secret_name
        TARGET_BUCKET    = aws_s3_bucket.data_lake.bucket
        SOURCE_PREFIX    = "bronze/wistia/media_metadata/"
        BRONZE_PREFIX    = "bronze/wistia/media_engagement"
        MAX_SOURCE_FILES = "20"
        MAX_MEDIA_IDS    = "100"
        CHUNK_SIZE       = "25"
      }
    }
    "wistia_redshift_loader_lambda" = {
      source_dir  = "${path.module}/../../../../lambdas/redshift_loader"
      handler     = "lambda_function.lambda_handler"
      description = "Loads final Wistia reporting data into Redshift."
      env = {
        REDSHIFT_WORKGROUP_NAME = var.create_redshift_serverless ? aws_redshiftserverless_workgroup.this[0].workgroup_name : var.redshift_workgroup_name
        REDSHIFT_DATABASE       = var.redshift_database_name
        REDSHIFT_SECRET_ARN     = local.redshift_secret_arn
      }
    }
  }

  glue_jobs = {
    "wistia-bronze-to-silver-media-metadata"   = "silver/wistia-bronze-to-silver-media-metadata.py"
    "wistia_silver_media_engagement"           = "silver/wistia_silver_media_engagement.py"
    "dim_media_silver_to_gold"                 = "gold/dim_media_silver_to_gold.py"
    "wistia_gold_media_engagement"             = "gold/wistia_gold_media_engagement.py"
    "gold_video_engagement_reporting"          = "reporting/gold_video_engagement_reporting.py"
    "wistia_report_video_retention_curve"      = "reporting/wistia_report_video_retention_curve.py"
    "wistia_report_campaign_performance"       = "reporting/wistia_report_campaign_performance.py"
    "wistia_report_video_performance_overview" = "reporting/wistia_report_video_performance_overview.py"
  }

  redshift_secret_arn = var.create_redshift_serverless ? aws_secretsmanager_secret.redshift_credentials[0].arn : var.redshift_secret_arn
}
