data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

# -----------------------------------------------------------------------------
# S3 buckets
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  bucket = local.data_lake_bucket_name
}

resource "aws_s3_bucket" "artifacts" {
  bucket = local.artifact_bucket_name
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# Secrets Manager
# -----------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "wistia" {
  count       = var.create_wistia_secret_placeholder ? 1 : 0
  name        = var.wistia_secret_name
  description = "Wistia API token secret. Replace placeholder value after creation."
}

resource "aws_secretsmanager_secret_version" "wistia_placeholder" {
  count         = var.create_wistia_secret_placeholder ? 1 : 0
  secret_id     = aws_secretsmanager_secret.wistia[0].id
  secret_string = jsonencode({ api_token = "REPLACE_ME" })
}

resource "aws_secretsmanager_secret" "redshift_credentials" {
  count       = var.create_redshift_serverless ? 1 : 0
  name        = "${local.name_prefix}-redshift-credentials"
  description = "Redshift credentials for Wistia Redshift loader Lambda."
}

resource "aws_secretsmanager_secret_version" "redshift_credentials" {
  count     = var.create_redshift_serverless ? 1 : 0
  secret_id = aws_secretsmanager_secret.redshift_credentials[0].id
  secret_string = jsonencode({
    username = var.redshift_admin_username
    password = var.redshift_admin_password
  })
}

# -----------------------------------------------------------------------------
# DynamoDB Watermark Table
# -----------------------------------------------------------------------------
resource "aws_dynamodb_table" "watermarks" {
  name         = local.watermark_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "dataset"

  attribute {
    name = "dataset"
    type = "S"
  }

  point_in_time_recovery { enabled = true }

  server_side_encryption { enabled = true }
}

resource "aws_dynamodb_table_item" "media_metadata_seed" {
  table_name = aws_dynamodb_table.watermarks.name
  hash_key   = aws_dynamodb_table.watermarks.hash_key
  item = jsonencode({
    dataset                = { S = "media_metadata" }
    last_success_watermark = { S = "1970-01-01T00:00:00Z" }
    last_run_status        = { S = "SEEDED" }
    updated_at             = { S = "1970-01-01T00:00:00Z" }
  })
}

resource "aws_dynamodb_table_item" "media_engagement_seed" {
  table_name = aws_dynamodb_table.watermarks.name
  hash_key   = aws_dynamodb_table.watermarks.hash_key
  item = jsonencode({
    dataset                = { S = "media_engagement" }
    last_success_watermark = { S = "1970-01-01T00:00:00Z" }
    last_run_status        = { S = "SEEDED" }
    updated_at             = { S = "1970-01-01T00:00:00Z" }
  })
}

# -----------------------------------------------------------------------------
# CloudWatch log groups
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "lambda" {
  for_each          = local.lambda_functions
  name              = "/aws/lambda/${each.key}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/vendedlogs/states/${local.name_prefix}-state-machine"
  retention_in_days = 30
}

# -----------------------------------------------------------------------------
# IAM roles and policies
# -----------------------------------------------------------------------------
resource "aws_iam_role" "lambda_exec" {
  name = "${local.name_prefix}-lambda-exec-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_exec" {
  name = "${local.name_prefix}-lambda-policy"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*"]
      },
      {
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = concat(
          var.create_wistia_secret_placeholder ? [aws_secretsmanager_secret.wistia[0].arn] : ["arn:${data.aws_partition.current.partition}:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${var.wistia_secret_name}*"],
          local.redshift_secret_arn != "" ? [local.redshift_secret_arn] : []
        )
      },
      {
        Effect   = "Allow"
        Action   = ["redshift-data:ExecuteStatement", "redshift-data:DescribeStatement", "redshift-data:GetStatementResult"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "glue_exec" {
  name = "${local.name_prefix}-glue-exec-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_exec.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_exec" {
  name = "${local.name_prefix}-glue-policy"
  role = aws_iam_role.glue_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*",
          aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateDatabase", "glue:CreateTable", "glue:UpdateTable"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "sfn_exec" {
  name = "${local.name_prefix}-step-functions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_exec" {
  name = "${local.name_prefix}-step-functions-policy"
  role = aws_iam_role.sfn_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = [for f in aws_lambda_function.this : f.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:job/*"
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:UpdateItem"]
        Resource = aws_dynamodb_table.watermarks.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "scheduler_exec" {
  name = "${local.name_prefix}-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler_exec" {
  name = "${local.name_prefix}-scheduler-policy"
  role = aws_iam_role.scheduler_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = aws_sfn_state_machine.wistia.arn
    }]
  })
}

# -----------------------------------------------------------------------------
# Lambda functions
# -----------------------------------------------------------------------------
data "archive_file" "lambda_zip" {
  for_each    = local.lambda_functions
  type        = "zip"
  source_dir  = each.value.source_dir
  output_path = "${path.module}/.terraform/build/${each.key}.zip"
}

resource "aws_lambda_function" "this" {
  for_each         = local.lambda_functions
  function_name    = each.key
  description      = each.value.description
  role             = aws_iam_role.lambda_exec.arn
  handler          = each.value.handler
  runtime          = "python3.11"
  filename         = data.archive_file.lambda_zip[each.key].output_path
  source_code_hash = data.archive_file.lambda_zip[each.key].output_base64sha256
  timeout          = var.lambda_timeout_seconds
  memory_size      = var.lambda_memory_mb

  environment {
    variables = each.value.env
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
}

# -----------------------------------------------------------------------------
# Glue scripts and jobs
# -----------------------------------------------------------------------------
resource "aws_s3_object" "glue_script" {
  for_each = local.glue_jobs
  bucket   = aws_s3_bucket.artifacts.bucket
  key      = "glue-scripts/${each.value}"
  source   = "${path.module}/../../../../glue_jobs/${each.value}"
  etag     = filemd5("${path.module}/../../../../glue_jobs/${each.value}")
}

resource "aws_glue_job" "this" {
  for_each          = local.glue_jobs
  name              = each.key
  role_arn          = aws_iam_role.glue_exec.arn
  glue_version      = "5.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 480

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.glue_script[each.key].key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--DATA_LAKE_BUCKET"                 = aws_s3_bucket.data_lake.bucket
  }
}

# -----------------------------------------------------------------------------
# Glue Data Catalog databases
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "bronze" { name = "wistia_bronze_${var.environment}" }
resource "aws_glue_catalog_database" "silver" { name = "wistia_silver_${var.environment}" }
resource "aws_glue_catalog_database" "gold" { name = "wistia_gold_${var.environment}" }

# -----------------------------------------------------------------------------
# Step Functions state machine
# -----------------------------------------------------------------------------
resource "aws_sfn_state_machine" "wistia" {
  name     = "${local.name_prefix}-orchestration"
  role_arn = aws_iam_role.sfn_exec.arn
  type     = "STANDARD"

  definition = file("${path.module}/../../../../step_functions/wistia_state_machine.json")

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  depends_on = [
    aws_lambda_function.this,
    aws_glue_job.this,
    aws_dynamodb_table.watermarks,
    aws_iam_role_policy.sfn_exec
  ]
}

# -----------------------------------------------------------------------------
# EventBridge Scheduler
# -----------------------------------------------------------------------------
resource "aws_scheduler_schedule" "daily" {
  name       = "${local.name_prefix}-daily-pipeline"
  group_name = "default"
  state      = var.schedule_enabled ? "ENABLED" : "DISABLED"

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone

  flexible_time_window { mode = "OFF" }

  target {
    arn      = aws_sfn_state_machine.wistia.arn
    role_arn = aws_iam_role.scheduler_exec.arn
    input    = jsonencode({})

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }
}

# -----------------------------------------------------------------------------
# Optional Redshift Serverless
# -----------------------------------------------------------------------------
resource "aws_iam_role" "redshift_role" {
  count = var.create_redshift_serverless ? 1 : 0
  name  = "${local.name_prefix}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3" {
  count = var.create_redshift_serverless ? 1 : 0
  name  = "${local.name_prefix}-redshift-s3-policy"
  role  = aws_iam_role.redshift_role[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*"]
    }]
  })
}

resource "aws_redshiftserverless_namespace" "this" {
  count               = var.create_redshift_serverless ? 1 : 0
  namespace_name      = var.redshift_namespace_name
  db_name             = var.redshift_database_name
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_role[0].arn]
}

resource "aws_redshiftserverless_workgroup" "this" {
  count               = var.create_redshift_serverless ? 1 : 0
  workgroup_name      = var.redshift_workgroup_name
  namespace_name      = aws_redshiftserverless_namespace.this[0].namespace_name
  base_capacity       = 8
  publicly_accessible = false
}
