variable "aws_region" {
  description = "AWS region for the Wistia analytics project."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for naming resources."
  type        = string
  default     = "wistia-analytics"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
}

variable "data_lake_bucket_name" {
  description = "Globally unique S3 bucket name for the data lake. Leave empty to use generated name."
  type        = string
  default     = ""
}

variable "artifact_bucket_name" {
  description = "Globally unique S3 bucket name for Lambda/Glue artifacts. Leave empty to use generated name."
  type        = string
  default     = ""
}

variable "wistia_secret_name" {
  description = "Secrets Manager secret name containing the Wistia API token JSON, for example {\"api_token\":\"...\"}."
  type        = string
  default     = "wistia-api-secret"
}

variable "create_wistia_secret_placeholder" {
  description = "Create a placeholder Wistia secret. Set false if the secret already exists."
  type        = bool
  default     = true
}

variable "schedule_enabled" {
  description = "Enable EventBridge Scheduler for daily pipeline execution."
  type        = bool
  default     = false
}

variable "schedule_expression" {
  description = "EventBridge Scheduler cron/rate expression."
  type        = string
  default     = "cron(0 1 * * ? *)"
}

variable "schedule_timezone" {
  description = "Timezone for EventBridge Scheduler."
  type        = string
  default     = "America/Los_Angeles"
}

variable "create_redshift_serverless" {
  description = "Whether to create Redshift Serverless resources."
  type        = bool
  default     = false
}

variable "redshift_namespace_name" {
  description = "Redshift Serverless namespace name."
  type        = string
  default     = "wistia-analytics-dev"
}

variable "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name."
  type        = string
  default     = "wistia-analytics-dev-wg"
}

variable "redshift_database_name" {
  description = "Redshift database name used by the loader Lambda."
  type        = string
  default     = "dev"
}

variable "redshift_admin_username" {
  description = "Redshift admin username if creating Redshift Serverless."
  type        = string
  default     = "admin"
}

variable "redshift_admin_password" {
  description = "Redshift admin password if creating Redshift Serverless. Use tfvars or secrets manager; do not commit real values."
  type        = string
  default     = null
  sensitive   = true
}

variable "redshift_secret_arn" {
  description = "Existing Secrets Manager secret ARN for Redshift Data API credentials. Required if create_redshift_serverless=false and loader is used."
  type        = string
  default     = ""
}

variable "glue_worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of workers per Glue job."
  type        = number
  default     = 2
}

variable "lambda_timeout_seconds" {
  description = "Default Lambda timeout."
  type        = number
  default     = 900
}

variable "lambda_memory_mb" {
  description = "Default Lambda memory."
  type        = number
  default     = 512
}
