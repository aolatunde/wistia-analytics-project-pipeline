output "data_lake_bucket" {
  value = aws_s3_bucket.data_lake.bucket
}

output "artifact_bucket" {
  value = aws_s3_bucket.artifacts.bucket
}

output "watermark_table" {
  value = aws_dynamodb_table.watermarks.name
}

output "lambda_function_names" {
  value = keys(aws_lambda_function.this)
}

output "glue_job_names" {
  value = keys(aws_glue_job.this)
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.wistia.arn
}

output "eventbridge_schedule_name" {
  value = aws_scheduler_schedule.daily.name
}

output "redshift_workgroup_name" {
  value = var.create_redshift_serverless ? aws_redshiftserverless_workgroup.this[0].workgroup_name : var.redshift_workgroup_name
}
