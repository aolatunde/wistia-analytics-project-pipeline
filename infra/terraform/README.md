# Wistia Analytics Terraform Infrastructure

This Terraform package creates the AWS infrastructure for the Wistia Analytics project.

## Resources Created

- S3 data lake bucket
- S3 artifact bucket for Glue scripts
- Secrets Manager placeholder for Wistia API token
- DynamoDB watermark table seeded with `media_metadata` and `media_engagement`
- Lambda functions:
  - `wistia-lambda-metadata-api-extractor`
  - `media_engagement_lambda_function`
  - `wistia_redshift_loader_lambda`
- Glue jobs:
  - `wistia-bronze-to-silver-media-metadata`
  - `wistia_silver_media_engagement`
  - `dim_media_silver_to_gold`
  - `wistia_gold_media_engagement`
  - `gold_video_engagement_reporting`
  - `wistia_report_video_retention_curve`
  - `wistia_report_campaign_performance`
  - `wistia_report_video_performance_overview`
- Glue Catalog databases for bronze, silver, and gold
- Step Functions state machine using the final incremental orchestration JSON
- EventBridge Scheduler for daily execution
- Optional Redshift Serverless namespace/workgroup
- IAM roles and policies for Lambda, Glue, Step Functions, Scheduler, and optional Redshift

## Deploy

```bash
cd infra/terraform/envs/dev
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform fmt -recursive
terraform validate
terraform plan
terraform apply
```

## Important Notes

1. Scheduler is disabled by default. Enable it only after a successful manual Step Functions run.
2. Replace the placeholder Wistia secret value in Secrets Manager before running ingestion.
3. Glue scripts are included as deployable placeholders. Replace them with your final PySpark ETL code.
4. The Redshift loader Lambda contains a placeholder SQL statement. Replace it with your final COPY/MERGE logic.
