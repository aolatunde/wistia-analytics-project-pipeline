# Infrastructure Notes

The Terraform code provisions the Wistia pipeline core infrastructure.

## Key Resources

- S3 data lake bucket
- DynamoDB watermark table
- Lambda functions
- Glue jobs
- Step Functions orchestration
- EventBridge Scheduler
- Optional Redshift Serverless

## Deployment Safety

The EventBridge schedule is disabled by default through `schedule_enabled = false`. Enable it only after validating a manual Step Functions execution.

## Glue Scripts

Terraform expects Glue scripts to be uploaded to:

```text
s3://<data-bucket>/scripts/glue/silver/
s3://<data-bucket>/scripts/glue/gold/
s3://<data-bucket>/scripts/glue/reporting/
```

You can upload them manually first, then automate uploads through CI/CD.
