# Wistia Analytics Pipeline

Production-grade AWS data engineering project for ingesting, transforming, warehousing, and visualizing Wistia video analytics data.

## Architecture

```text
Wistia API
  ↓
AWS Lambda ingestion
  ↓
Amazon S3 Bronze
  ↓
AWS Glue Silver / Gold / Reporting
  ↓
AWS Step Functions orchestration
  ↓
Amazon Redshift Serverless
  ↓
QuickSight / Streamlit dashboard
```

## Services Used

- AWS Lambda for API ingestion and Redshift loading
- Amazon S3 for Bronze, Silver, Gold, and Reporting layers
- AWS Glue for Spark-based transformations
- AWS Step Functions for orchestration, retry, catch, and parallel execution
- Amazon DynamoDB for incremental load watermarks
- Amazon EventBridge Scheduler for daily pipeline execution
- Amazon Redshift Serverless for analytics warehouse
- Amazon QuickSight and Streamlit for BI visualization

## Implemented Pipeline Stages

### Lambda
- `wistia-lambda-metadata-api-extractor`
- `media_engagement_lambda_function`
- `wistia_redshift_loader_lambda`

### Glue Silver
- `wistia-bronze-to-silver-media-metadata`
- `wistia_silver_media_engagement`

### Glue Gold
- `dim_media_silver_to_gold`
- `wistia_gold_media_engagement`

### Reporting
- `gold_video_engagement_reporting`
- `wistia_report_campaign_performance`
- `wistia_report_video_performance_overview`
- `wistia_report_video_retention_curve`

## Dashboard Views

The Streamlit dashboard includes:

- Executive overview
- Campaign performance
- Video performance overview
- Video retention curve

## Incremental Loading

The pipeline uses a DynamoDB watermark table to track the last successful extraction timestamp per dataset. Watermarks are updated only after the full workflow succeeds to avoid skipped records during partial failures.

## Local Streamlit Setup

```bash
cd streamlit_app
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## Environment Variables

For Redshift connection:

```bash
export REDSHIFT_HOST="your-redshift-endpoint"
export REDSHIFT_PORT="5439"
export REDSHIFT_DATABASE="dev"
export REDSHIFT_USER="your_user"
export REDSHIFT_PASSWORD="your_password"
export REDSHIFT_SCHEMA="public"
```

If these are not set, the dashboard runs with sample data.

## Future Enhancements

- Add CI/CD deployment for Lambda, Glue scripts, Step Functions, and Streamlit
- Add data quality checks with row counts, null checks, and schema validation
- Add alerting via SNS or Slack
- Add Iceberg/Delta tables for lakehouse features
- Add ML-based engagement prediction
