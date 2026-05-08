# Wistia Analytics Pipeline Project Report

## Summary
This project implements a serverless AWS data pipeline for Wistia video analytics. It ingests media metadata and engagement data, stores raw data in S3, transforms it through Silver, Gold, and Reporting layers with Glue, orchestrates the full workflow with Step Functions, tracks incremental state in DynamoDB, loads curated datasets into Redshift, and presents insights through QuickSight and Streamlit.

## Key Services
- Lambda: API extraction and Redshift loader
- S3: Bronze/Silver/Gold/Reporting storage
- Glue: ETL transformations
- Step Functions: workflow orchestration
- DynamoDB: incremental watermarks
- EventBridge: daily scheduling
- Redshift: analytics warehouse
- QuickSight/Streamlit: dashboards

## Challenges
- Preserving Step Functions state across Lambda and Parallel states
- Designing safe incremental loading for API-based data
- Preventing skipped data when downstream stages fail
- Handling API response variability

## Future Improvements
- CI/CD deployment automation
- Data quality checks
- SNS/Slack alerting
- Glue Data Catalog / Athena support
- Iceberg or Delta tables for lakehouse features
