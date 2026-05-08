create schema if not exists marketing_analytics;

create table marketing_analytics.video_engagement_reporting (
    media_key varchar,
    media_id varchar,
    media_hashed_id varchar,
    media_name varchar,
    media_type varchar,
    status varchar,
    is_active boolean,
    duration_seconds double precision,
    project_id varchar,
    folder_id varchar,
    folder_hashed_id varchar,
    folder_name varchar,
    channel_key varchar,
    campaign_key varchar,
    media_created_date date,
    media_updated_date date,
    engagement_score double precision,
    timeline_points int,
    start_engagement_count bigint,
    max_engagement_count bigint,
    max_rewatch_count bigint,
    avg_engagement_count double precision,
    avg_rewatch_count double precision,
    engagement_dropoff_pct double precision,
    peak_rewatch_index int,
    peak_rewatch_count bigint,
    fact_run_id varchar,
    fact_ingested_at timestamp,
    dim_run_id varchar,
    dim_ingested_at_utc timestamp
);

create table marketing_analytics.video_retention_curve (
    media_id varchar,
    load_date date,
    timeline_index int,
    engagement_pct_of_start double precision,
    rewatch_pct_of_peak double precision
);

create table marketing_analytics.campaign_performance (
    campaign_key varchar,
    channel_key varchar,
    folder_name varchar,
    video_count bigint,
    avg_engagement_score double precision,
    avg_dropoff_pct double precision,
    total_rewatch_count bigint
);

drop table if exists marketing_analytics.video_retention_curve;

copy marketing_analytics.video_engagement_reporting
from 's3://ola-wistia-analytics-project-dev/gold/gold_video_engagement_reporting/'
iam_role 'arn:aws:iam::279586434333:role/wistiaredshiftrole'
format as parquet;

copy marketing_analytics.video_retention_curve
from 's3://ola-wistia-analytics-project-dev/report/report_video_retention_curve/'
iam_role 'arn:aws:iam::279586434333:role/wistiaredshiftrole'
format as parquet;

copy marketing_analytics.campaign_performance
from 's3://ola-wistia-analytics-project-dev/report/report_campaign_performance/'
iam_role 'arn:aws:iam::279586434333:role/wistiaredshiftrole'
format as parquet;


select count(*) from marketing_analytics.video_engagement_reporting;
select count(*) from marketing_analytics.video_retention_curve;
select count(*) from marketing_analytics.campaign_performance;

select * from marketing_analytics.video_engagement_reporting limit 10;

CREATE TABLE load_audit (
    table_name VARCHAR,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_loaded INT
);


select * from load_audit;