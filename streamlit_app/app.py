import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

try:
    import psycopg2
except Exception:
    psycopg2 = None

load_dotenv()

st.set_page_config(
    page_title="Wistia Analytics Dashboard",
    layout="wide"
)

# -----------------------------
# Configuration
# -----------------------------
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")

REDSHIFT_CONFIGURED = all([
    REDSHIFT_HOST,
    REDSHIFT_DATABASE,
    REDSHIFT_USER,
    REDSHIFT_PASSWORD,
    psycopg2 is not None
])


# -----------------------------
# Sample Data Fallback
# -----------------------------
def build_sample_data():
    np.random.seed(42)
    dates = pd.date_range(datetime.today() - timedelta(days=29), periods=30, freq="D")
    videos = [
        "Product Demo",
        "Customer Story",
        "Feature Launch",
        "Webinar Replay",
        "Pricing Explainer"
    ]
    campaigns = ["Facebook", "YouTube", "Email", "LinkedIn"]

    rows = []
    for d in dates:
        for video in videos:
            campaign = np.random.choice(campaigns)
            loads = np.random.randint(300, 1800)
            plays = int(loads * np.random.uniform(0.25, 0.75))
            engagement = np.random.uniform(0.35, 0.9)
            dropoff = np.random.uniform(0.10, 0.65)
            rows.append({
                "report_date": d.date(),
                "media_id": video.lower().replace(" ", "_"),
                "media_name": video,
                "campaign_name": campaign,
                "load_count": loads,
                "play_count": plays,
                "play_rate": plays / loads,
                "engagement_score": engagement,
                "dropoff_pct": dropoff,
                "hours_watched": np.random.uniform(8, 90),
                "unique_visitors": np.random.randint(80, 900),
                "rewatch_count": np.random.randint(5, 150)
            })

    performance = pd.DataFrame(rows)

    retention_rows = []
    for video in videos:
        base = np.random.uniform(0.85, 0.98)
        for pct in range(0, 101, 5):
            retention = max(0.05, base * np.exp(-pct / np.random.uniform(75, 120)) + np.random.normal(0, 0.015))
            retention_rows.append({
                "media_name": video,
                "video_pct": pct,
                "retention_pct": min(1.0, retention)
            })
    retention = pd.DataFrame(retention_rows)

    pipeline_runs = pd.DataFrame([
        {
            "run_date": (datetime.today() - timedelta(days=i)).date(),
            "status": "SUCCEEDED" if i not in [5, 13] else "FAILED",
            "duration_minutes": np.random.randint(12, 45),
            "records_ingested": np.random.randint(100, 1000),
            "load_date": (datetime.today() - timedelta(days=i)).strftime("%Y-%m-%d")
        }
        for i in range(20)
    ])

    return performance, retention, pipeline_runs


# -----------------------------
# Redshift Helpers
# -----------------------------
def get_redshift_connection():
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )


@st.cache_data(ttl=600)
def query_redshift(sql: str) -> pd.DataFrame:
    if not REDSHIFT_CONFIGURED:
        raise RuntimeError("Redshift is not configured. Using sample data.")
    with get_redshift_connection() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=600)
def load_data():
    if not REDSHIFT_CONFIGURED:
        return build_sample_data(), "sample"

    try:
        performance_sql = f"""
            SELECT
                report_date,
                media_id,
                media_name,
                campaign_name,
                load_count,
                play_count,
                play_rate,
                engagement_score,
                dropoff_pct,
                hours_watched,
                unique_visitors,
                rewatch_count
            FROM {REDSHIFT_SCHEMA}.wistia_report_video_performance_overview
        """

        campaign_sql = f"""
            SELECT
                report_date,
                media_id,
                media_name,
                campaign_name,
                load_count,
                play_count,
                play_rate,
                engagement_score,
                dropoff_pct,
                hours_watched,
                unique_visitors,
                rewatch_count
            FROM {REDSHIFT_SCHEMA}.wistia_report_campaign_performance
        """

        retention_sql = f"""
            SELECT
                media_name,
                video_pct,
                retention_pct
            FROM {REDSHIFT_SCHEMA}.wistia_report_video_retention_curve
        """

        performance = query_redshift(performance_sql)
        campaign = query_redshift(campaign_sql)
        retention = query_redshift(retention_sql)

        # Blend performance/campaign if both exist; otherwise use performance.
        if campaign is not None and not campaign.empty:
            performance = campaign

        pipeline_runs = pd.DataFrame()
        return (performance, retention, pipeline_runs), "redshift"

    except Exception as exc:
        st.warning(f"Could not query Redshift. Using sample data. Details: {exc}")
        return build_sample_data(), "sample"


# -----------------------------
# UI Helpers
# -----------------------------
def metric_card(label, value, help_text=None):
    st.metric(label=label, value=value, help=help_text)


def format_pct(value):
    if pd.isna(value):
        return "0.0%"
    return f"{value * 100:.1f}%"


# -----------------------------
# App
# -----------------------------
st.title("📹 Wistia Analytics Dashboard")
st.caption("Serverless AWS analytics pipeline: Lambda → S3 → Glue → Step Functions → Redshift → Streamlit")

(data, source) = load_data()
performance_df, retention_df, pipeline_runs_df = data

if performance_df.empty:
    st.error("No performance data available.")
    st.stop()

performance_df["report_date"] = pd.to_datetime(performance_df["report_date"])

with st.sidebar:
    st.header("Filters")
    st.caption(f"Data source: **{source.upper()}**")

    min_date = performance_df["report_date"].min().date()
    max_date = performance_df["report_date"].max().date()
    selected_dates = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        start_date, end_date = min_date, max_date

    campaigns = sorted(performance_df["campaign_name"].dropna().unique()) if "campaign_name" in performance_df.columns else []
    selected_campaigns = st.multiselect("Campaign", campaigns, default=campaigns)

    videos = sorted(performance_df["media_name"].dropna().unique())
    selected_videos = st.multiselect("Video", videos, default=videos)

filtered = performance_df[
    (performance_df["report_date"].dt.date >= start_date) &
    (performance_df["report_date"].dt.date <= end_date) &
    (performance_df["media_name"].isin(selected_videos))
]

if selected_campaigns and "campaign_name" in filtered.columns:
    filtered = filtered[filtered["campaign_name"].isin(selected_campaigns)]

if filtered.empty:
    st.warning("No data for the selected filters.")
    st.stop()

# KPI Row
total_loads = int(filtered["load_count"].sum()) if "load_count" in filtered.columns else 0
total_plays = int(filtered["play_count"].sum()) if "play_count" in filtered.columns else 0
avg_play_rate = total_plays / total_loads if total_loads else 0
avg_engagement = filtered["engagement_score"].mean() if "engagement_score" in filtered.columns else 0
avg_dropoff = filtered["dropoff_pct"].mean() if "dropoff_pct" in filtered.columns else 0
hours_watched = filtered["hours_watched"].sum() if "hours_watched" in filtered.columns else 0

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    metric_card("Loads", f"{total_loads:,}")
with col2:
    metric_card("Plays", f"{total_plays:,}")
with col3:
    metric_card("Play Rate", format_pct(avg_play_rate))
with col4:
    metric_card("Engagement", format_pct(avg_engagement))
with col5:
    metric_card("Drop-off", format_pct(avg_dropoff))

st.divider()

tab_overview, tab_campaign, tab_video, tab_retention = st.tabs([
    "Executive Overview",
    "Campaign Performance",
    "Video Performance",
    "Retention Curve"
])

with tab_overview:
    st.subheader("Executive Overview")
    daily = filtered.groupby("report_date", as_index=False).agg({
        "load_count": "sum",
        "play_count": "sum",
        "hours_watched": "sum",
        "engagement_score": "mean",
        "dropoff_pct": "mean"
    })

    fig = px.line(
        daily,
        x="report_date",
        y=["play_count", "load_count"],
        markers=True,
        title="Loads and Plays Over Time"
    )
    st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        fig_eng = px.line(
            daily,
            x="report_date",
            y="engagement_score",
            markers=True,
            title="Average Engagement Score Over Time"
        )
        fig_eng.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig_eng, use_container_width=True)
    with c2:
        fig_drop = px.line(
            daily,
            x="report_date",
            y="dropoff_pct",
            markers=True,
            title="Average Drop-off Over Time"
        )
        fig_drop.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig_drop, use_container_width=True)

with tab_campaign:
    st.subheader("Campaign Performance")
    if "campaign_name" not in filtered.columns:
        st.info("campaign_name column not available in this dataset.")
    else:
        campaign_summary = filtered.groupby("campaign_name", as_index=False).agg({
            "load_count": "sum",
            "play_count": "sum",
            "hours_watched": "sum",
            "unique_visitors": "sum",
            "engagement_score": "mean",
            "dropoff_pct": "mean"
        })
        campaign_summary["play_rate"] = campaign_summary["play_count"] / campaign_summary["load_count"]

        fig = px.bar(
            campaign_summary.sort_values("play_count", ascending=False),
            x="campaign_name",
            y="play_count",
            title="Plays by Campaign",
            text_auto=True
        )
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.scatter(
            campaign_summary,
            x="play_rate",
            y="engagement_score",
            size="hours_watched",
            hover_name="campaign_name",
            title="Campaign Quality: Play Rate vs Engagement"
        )
        fig2.update_xaxes(tickformat=".0%")
        fig2.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig2, use_container_width=True)

        st.dataframe(campaign_summary, use_container_width=True)

with tab_video:
    st.subheader("Video Performance Overview")
    video_summary = filtered.groupby("media_name", as_index=False).agg({
        "load_count": "sum",
        "play_count": "sum",
        "hours_watched": "sum",
        "unique_visitors": "sum",
        "engagement_score": "mean",
        "dropoff_pct": "mean",
        "rewatch_count": "sum"
    })
    video_summary["play_rate"] = video_summary["play_count"] / video_summary["load_count"]

    c1, c2 = st.columns(2)
    with c1:
        fig = px.bar(
            video_summary.sort_values("engagement_score", ascending=False),
            x="media_name",
            y="engagement_score",
            title="Engagement Score by Video",
            text_auto=".1%"
        )
        fig.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig2 = px.bar(
            video_summary.sort_values("dropoff_pct", ascending=False),
            x="media_name",
            y="dropoff_pct",
            title="Drop-off by Video",
            text_auto=".1%"
        )
        fig2.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig2, use_container_width=True)

    fig3 = px.scatter(
        video_summary,
        x="play_rate",
        y="engagement_score",
        size="unique_visitors",
        hover_name="media_name",
        title="Video Quality: Play Rate vs Engagement"
    )
    fig3.update_xaxes(tickformat=".0%")
    fig3.update_yaxes(tickformat=".0%")
    st.plotly_chart(fig3, use_container_width=True)

    st.dataframe(video_summary, use_container_width=True)

with tab_retention:
    st.subheader("Video Retention Curve")
    selected_retention_videos = st.multiselect(
        "Select videos for retention curve",
        sorted(retention_df["media_name"].dropna().unique()) if not retention_df.empty else [],
        default=selected_videos[:3]
    )

    retention_filtered = retention_df[retention_df["media_name"].isin(selected_retention_videos)]
    if retention_filtered.empty:
        st.info("No retention data available for selected videos.")
    else:
        fig = px.line(
            retention_filtered,
            x="video_pct",
            y="retention_pct",
            color="media_name",
            markers=True,
            title="Audience Retention by Video Position"
        )
        fig.update_xaxes(title="Video Progress (%)")
        fig.update_yaxes(title="Retention (%)", tickformat=".0%")
        st.plotly_chart(fig, use_container_width=True)
