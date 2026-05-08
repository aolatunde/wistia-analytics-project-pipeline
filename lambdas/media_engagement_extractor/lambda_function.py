import os
import json
import gzip
import logging
from datetime import datetime, timezone
from urllib.parse import quote
import urllib.request
import urllib.error

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

WISTIA_MEDIA_ENGAGEMENT_BASE_URL = "https://api.wistia.com/modern/stats/medias"


def get_secret(secret_name: str) -> dict:
    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret_string = response.get("SecretString")
    if not secret_string:
        raise ValueError(f"Secret {secret_name} does not contain SecretString")
    return json.loads(secret_string)


def get_api_token(secret_dict: dict) -> str:
    token = (
        secret_dict.get("api_token")
        or secret_dict.get("wistia_api_token")
        or secret_dict.get("token")
    )
    if not token:
        raise ValueError("Secret must contain one of: api_token, wistia_api_token, token")
    return token


def build_headers(api_token: str) -> dict:
    return {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json"
    }


def parse_iso8601(value: str) -> datetime:
    if not value:
        raise ValueError("Timestamp value is empty")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_iso8601(value: str) -> str:
    return parse_iso8601(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def list_s3_keys(bucket: str, prefix: str) -> list[str]:
    keys = []
    continuation_token = None

    while True:
        params = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**params)

        for obj in response.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                keys.append(key)

        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break

    return keys


def read_s3_text(bucket: str, key: str) -> str:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    raw = response["Body"].read()

    if key.endswith(".gz"):
        return gzip.decompress(raw).decode("utf-8")
    return raw.decode("utf-8")


def extract_media_ids_from_jsonl_text(text: str) -> list[str]:
    media_ids = []

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        try:
            row = json.loads(line)

            media_id = (
                row.get("raw_record", {}).get("media_id")
                or row.get("raw_record", {}).get("id")
                or row.get("media_id")
                or row.get("id")
            )

            if media_id is not None:
                media_ids.append(str(media_id))
        except Exception as e:
            logger.warning(f"Skipping malformed line: {e}")

    return media_ids


def get_unique_media_ids_from_bronze(
    bucket: str,
    source_prefix: str,
    max_source_files: int = 20
) -> list[str]:
    keys = list_s3_keys(bucket, source_prefix)
    logger.info(f"Found {len(keys)} source files under s3://{bucket}/{source_prefix}")

    if max_source_files > 0:
        keys = sorted(keys, reverse=True)[:max_source_files]

    media_ids = set()

    for key in keys:
        logger.info(f"Reading source file: s3://{bucket}/{key}")
        try:
            text = read_s3_text(bucket, key)
            extracted = extract_media_ids_from_jsonl_text(text)
            media_ids.update(extracted)
        except Exception as e:
            logger.warning(f"Failed reading/parsing s3://{bucket}/{key}: {e}")

    media_ids = sorted(media_ids)
    logger.info(f"Extracted {len(media_ids)} unique media ids")
    return media_ids


def fetch_media_engagement(media_id: str, headers: dict, timeout_seconds: int = 60) -> dict:
    encoded_media_id = quote(str(media_id), safe="")
    url = f"{WISTIA_MEDIA_ENGAGEMENT_BASE_URL}/{encoded_media_id}/engagement"

    req = urllib.request.Request(url, headers=headers, method="GET")

    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8")
        return json.loads(body)


def write_json_lines_to_s3(bucket: str, key: str, rows: list[dict]):
    body = "\n".join(json.dumps(row, default=str) for row in rows)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )
    logger.info(f"Wrote {len(rows)} records to s3://{bucket}/{key}")


def chunk_list(values: list, chunk_size: int) -> list[list]:
    return [values[i:i + chunk_size] for i in range(0, len(values), chunk_size)]


def get_engagement_record_timestamp(payload: dict, fallback_ts: str) -> str:
    """
    Try the most likely timestamp fields from the Wistia engagement payload.
    Falls back to ingestion timestamp if none are present.
    """
    candidate_fields = [
        "updated_at",
        "last_updated_at",
        "created_at",
        "created",
        "event_time",
        "timestamp"
    ]

    for field in candidate_fields:
        value = payload.get(field)
        if value:
            try:
                return normalize_iso8601(value)
            except Exception:
                logger.warning(f"Invalid timestamp in field '{field}': {value}")

    return fallback_ts


def lambda_handler(event, context):
    """
    Expected event from Step Functions:
    {
      "load_date": "2026-04-10",
      "last_success_watermark": "2026-04-09T01:00:00Z",
      "source_prefix": "bronze/wistia/media_metadata/",
      "bronze_prefix": "bronze/wistia/media_engagement",
      "max_source_files": 10,
      "max_media_ids": 100,
      "chunk_size": 25,
      "media_ids": ["abc123", "xyz789"]
    }
    """

    logger.info(f"Received event: {json.dumps(event)}")

    secret_name = os.environ["SECRET_NAME"]
    target_bucket = os.environ["TARGET_BUCKET"]
    default_source_prefix = os.environ.get("SOURCE_PREFIX", "bronze/wistia/media_metadata/")
    default_bronze_prefix = os.environ.get("BRONZE_PREFIX", "bronze/wistia/media_engagement")
    default_max_source_files = int(os.environ.get("MAX_SOURCE_FILES", "20"))
    default_max_media_ids = int(os.environ.get("MAX_MEDIA_IDS", "100"))
    default_chunk_size = int(os.environ.get("CHUNK_SIZE", "25"))

    source_prefix = event.get("source_prefix", default_source_prefix).strip("/") + "/"
    bronze_prefix = event.get("bronze_prefix", default_bronze_prefix).strip("/")
    max_source_files = int(event.get("max_source_files", default_max_source_files))
    max_media_ids = int(event.get("max_media_ids", default_max_media_ids))
    chunk_size = int(event.get("chunk_size", default_chunk_size))

    utc_now = datetime.now(timezone.utc)
    load_date = event.get("load_date") or utc_now.strftime("%Y-%m-%d")
    run_id = utc_now.strftime("%Y-%m-%dT%H-%M-%SZ")
    ingested_at = utc_now.isoformat().replace("+00:00", "Z")

    last_success_watermark = event.get("last_success_watermark", "1970-01-01T00:00:00Z")
    normalized_last_success_watermark = normalize_iso8601(last_success_watermark)
    watermark_dt = parse_iso8601(normalized_last_success_watermark)
    max_extracted_watermark = normalized_last_success_watermark

    try:
        secret_dict = get_secret(secret_name)
        api_token = get_api_token(secret_dict)
        headers = build_headers(api_token)

        media_ids = event.get("media_ids")
        if media_ids:
            logger.info(f"Using {len(media_ids)} media ids from event payload")
        else:
            media_ids = get_unique_media_ids_from_bronze(
                bucket=target_bucket,
                source_prefix=source_prefix,
                max_source_files=max_source_files
            )

        if not media_ids:
            result = {
                "status": "success",
                "dataset": "media_engagement",
                "message": "No media ids found to process",
                "requested_media_count": 0,
                "record_count": 0,
                "success_count": 0,
                "error_count": 0,
                "load_date": load_date,
                "run_id": run_id,
                "last_success_watermark": normalized_last_success_watermark,
                "max_extracted_watermark": normalized_last_success_watermark,
                "output_prefix": None
            }
            logger.info(json.dumps(result))
            return result

        media_ids = media_ids[:max_media_ids]
        logger.info(f"Processing {len(media_ids)} media ids")

        success_rows = []
        error_rows = []

        for media_id in media_ids:
            try:
                engagement_payload = fetch_media_engagement(media_id=media_id, headers=headers)
                record_ts = get_engagement_record_timestamp(engagement_payload, ingested_at)
                record_dt = parse_iso8601(record_ts)

                if record_dt <= watermark_dt:
                    logger.info(
                        f"Skipping media_id={media_id} because record timestamp "
                        f"{record_ts} is not newer than watermark {normalized_last_success_watermark}"
                    )
                    continue

                success_rows.append({
                    "load_date": load_date,
                    "run_id": run_id,
                    "ingested_at": ingested_at,
                    "record_timestamp": record_ts,
                    "source": "wistia_stats_media_engagement",
                    "media_id": str(media_id),
                    "raw_record": engagement_payload
                })

                if record_dt > parse_iso8601(max_extracted_watermark):
                    max_extracted_watermark = record_ts

            except urllib.error.HTTPError as e:
                error_body = ""
                try:
                    error_body = e.read().decode("utf-8")
                except Exception:
                    pass

                logger.warning(f"HTTPError for media_id={media_id}: {e.code} {error_body}")
                error_rows.append({
                    "load_date": load_date,
                    "run_id": run_id,
                    "ingested_at": ingested_at,
                    "source": "wistia_stats_media_engagement",
                    "media_id": str(media_id),
                    "error_type": "HTTPError",
                    "status_code": e.code,
                    "error_body": error_body
                })
            except Exception as e:
                logger.warning(f"Error for media_id={media_id}: {str(e)}")
                error_rows.append({
                    "load_date": load_date,
                    "run_id": run_id,
                    "ingested_at": ingested_at,
                    "source": "wistia_stats_media_engagement",
                    "media_id": str(media_id),
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                })

        output_prefix = f"s3://{target_bucket}/{bronze_prefix}/load_date={load_date}/run_id={run_id}/"

        if success_rows:
            success_chunks = chunk_list(success_rows, chunk_size)
            for idx, chunk in enumerate(success_chunks, start=1):
                output_key = (
                    f"{bronze_prefix}/load_date={load_date}/run_id={run_id}/success/"
                    f"media_engagement_part_{idx}_{run_id}.json"
                )
                write_json_lines_to_s3(target_bucket, output_key, chunk)
        else:
            logger.info("No incremental media engagement rows to write")

        if error_rows:
            error_chunks = chunk_list(error_rows, chunk_size)
            for idx, chunk in enumerate(error_chunks, start=1):
                error_key = (
                    f"{bronze_prefix}/load_date={load_date}/run_id={run_id}/errors/"
                    f"media_engagement_errors_part_{idx}_{run_id}.json"
                )
                write_json_lines_to_s3(target_bucket, error_key, chunk)

        result = {
            "status": "success",
            "dataset": "media_engagement",
            "message": "Media engagement ingestion completed",
            "requested_media_count": len(media_ids),
            "record_count": len(success_rows),
            "success_count": len(success_rows),
            "error_count": len(error_rows),
            "load_date": load_date,
            "run_id": run_id,
            "last_success_watermark": normalized_last_success_watermark,
            "max_extracted_watermark": max_extracted_watermark,
            "output_prefix": output_prefix
        }

        logger.info(json.dumps(result))
        return result

    except Exception as e:
        logger.exception("Media engagement ingestion failed")
        return {
            "status": "failed",
            "dataset": "media_engagement",
            "message": "Media engagement ingestion failed",
            "error": str(e),
            "load_date": load_date,
            "run_id": run_id,
            "last_success_watermark": normalized_last_success_watermark,
            "max_extracted_watermark": normalized_last_success_watermark
        }