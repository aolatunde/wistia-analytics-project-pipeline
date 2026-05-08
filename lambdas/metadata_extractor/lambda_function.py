import json
import logging
import os
import random
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import boto3


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

# -----------------------------------------------------------------------------
# Environment
# -----------------------------------------------------------------------------
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SECRET_NAME = os.environ["SECRET_NAME"]
BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "bronze/wistia").strip("/")
WISTIA_BASE_URL = os.getenv("WISTIA_BASE_URL", "https://api.wistia.com").rstrip("/")
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
ONLY_VIDEO = os.getenv("ONLY_VIDEO", "true").lower() == "true"

DATASET_NAME = "media_metadata"
PER_PAGE = 100
MAX_RETRIES = 6

# -----------------------------------------------------------------------------
# AWS clients
# -----------------------------------------------------------------------------
session = boto3.session.Session(region_name=AWS_REGION)
secrets_client = session.client("secretsmanager")
s3_client = session.client("s3")

# -----------------------------------------------------------------------------
# Simple in-memory cache for secret during warm Lambda invocations
# -----------------------------------------------------------------------------
_SECRET_CACHE: Optional[str] = None


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso8601(value: str) -> datetime:
    if not value:
        raise ValueError("Timestamp value is empty")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_iso8601(value: str) -> str:
    return parse_iso8601(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def get_secret_token() -> str:
    """
    Reads the Wistia API token from Secrets Manager.
    Expected secret format:
    {
      "api_token": "xxxxx"
    }
    """
    global _SECRET_CACHE
    if _SECRET_CACHE:
        return _SECRET_CACHE

    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)

    secret_str = response.get("SecretString")
    if not secret_str:
        raise ValueError(f"Secret {SECRET_NAME} does not contain SecretString")

    secret_json = json.loads(secret_str)
    token = secret_json.get("api_token")
    if not token:
        raise ValueError(f"Secret {SECRET_NAME} missing 'api_token' field")

    _SECRET_CACHE = token
    return token


def build_url(endpoint_path: str, query_params: Optional[Dict[str, Any]] = None) -> str:
    endpoint_path = endpoint_path if endpoint_path.startswith("/") else f"/{endpoint_path}"
    url = f"{WISTIA_BASE_URL}{endpoint_path}"

    if query_params:
        cleaned = {}
        for k, v in query_params.items():
            if v is None:
                continue
            cleaned[k] = v
        if cleaned:
            url = f"{url}?{urlencode(cleaned, doseq=True)}"

    return url


def parse_retry_after_seconds(http_error: HTTPError) -> int:
    retry_after = None
    if hasattr(http_error, "headers") and http_error.headers:
        retry_after = http_error.headers.get("Retry-After")

    if retry_after:
        try:
            return max(1, int(retry_after))
        except Exception:
            pass

    return 5


def backoff_sleep_seconds(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    return min((base * (2 ** (attempt - 1))) + random.uniform(0, 1.0), cap)


def http_get_json_with_retries(url: str, token: str) -> Any:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "ola-wistia-metadata-extractor/1.0"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        req = Request(url=url, headers=headers, method="GET")

        try:
            with urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
                body = resp.read().decode("utf-8")
                status_code = resp.getcode()

                if 200 <= status_code < 300:
                    return json.loads(body)

                raise RuntimeError(f"Non-2xx response from Wistia: {status_code}, body={body[:1000]}")

        except HTTPError as e:
            status_code = getattr(e, "code", None)
            error_body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""

            if status_code == 429 and attempt < MAX_RETRIES:
                sleep_s = parse_retry_after_seconds(e) + random.uniform(0, 1.0)
                logger.warning(json.dumps({
                    "event": "wistia_rate_limited",
                    "status_code": status_code,
                    "attempt": attempt,
                    "sleep_seconds": round(sleep_s, 2),
                    "url": url
                }))
                time.sleep(sleep_s)
                continue

            if status_code in (500, 502, 503, 504) and attempt < MAX_RETRIES:
                sleep_s = backoff_sleep_seconds(attempt)
                logger.warning(json.dumps({
                    "event": "wistia_transient_http_error",
                    "status_code": status_code,
                    "attempt": attempt,
                    "sleep_seconds": round(sleep_s, 2),
                    "url": url,
                    "body": error_body[:500]
                }))
                time.sleep(sleep_s)
                continue

            raise RuntimeError(
                f"HTTPError calling Wistia: status={status_code}, body={error_body[:1000]}"
            ) from e

        except URLError as e:
            if attempt < MAX_RETRIES:
                sleep_s = backoff_sleep_seconds(attempt)
                logger.warning(json.dumps({
                    "event": "wistia_url_error",
                    "attempt": attempt,
                    "sleep_seconds": round(sleep_s, 2),
                    "url": url,
                    "error": str(e)
                }))
                time.sleep(sleep_s)
                continue

            raise RuntimeError(f"URLError calling Wistia: {str(e)}") from e

        except Exception as e:
            if attempt < MAX_RETRIES:
                sleep_s = backoff_sleep_seconds(attempt)
                logger.warning(json.dumps({
                    "event": "wistia_unexpected_retryable_error",
                    "attempt": attempt,
                    "sleep_seconds": round(sleep_s, 2),
                    "url": url,
                    "error": str(e)
                }))
                time.sleep(sleep_s)
                continue

            raise RuntimeError(f"Unexpected error calling Wistia: {str(e)}") from e

    raise RuntimeError(f"Exceeded max retries for url={url}")


def extract_media_rows(response_json: Any) -> List[Dict[str, Any]]:
    if isinstance(response_json, list):
        return response_json

    if isinstance(response_json, dict):
        if isinstance(response_json.get("medias"), list):
            return response_json["medias"]
        if isinstance(response_json.get("results"), list):
            return response_json["results"]

    raise ValueError(f"Unexpected Wistia media response type: {type(response_json).__name__}")


def get_media_record_timestamp(raw: Dict[str, Any], fallback_ts: str) -> str:
    """
    Prefer updated timestamp for incremental logic; fall back to created timestamp.
    """
    candidate_fields = ["updated", "created"]

    for field in candidate_fields:
        value = raw.get(field)
        if value:
            try:
                return normalize_iso8601(value)
            except Exception:
                logger.warning(f"Invalid timestamp in field '{field}': {value}")

    return fallback_ts


def normalize_media_record(
    raw: Dict[str, Any],
    run_id: str,
    ingested_at_utc: str,
    record_timestamp: str
) -> Dict[str, Any]:
    assets = raw.get("assets") if isinstance(raw.get("assets"), list) else []

    thumbnail_url = None
    asset_count = len(assets)

    for asset in assets:
        if not isinstance(asset, dict):
            continue
        asset_type = str(asset.get("type") or "").lower()
        if "still" in asset_type or "thumbnail" in asset_type:
            thumbnail_url = asset.get("url")
            break

    return {
        "media_id": raw.get("id"),
        "media_hashed_id": raw.get("hashed_id"),
        "project_id": raw.get("project_id"),
        "name": raw.get("name"),
        "type": raw.get("type"),
        "status": raw.get("status"),
        "description": raw.get("description"),
        "created_at": raw.get("created"),
        "updated_at": raw.get("updated"),
        "record_timestamp": record_timestamp,
        "duration": raw.get("duration"),
        "thumbnail_url": thumbnail_url,
        "embed_url": raw.get("embed_url"),
        "seo_description": raw.get("seo_description"),
        "asset_count": asset_count,
        "run_id": run_id,
        "ingested_at_utc": ingested_at_utc,
        "raw_payload": raw
    }


def fetch_all_media(token: str) -> List[Dict[str, Any]]:
    endpoint_path = "/modern/medias"
    page = 1
    all_rows: List[Dict[str, Any]] = []

    while True:
        query_params = {
            "page": page,
            "per_page": PER_PAGE
        }

        if ONLY_VIDEO:
            query_params["type"] = "Video"

        request_url = build_url(endpoint_path, query_params)

        logger.info(json.dumps({
            "event": "wistia_metadata_page_started",
            "endpoint_path": endpoint_path,
            "page": page,
            "request_url": request_url
        }))

        started_at = time.time()
        response_json = http_get_json_with_retries(request_url, token)
        elapsed_ms = int((time.time() - started_at) * 1000)

        page_rows = extract_media_rows(response_json)
        all_rows.extend(page_rows)

        logger.info(json.dumps({
            "event": "wistia_metadata_page_succeeded",
            "endpoint_path": endpoint_path,
            "page": page,
            "row_count": len(page_rows),
            "elapsed_ms": elapsed_ms,
            "cumulative_row_count": len(all_rows)
        }))

        if not page_rows or len(page_rows) < PER_PAGE:
            break

        page += 1

    return all_rows


def build_s3_key(run_id: str, load_date: str) -> str:
    safe_run_id = run_id.replace(":", "-")

    return (
        f"{BRONZE_PREFIX}/{DATASET_NAME}/"
        f"load_date={load_date}/"
        f"run_id={safe_run_id}/"
        f"{DATASET_NAME}.ndjson"
    )


def write_ndjson_to_s3(records: List[Dict[str, Any]], run_id: str, load_date: str) -> str:
    s3_key = build_s3_key(run_id, load_date)
    body = "\n".join(json.dumps(record) for record in records) + "\n"

    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson"
    )

    return s3_key


# -----------------------------------------------------------------------------
# Lambda entry point
# -----------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Expected event from Step Functions:
    {
      "load_date": "2026-04-10",
      "last_success_watermark": "2026-04-09T01:00:00Z"
    }
    """
    run_id = utc_now_iso()
    ingested_at_utc = utc_now_iso()
    load_date = event.get("load_date") or utc_now().strftime("%Y-%m-%d")
    last_success_watermark = event.get("last_success_watermark", "1970-01-01T00:00:00Z")
    normalized_last_success_watermark = normalize_iso8601(last_success_watermark)
    watermark_dt = parse_iso8601(normalized_last_success_watermark)
    max_extracted_watermark = normalized_last_success_watermark

    logger.info(json.dumps({
        "event": "metadata_lambda_started",
        "run_id": run_id,
        "load_date": load_date,
        "last_success_watermark": normalized_last_success_watermark,
        "aws_request_id": getattr(context, "aws_request_id", None),
        "only_video": ONLY_VIDEO
    }))

    try:
        token = get_secret_token()
        raw_rows = fetch_all_media(token)

        incremental_rows: List[Dict[str, Any]] = []

        for row in raw_rows:
            record_timestamp = get_media_record_timestamp(row, ingested_at_utc)
            record_dt = parse_iso8601(record_timestamp)

            if record_dt <= watermark_dt:
                continue

            incremental_rows.append(
                normalize_media_record(
                    raw=row,
                    run_id=run_id,
                    ingested_at_utc=ingested_at_utc,
                    record_timestamp=record_timestamp
                )
            )

            if record_dt > parse_iso8601(max_extracted_watermark):
                max_extracted_watermark = record_timestamp

        s3_key = None
        if incremental_rows:
            s3_key = write_ndjson_to_s3(incremental_rows, run_id, load_date)
        else:
            logger.info(json.dumps({
                "event": "metadata_lambda_no_incremental_rows",
                "run_id": run_id,
                "load_date": load_date
            }))

        result = {
            "status": "success",
            "dataset": DATASET_NAME,
            "record_count": len(incremental_rows),
            "s3_bucket": BRONZE_BUCKET,
            "s3_key": s3_key,
            "run_id": run_id,
            "load_date": load_date,
            "last_success_watermark": normalized_last_success_watermark,
            "max_extracted_watermark": max_extracted_watermark,
            "aws_request_id": getattr(context, "aws_request_id", None)
        }

        logger.info(json.dumps({
            "event": "metadata_lambda_succeeded",
            **result
        }))

        return result

    except Exception as exc:
        logger.error(json.dumps({
            "event": "metadata_lambda_failed",
            "run_id": run_id,
            "load_date": load_date,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "aws_request_id": getattr(context, "aws_request_id", None)
        }))
        raise