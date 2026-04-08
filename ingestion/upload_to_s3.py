"""Upload raw odds JSON to S3 (Bronze layer)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ingestion.config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    S3_BUCKET_NAME,
)


def get_s3_client():
    kwargs: dict[str, str] = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    return boto3.client("s3", **kwargs)


def upload_json(
    data: list[dict[str, Any]] | dict[str, Any],
    *,
    sport: str = "basketball_nba",
) -> str:
    """Serialize data to JSON and upload to S3. Returns the object key."""
    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME is not set")

    now = datetime.now(timezone.utc)
    date_part = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    key = f"bronze/odds/{sport}/{date_part}/{timestamp}.json"
    body = json.dumps(data, indent=2).encode("utf-8")

    client = get_s3_client()
    try:
        client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
    except ClientError as e:
        raise RuntimeError(f"S3 upload failed: {e}") from e

    return key


def main() -> None:
    from ingestion.fetch_odds import fetch_odds

    payload = fetch_odds("basketball_nba")
    key = upload_json(payload, sport="basketball_nba")
    print(f"Success: uploaded raw NBA odds to s3://{S3_BUCKET_NAME}/{key}")


if __name__ == "__main__":
    main()
