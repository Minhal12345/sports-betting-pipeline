"""Upload raw odds JSON to S3 (Bronze layer)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ingestion.config import AWS_REGION, S3_BUCKET_NAME


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
    )


def upload_json(
    data: list[dict[str, Any]] | dict[str, Any],
    key_prefix: str = "bronze/odds",
    *,
    sport: str = "unknown",
) -> str:
    """Serialize data to JSON and upload to S3. Returns the object key."""
    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME is not set")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{key_prefix.rstrip('/')}/{sport}/{ts}.json"
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

    payload = fetch_odds("americanfootball_nfl")
    key = upload_json(payload, sport="americanfootball_nfl")
    print(f"Uploaded s3://{S3_BUCKET_NAME}/{key}")


if __name__ == "__main__":
    main()
