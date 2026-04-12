"""Read Bronze NBA odds JSON from S3 and write flattened Silver Parquet."""

from __future__ import annotations

import io
import json
from typing import Any

import pandas as pd

from ingestion.config import S3_BUCKET_NAME
from ingestion.upload_to_s3 import get_s3_client

BRONZE_PREFIX = "bronze/odds/basketball_nba/"
SILVER_PREFIX = "silver/odds/basketball_nba/"


def bronze_to_silver_key(bronze_key: str) -> str | None:
    """Map a bronze JSON key to the matching silver Parquet key."""
    if not bronze_key.startswith(BRONZE_PREFIX) or not bronze_key.endswith(".json"):
        return None
    rel = bronze_key[len(BRONZE_PREFIX) :]
    return f"{SILVER_PREFIX}{rel[:-5]}.parquet"


def snapshot_time_from_bronze_key(bronze_key: str) -> str:
    """Return the snapshot id from the object key (basename without .json)."""
    base = bronze_key.rsplit("/", 1)[-1]
    if base.endswith(".json"):
        return base[: -len(".json")]
    return base


def list_keys_with_suffix(client: Any, *, bucket: str, prefix: str, suffix: str) -> set[str]:
    keys: set[str] = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if key.endswith(suffix):
                keys.add(key)
    return keys


SILVER_COLUMNS = [
    "game_id",
    "sport_key",
    "sport_title",
    "commence_time",
    "home_team",
    "away_team",
    "bookmaker_key",
    "bookmaker_title",
    "market_key",
    "team",
    "odds",
    "snapshot_time",
]


def flatten_odds_json(payload: Any, snapshot_time: str) -> list[dict[str, Any]]:
    """Turn Odds API JSON into one row per outcome."""
    if isinstance(payload, dict):
        games = [payload]
    elif isinstance(payload, list):
        games = payload
    else:
        return []

    rows: list[dict[str, Any]] = []
    for game in games:
        if not isinstance(game, dict):
            continue
        game_id = game.get("id", "")
        sport_key = game.get("sport_key", "")
        sport_title = game.get("sport_title", "")
        commence_time = game.get("commence_time", "")
        home_team = game.get("home_team", "")
        away_team = game.get("away_team", "")

        for bookmaker in game.get("bookmakers") or []:
            if not isinstance(bookmaker, dict):
                continue
            bookmaker_key = bookmaker.get("key", "")
            bookmaker_title = bookmaker.get("title", "")
            for market in bookmaker.get("markets") or []:
                if not isinstance(market, dict):
                    continue
                market_key = market.get("key", "")
                for outcome in market.get("outcomes") or []:
                    if not isinstance(outcome, dict):
                        continue
                    team = outcome.get("name", "")
                    odds = outcome.get("price")
                    rows.append(
                        {
                            "game_id": game_id,
                            "sport_key": sport_key,
                            "sport_title": sport_title,
                            "commence_time": commence_time,
                            "home_team": home_team,
                            "away_team": away_team,
                            "bookmaker_key": bookmaker_key,
                            "bookmaker_title": bookmaker_title,
                            "market_key": market_key,
                            "team": team,
                            "odds": odds,
                            "snapshot_time": snapshot_time,
                        }
                    )
    return rows


def process_bronze_object(
    client: Any,
    *,
    bucket: str,
    bronze_key: str,
    silver_key: str,
    snapshot_time: str,
) -> None:
    obj = client.get_object(Bucket=bucket, Key=bronze_key)
    body = obj["Body"].read()
    payload = json.loads(body.decode("utf-8"))
    rows = flatten_odds_json(payload, snapshot_time)
    df = pd.DataFrame(rows, columns=SILVER_COLUMNS)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    client.put_object(
        Bucket=bucket,
        Key=silver_key,
        Body=buf.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )


def run_transform() -> int:
    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME is not set")

    client = get_s3_client()
    bucket = S3_BUCKET_NAME

    bronze_keys = list_keys_with_suffix(client, bucket=bucket, prefix=BRONZE_PREFIX, suffix=".json")
    silver_keys = list_keys_with_suffix(client, bucket=bucket, prefix=SILVER_PREFIX, suffix=".parquet")

    to_process: list[str] = []
    for bk in sorted(bronze_keys):
        sk = bronze_to_silver_key(bk)
        if sk is None:
            continue
        if sk not in silver_keys:
            to_process.append(bk)

    processed = 0
    for bronze_key in to_process:
        silver_key = bronze_to_silver_key(bronze_key)
        assert silver_key is not None
        snapshot_time = snapshot_time_from_bronze_key(bronze_key)
        process_bronze_object(
            client,
            bucket=bucket,
            bronze_key=bronze_key,
            silver_key=silver_key,
            snapshot_time=snapshot_time,
        )
        processed += 1

    return processed


def main() -> None:
    n = run_transform()
    print(f"Summary: processed {n} bronze JSON file(s) into silver Parquet.")


if __name__ == "__main__":
    main()
