"""Fetch sports betting odds from The Odds API."""

from __future__ import annotations

import json
from typing import Any

import requests

from ingestion.config import ODDS_API_KEY

ODDS_API_BASE = "https://api.the-odds-api.com/v4"


def fetch_odds(
    sport: str,
    regions: str = "us",
    markets: str = "h2h",
) -> list[dict[str, Any]]:
    """Return odds JSON for the given sport key (e.g. basketball_nba)."""
    if not ODDS_API_KEY:
        raise ValueError("ODDS_API_KEY is not set")

    url = f"{ODDS_API_BASE}/sports/{sport}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": regions,
        "markets": markets,
        "oddsFormat": "decimal",
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def main() -> None:
    data = fetch_odds("basketball_nba")
    print(f"Games returned: {len(data)}")
    if data:
        print("Sample (first game):")
        print(json.dumps(data[0], indent=2))
    else:
        print("No games in response.")


if __name__ == "__main__":
    main()
