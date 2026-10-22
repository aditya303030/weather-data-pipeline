"""USGS Earthquake API client.

Exposes fetch_earthquakes() for pipeline use and main() for standalone execution.
"""
import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Default API URL (public USGS all_day feed) ──
DEFAULT_API_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


def fetch_earthquakes(api_url=None):
    """Fetch the complete USGS earthquake GeoJSON feed.

    Args:
        api_url: USGS API endpoint. Falls back to DEFAULT_API_URL if not provided.

    Returns:
        Tuple of (data, params) where data is the raw API response dict
        and params is an empty dict (USGS feed takes no query parameters).
    """
    url = api_url or DEFAULT_API_URL

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    from src.validation.ingestion import validate_response
    validate_response("earthquakes", data)

    return data, {}


def main():
    """Standalone execution: load .env, fetch feed, print results."""
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")
    api_url = os.getenv("USGS_API_URL", DEFAULT_API_URL)

    print("Fetching earthquake data from USGS...")
    data, _ = fetch_earthquakes(api_url=api_url)
    earthquakes = data.get("features", [])

    print(f"Total earthquakes: {len(earthquakes)}")
    print(f"Feed metadata: {data.get('metadata', {})}")
    print(json.dumps(data, indent=4))


if __name__ == "__main__":
    main()
