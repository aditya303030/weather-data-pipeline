from src.contracts import WEATHER_CURRENT_VARS, WEATHER_HOURLY_VARS, WEATHER_DAILY_VARS
"""Weather API client for the Open-Meteo Weather API.

Exposes fetch_weather() for pipeline use and main() for standalone execution.
Variable definitions are the verified production set (aligned with save_raw_data notebook).
"""
import os

import requests

# ── Default API URL (public Open-Meteo endpoint) ──
DEFAULT_API_URL = "https://api.open-meteo.com/v1/forecast"

# ── Weather API variables (verified against Open-Meteo docs) ──




def fetch_weather(latitude, longitude, api_url=None):
    """Fetch comprehensive weather data for a single location.

    Args:
        latitude: Location latitude.
        longitude: Location longitude.
        api_url: Weather API endpoint. Falls back to DEFAULT_API_URL if not provided.

    Returns:
        Tuple of (data, params) where data is the raw API response dict
        and params is the request parameters dict.
    """
    url = api_url or DEFAULT_API_URL

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": WEATHER_CURRENT_VARS,
        "hourly": WEATHER_HOURLY_VARS,
        "daily": WEATHER_DAILY_VARS,
        "forecast_days": 1,
        "timezone": "UTC",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    from src.validation.ingestion import validate_response
    validate_response("weather", data)

    return data, params


def load_locations(spark=None, config=None):
    """Load weather ingestion locations from the Delta registry.

    Reads from ``weather_api.reference.location_registry`` where
    ``is_active = true`` and ``weather_enabled = true``.

    Raises RuntimeError if the registry is unavailable or empty.
    No YAML fallback — the registry is the sole source of truth.
    """
    if spark is None:
        raise RuntimeError("Spark session required to read from the location registry")
    from src.ingestion.location_discovery import load_active_locations
    locations = load_active_locations(spark, source="weather", config=config)
    if not locations:
        raise RuntimeError(
            "No weather-enabled locations found in registry. "
            "Run initialize_registry_geonames() first."
        )
    return locations


def main():
    """Standalone execution: use process environment, fetch all locations, print results."""
    api_url = os.getenv("WEATHER_API_URL", DEFAULT_API_URL)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    locations = load_locations(spark=spark)
    for loc in locations:
        data, params = fetch_weather(loc["latitude"], loc["longitude"], api_url=api_url)
        print(data)
        print("-" * 40)


if __name__ == "__main__":
    from src.ingestion.diagnostics import run_diagnostic
    run_diagnostic(main)
