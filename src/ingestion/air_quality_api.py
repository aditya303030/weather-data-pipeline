from src.contracts import AQ_CURRENT_VARS, AQ_HOURLY_VARS
"""Air Quality API client for the Open-Meteo Air Quality API.

Exposes fetch_air_quality() for pipeline use and main() for standalone execution.
Variable definitions are the verified production set (aligned with save_raw_data notebook).
"""
import os

import requests

# ── Default API URL (public Open-Meteo endpoint) ──
DEFAULT_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# ── Air Quality API variables (verified — European AQI sub-indices NOT supported) ──



def fetch_air_quality(latitude, longitude, api_url=None):
    """Fetch comprehensive air quality data for a single location.

    Args:
        latitude: Location latitude.
        longitude: Location longitude.
        api_url: Air Quality API endpoint. Falls back to DEFAULT_API_URL if not provided.

    Returns:
        Tuple of (data, params) where data is the raw API response dict
        and params is the request parameters dict.
    """
    url = api_url or DEFAULT_API_URL

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": AQ_CURRENT_VARS,
        "hourly": AQ_HOURLY_VARS,
        "timezone": "UTC",
        "forecast_days": 1,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    from src.validation.ingestion import validate_response
    validate_response("air_quality", data)

    return data, params


def load_locations(spark=None, config=None):
    """Load air-quality ingestion locations from the Delta registry.

    Reads from ``weather_api.reference.location_registry`` where
    ``is_active = true`` and ``air_quality_enabled = true``.

    Raises RuntimeError if the registry is unavailable or empty.
    No YAML fallback — the registry is the sole source of truth.
    """
    if spark is None:
        raise RuntimeError("Spark session required to read from the location registry")
    from src.ingestion.location_discovery import load_active_locations
    locations = load_active_locations(spark, source="air_quality", config=config)
    if not locations:
        raise RuntimeError(
            "No air-quality-enabled locations found in registry. "
            "Run initialize_registry_geonames() first."
        )
    return locations


def main():
    """Standalone execution: use process environment, fetch all locations, print results."""
    api_url = os.getenv("AIR_QUALITY_API_URL", DEFAULT_API_URL)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    locations = load_locations(spark=spark)
    for loc in locations:
        data, params = fetch_air_quality(loc["latitude"], loc["longitude"], api_url=api_url)
        print(data)
        print("-" * 40)


if __name__ == "__main__":
    from src.ingestion.diagnostics import run_diagnostic
    run_diagnostic(main)
