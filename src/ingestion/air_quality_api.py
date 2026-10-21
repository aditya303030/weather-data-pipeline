"""Air Quality API client for the Open-Meteo Air Quality API.

Exposes fetch_air_quality() for pipeline use and main() for standalone execution.
Variable definitions are the verified production set (aligned with save_raw_data notebook).
"""
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Default API URL (public Open-Meteo endpoint) ──
DEFAULT_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# ── Air Quality API variables (verified — European AQI sub-indices NOT supported) ──
AQ_CURRENT_VARS = ",".join([
    "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
    "sulphur_dioxide", "ozone", "aerosol_optical_depth", "dust",
    "uv_index", "uv_index_clear_sky", "ammonia",
    "alder_pollen", "birch_pollen", "grass_pollen",
    "mugwort_pollen", "olive_pollen", "ragweed_pollen",
    "european_aqi", "us_aqi",
    "us_aqi_pm2_5", "us_aqi_pm10", "us_aqi_ozone",
    "us_aqi_nitrogen_dioxide", "us_aqi_sulphur_dioxide", "us_aqi_carbon_monoxide",
])

AQ_HOURLY_VARS = AQ_CURRENT_VARS  # Same variables available hourly


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

    if not data or "latitude" not in data:
        raise ValueError("Empty or malformed air quality response")

    return data, params


def load_locations(spark=None):
    """Load air-quality ingestion locations from the Delta registry.

    Reads from ``weather_api.reference.location_registry`` where
    ``is_active = true`` and ``air_quality_enabled = true``.

    Raises RuntimeError if the registry is unavailable or empty.
    No YAML fallback — the registry is the sole source of truth.
    """
    if spark is None:
        raise RuntimeError("Spark session required to read from the location registry")
    from location_discovery import load_active_locations
    locations = load_active_locations(spark, source="air_quality")
    if not locations:
        raise RuntimeError(
            "No air-quality-enabled locations found in registry. "
            "Run initialize_registry_geonames() first."
        )
    return locations


def main():
    """Standalone execution: load .env, fetch all locations, print results."""
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")
    api_url = os.getenv("AIR_QUALITY_API_URL", DEFAULT_API_URL)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    locations = load_locations(spark=spark)
    for loc in locations:
        data, params = fetch_air_quality(loc["latitude"], loc["longitude"], api_url=api_url)
        print(data)
        print("-" * 40)


if __name__ == "__main__":
    main()
