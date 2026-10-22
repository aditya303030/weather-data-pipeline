"""Weather API client for the Open-Meteo Weather API.

Exposes fetch_weather() for pipeline use and main() for standalone execution.
Variable definitions are the verified production set (aligned with save_raw_data notebook).
"""
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Default API URL (public Open-Meteo endpoint) ──
DEFAULT_API_URL = "https://api.open-meteo.com/v1/forecast"

# ── Weather API variables (verified against Open-Meteo docs) ──
WEATHER_CURRENT_VARS = ",".join([
    "temperature_2m", "relative_humidity_2m", "apparent_temperature",
    "is_day", "precipitation", "rain", "snowfall", "weather_code",
    "cloud_cover", "pressure_msl", "surface_pressure",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
])

WEATHER_HOURLY_VARS = ",".join([
    "temperature_2m", "relative_humidity_2m", "dew_point_2m",
    "apparent_temperature", "precipitation", "rain", "snowfall",
    "snow_depth", "weather_code", "pressure_msl", "surface_pressure",
    "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
    "visibility", "wind_speed_10m", "wind_speed_80m",
    "wind_direction_10m", "wind_direction_80m", "wind_gusts_10m",
    "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm", "soil_temperature_54cm",
    "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm",
    "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm",
    "uv_index", "uv_index_clear_sky", "is_day", "sunshine_duration",
    "wet_bulb_temperature_2m", "vapour_pressure_deficit",
    "shortwave_radiation", "direct_radiation", "diffuse_radiation",
    "direct_normal_irradiance", "terrestrial_radiation",
])

WEATHER_DAILY_VARS = ",".join([
    "weather_code", "temperature_2m_max", "temperature_2m_min",
    "apparent_temperature_max", "apparent_temperature_min",
    "sunrise", "sunset", "daylight_duration", "sunshine_duration",
    "uv_index_max", "precipitation_sum", "rain_sum", "snowfall_sum",
    "precipitation_hours", "precipitation_probability_max",
    "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
    "shortwave_radiation_sum", "et0_fao_evapotranspiration",
])


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


def load_locations(spark=None):
    """Load weather ingestion locations from the Delta registry.

    Reads from ``weather_api.reference.location_registry`` where
    ``is_active = true`` and ``weather_enabled = true``.

    Raises RuntimeError if the registry is unavailable or empty.
    No YAML fallback — the registry is the sole source of truth.
    """
    if spark is None:
        raise RuntimeError("Spark session required to read from the location registry")
    from src.ingestion.location_discovery import load_active_locations
    locations = load_active_locations(spark, source="weather")
    if not locations:
        raise RuntimeError(
            "No weather-enabled locations found in registry. "
            "Run initialize_registry_geonames() first."
        )
    return locations


def main():
    """Standalone execution: load .env, fetch all locations, print results."""
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")
    api_url = os.getenv("WEATHER_API_URL", DEFAULT_API_URL)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    locations = load_locations(spark=spark)
    for loc in locations:
        data, params = fetch_weather(loc["latitude"], loc["longitude"], api_url=api_url)
        print(data)
        print("-" * 40)


if __name__ == "__main__":
    main()
