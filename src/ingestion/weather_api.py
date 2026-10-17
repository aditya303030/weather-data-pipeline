import os
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

WEATHER_API_URL = os.getenv("WEATHER_API_URL")
LOCATIONS_FILE = PROJECT_ROOT / "config" / "locations.yaml"


def load_locations():
    with open(LOCATIONS_FILE) as f:
        config = yaml.safe_load(f)
    return config["locations"]


def fetch_weather(latitude, longitude):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": "UTC",

        "current": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "apparent_temperature,"
            "dew_point_2m,"
            "is_day,"
            "precipitation,"
            "rain,"
            "showers,"
            "snowfall,"
            "weather_code,"
            "cloud_cover,"
            "pressure_msl,"
            "surface_pressure,"
            "wind_speed_10m,"
            "wind_direction_10m,"
            "wind_gusts_10m"
        ),

        "hourly": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "apparent_temperature,"
            "dew_point_2m,"
            "precipitation_probability,"
            "precipitation,"
            "rain,"
            "showers,"
            "snowfall,"
            "weather_code,"
            "pressure_msl,"
            "surface_pressure,"
            "cloud_cover,"
            "cloud_cover_low,"
            "cloud_cover_mid,"
            "cloud_cover_high,"
            "visibility,"
            "wind_speed_10m,"
            "wind_direction_10m,"
            "wind_gusts_10m,"
            "uv_index,"
            "uv_index_clear_sky,"
            "is_day,"
            "sunshine_duration,"
            "shortwave_radiation,"
            "et0_fao_evapotranspiration,"
            "vapour_pressure_deficit,"
            "soil_temperature_0_to_7cm,"
            "soil_moisture_0_to_7cm"
        ),

        "daily": (
            "weather_code,"
            "temperature_2m_max,"
            "temperature_2m_min,"
            "apparent_temperature_max,"
            "apparent_temperature_min,"
            "sunrise,"
            "sunset,"
            "daylight_duration,"
            "sunshine_duration,"
            "uv_index_max,"
            "precipitation_sum,"
            "precipitation_hours,"
            "precipitation_probability_max,"
            "wind_speed_10m_max,"
            "wind_gusts_10m_max,"
            "wind_direction_10m_dominant,"
            "shortwave_radiation_sum,"
            "et0_fao_evapotranspiration"
        ),

        "forecast_days": 7,
    }

    response = requests.get(
        WEATHER_API_URL,
        params=params,
        timeout=30
    )
    response.raise_for_status()
    return response.json()


def main():
    locations = load_locations()
    for loc in locations:
        print(f"Fetching weather for {loc['name']}...")
        data = fetch_weather(loc["latitude"], loc["longitude"])
        print(data)
        print("-" * 40)


if __name__ == "__main__":
    main()
