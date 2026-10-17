import os
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

AIR_QUALITY_API_URL = os.getenv("AIR_QUALITY_API_URL")
LOCATIONS_FILE = PROJECT_ROOT / "config" / "locations.yaml"


def load_locations():
    with open(LOCATIONS_FILE) as f:
        config = yaml.safe_load(f)
    return config["locations"]


def fetch_air_quality(latitude, longitude):
    hourly_variables = [
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "carbon_dioxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "aerosol_optical_depth",
        "dust",
        "uv_index",
        "uv_index_clear_sky",
        "ammonia",
        "methane",
        "formaldehyde",
        "glyoxal",
        "non_methane_volatile_organic_compounds",
        "pm10_wildfires",
        "peroxyacyl_nitrates",
        "secondary_inorganic_aerosol",
        "residential_elementary_carbon",
        "total_elementary_carbon",
        "pm2_5_total_organic_matter",
        "sea_salt_aerosol",
        "nitrogen_monoxide",
        "european_aqi",
        "european_aqi_pm2_5",
        "european_aqi_pm10",
        "european_aqi_nitrogen_dioxide",
        "european_aqi_ozone",
        "european_aqi_sulphur_dioxide",
        "us_aqi",
        "us_aqi_pm2_5",
        "us_aqi_pm10",
        "us_aqi_nitrogen_dioxide",
        "us_aqi_carbon_monoxide",
        "us_aqi_ozone",
        "us_aqi_sulphur_dioxide",
    ]

    current_variables = [
        "european_aqi",
        "us_aqi",
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "aerosol_optical_depth",
        "dust",
        "uv_index",
        "uv_index_clear_sky",
    ]

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(hourly_variables),
        "current": ",".join(current_variables),
        "forecast_days": 7,
        "timezone": "UTC",
    }

    response = requests.get(
        AIR_QUALITY_API_URL,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    return response.json()


def main():
    locations = load_locations()
    for loc in locations:
        print(f"Fetching air quality for {loc['name']}...")
        data = fetch_air_quality(loc["latitude"], loc["longitude"])
        print(data)
        print("-" * 40)


if __name__ == "__main__":
    main()
