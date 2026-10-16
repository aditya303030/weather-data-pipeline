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
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone",
    }
    response = requests.get(AIR_QUALITY_API_URL, params=params)
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
