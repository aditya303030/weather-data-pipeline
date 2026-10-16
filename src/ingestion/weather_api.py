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
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
    }
    response = requests.get(WEATHER_API_URL, params=params)
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
