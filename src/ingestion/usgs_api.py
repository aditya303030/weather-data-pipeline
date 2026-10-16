import os
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

USGS_API_URL = os.getenv("USGS_API_URL")


def fetch_earthquakes():
    response = requests.get(USGS_API_URL)
    response.raise_for_status()
    return response.json()


def main():
    print("Fetching earthquake data from USGS...")
    data = fetch_earthquakes()
    earthquakes = data.get("features", [])
    print(f"Total earthquakes in the last 24 hours: {len(earthquakes)}")
    for eq in earthquakes[:10]:
        props = eq["properties"]
        print(f"  - {props['place']} | Magnitude: {props['mag']} | Time: {props['time']}")
    print("-" * 40)


if __name__ == "__main__":
    main()
