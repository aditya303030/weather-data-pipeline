
import os
import json
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

USGS_API_URL = os.getenv("USGS_API_URL")


def fetch_earthquakes():
    response = requests.get(USGS_API_URL, timeout=30)
    response.raise_for_status()
    return response.json()


def main():
    print("Fetching earthquake data from USGS...")

    data = fetch_earthquakes()
    earthquakes = data.get("features", [])

    print(f"Total earthquakes: {len(earthquakes)}")
    print(f"Feed metadata: {data.get('metadata', {})}")

    # Print the complete response, including all features and properties
    print(json.dumps(data, indent=4))


if __name__ == "__main__":
    main()
