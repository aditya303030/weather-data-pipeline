"""Location Discovery — Open-Meteo Geocoding API integration.

Discovers geographic locations via the Open-Meteo Geocoding API and manages
a Delta-based location registry in Unity Catalog
(``weather_api.reference.location_registry``).

This module is intentionally separated from scheduled weather / air-quality
ingestion.  Run the ``init_registry`` source in *save_raw_data* (or call
``initialize_registry`` directly) to populate the registry.  Subsequent
ingestion runs read from the registry without calling the Geocoding API.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import yaml
from dotenv import load_dotenv

# ── Logging ──
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── Default API URL (public Open-Meteo Geocoding endpoint) ──
DEFAULT_GEOCODING_API_URL = "https://geocoding-api.open-meteo.com/v1/search"

# ── Delta table constants ──
REGISTRY_SCHEMA = "weather_api.reference"
REGISTRY_TABLE = "weather_api.reference.location_registry"

# ── US state name → postal-code mapping ──
US_STATE_CODES: dict[str, str] = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT",
    "Delaware": "DE", "District of Columbia": "DC", "Florida": "FL",
    "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
    "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY",
    "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
    "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
    "Puerto Rico": "PR", "Guam": "GU", "American Samoa": "AS",
    "U.S. Virgin Islands": "VI", "Northern Mariana Islands": "MP",
}


# ═════════════════════════════════════════════════════════════════════
#  ENVIRONMENT HELPERS
# ═════════════════════════════════════════════════════════════════════

def _load_env() -> str:
    """Return the Geocoding API URL from .env, falling back to default."""
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")
    return os.getenv("GEOCODING_API_URL", DEFAULT_GEOCODING_API_URL)


# ═════════════════════════════════════════════════════════════════════
#  GEOCODING API CLIENT
# ═════════════════════════════════════════════════════════════════════

def geocode_location(
    name: str,
    *,
    count: int = 10,
    country_code: str | None = None,
    api_url: str | None = None,
    timeout: int = 30,
    retries: int = 3,
    retry_delay: float = 1.0,
) -> list[dict[str, Any]]:
    """Search the Open-Meteo Geocoding API for locations matching *name*.

    Sends an HTTP GET with timeouts and retry handling.  Filters results
    by *country_code* if provided.

    Returns a list of raw API result dicts (empty if no matches).
    Raises ``requests.RequestException`` if all retries fail.
    """
    url = api_url or _load_env()
    params: dict[str, Any] = {
        "name": name,
        "count": count,
        "language": "en",
        "format": "json",
    }

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            if not results:
                logger.info("Geocoding '%s': no results returned", name)
                return []
            if country_code:
                filtered = [
                    r for r in results
                    if r.get("country_code", "").upper() == country_code.upper()
                ]
                if not filtered:
                    logger.info(
                        "Geocoding '%s': %d result(s) but none for country=%s",
                        name, len(results), country_code,
                    )
                results = filtered
            return results
        except requests.RequestException as exc:
            last_error = exc
            if attempt < retries:
                logger.warning(
                    "Geocoding '%s' attempt %d/%d failed: %s — retrying in %.1fs",
                    name, attempt, retries, exc, retry_delay,
                )
                time.sleep(retry_delay)
            else:
                logger.error("Geocoding '%s' failed after %d attempts: %s", name, retries, exc)
    assert last_error is not None
    raise last_error


# ═════════════════════════════════════════════════════════════════════
#  PARSING & VALIDATION
# ═════════════════════════════════════════════════════════════════════

def parse_location(result: dict[str, Any]) -> dict[str, Any]:
    """Parse one raw Geocoding API result into a structured location record.

    Validates coordinates and required fields (id, name).  Resolves US
    state codes from the ``admin1`` name via :data:`US_STATE_CODES`.

    Raises ``ValueError`` if required fields are missing or coordinates
    are out of range.
    """
    loc_id = result.get("id")
    loc_name = result.get("name")
    if loc_id is None or not loc_name:
        raise ValueError(f"Missing id or name in geocoding result: {result}")

    lat = result.get("latitude")
    lon = result.get("longitude")
    if lat is None or lon is None:
        raise ValueError(f"Missing coordinates for '{loc_name}' (id={loc_id})")
    if not (-90.0 <= lat <= 90.0):
        raise ValueError(f"Invalid latitude {lat} for '{loc_name}'")
    if not (-180.0 <= lon <= 180.0):
        raise ValueError(f"Invalid longitude {lon} for '{loc_name}'")

    state = result.get("admin1")
    country = result.get("country")
    country_code = result.get("country_code")
    state_code: str | None = None
    if country_code and country_code.upper() == "US" and state:
        state_code = US_STATE_CODES.get(state)

    return {
        "location_id": loc_id,
        "location_name": loc_name,
        "latitude": float(lat),
        "longitude": float(lon),
        "state": state,
        "state_code": state_code,
        "country": country,
        "country_code": country_code,
        "timezone": result.get("timezone"),
        "elevation": result.get("elevation"),
        "population": result.get("population"),
        "feature_code": result.get("feature_code"),
        "admin2": result.get("admin2"),
    }


# ═════════════════════════════════════════════════════════════════════
#  DISCOVERY
# ═════════════════════════════════════════════════════════════════════

def discover_locations(
    search_terms: list[str],
    *,
    country_code: str = "US",
    api_url: str | None = None,
) -> list[dict[str, Any]]:
    """Discover locations for a list of search terms.

    For each term, queries the Geocoding API, filters by *country_code*,
    parses results, and deduplicates by Open-Meteo location ID.

    Ambiguous names (e.g. multiple US cities named "Portland") are **not**
    silently collapsed — every matching result with a distinct
    ``location_id`` is returned.  The caller is responsible for approving
    or filtering results before registry insertion.

    Returns a list of parsed, deduplicated location records.
    """
    discovered: dict[int, dict[str, Any]] = {}  # keyed by location_id
    rejected: list[tuple[str, str]] = []

    for term in search_terms:
        try:
            raw_results = geocode_location(term, country_code=country_code, api_url=api_url)
        except requests.RequestException as exc:
            logger.error("API failure for '%s': %s — skipping", term, exc)
            rejected.append((term, f"API error: {exc}"))
            continue
        if not raw_results:
            rejected.append((term, f"No results for country={country_code}"))
            continue
        for raw in raw_results:
            try:
                parsed = parse_location(raw)
            except ValueError as exc:
                logger.warning("Rejecting result for '%s': %s", term, exc)
                rejected.append((term, str(exc)))
                continue
            loc_id = parsed["location_id"]
            if loc_id in discovered:
                logger.debug("Duplicate location_id %d for '%s' — already registered", loc_id, term)
                continue
            discovered[loc_id] = parsed
            logger.info(
                "Discovered: %s, %s (%.4f, %.4f) [id=%d]",
                parsed["location_name"],
                parsed.get("state") or parsed.get("country") or "?",
                parsed["latitude"], parsed["longitude"], loc_id,
            )

    if rejected:
        logger.info("Rejected %d search term(s): %s", len(rejected), rejected)
    logger.info("Discovery complete: %d unique locations from %d search terms", len(discovered), len(search_terms))
    return list(discovered.values())


# ═════════════════════════════════════════════════════════════════════
#  MIGRATION FROM YAML
# ═════════════════════════════════════════════════════════════════════

def _yaml_fallback(name: str, lat: float, lon: float) -> dict[str, Any]:
    """Create a fallback registry record from YAML coordinates (no geocoding)."""
    return {
        "location_id": None, "location_name": name,
        "latitude": lat, "longitude": lon,
        "state": None, "state_code": None,
        "country": "United States", "country_code": "US",
        "timezone": None, "elevation": None,
        "population": None, "feature_code": None, "admin2": None,
    }


def migrate_from_yaml(
    yaml_path: str | Path | None = None,
    *,
    country_code: str = "US",
    api_url: str | None = None,
) -> list[dict[str, Any]]:
    """Migrate existing locations from ``config/locations.yaml`` to registry format.

    Reads the YAML config, geocodes each city name, and disambiguates
    using the coordinates already in the YAML file (closest match by
    Manhattan distance).  This ensures existing city coverage is preserved
    and each city is matched to the correct geocoding identity.

    If geocoding fails for a city, a fallback record is created from the
    YAML coordinates with ``location_id = None``.
    """
    if yaml_path is None:
        project_root = Path(__file__).resolve().parents[2]
        yaml_path = project_root / "config" / "locations.yaml"
    yaml_path = Path(yaml_path)

    with open(yaml_path) as f:
        config = yaml.safe_load(f)

    yaml_locations = config["locations"]
    logger.info("Loaded %d locations from %s", len(yaml_locations), yaml_path)

    registry_records: list[dict[str, Any]] = []

    for loc in yaml_locations:
        name = loc["name"]
        yaml_lat = float(loc["latitude"])
        yaml_lon = float(loc["longitude"])

        try:
            raw_results = geocode_location(name, country_code=country_code, api_url=api_url)
        except requests.RequestException as exc:
            logger.error("Geocoding failed for '%s': %s — YAML fallback", name, exc)
            registry_records.append(_yaml_fallback(name, yaml_lat, yaml_lon))
            continue
        if not raw_results:
            logger.warning("No geocoding results for '%s' — YAML fallback", name)
            registry_records.append(_yaml_fallback(name, yaml_lat, yaml_lon))
            continue

        # Disambiguate: find closest result to YAML coordinates
        best_match: dict[str, Any] | None = None
        best_dist = float("inf")
        for raw in raw_results:
            try:
                parsed = parse_location(raw)
            except ValueError:
                continue
            dist = abs(parsed["latitude"] - yaml_lat) + abs(parsed["longitude"] - yaml_lon)
            if dist < best_dist:
                best_dist = dist
                best_match = parsed

        if best_match is None:
            logger.warning("All results for '%s' failed validation — YAML fallback", name)
            registry_records.append(_yaml_fallback(name, yaml_lat, yaml_lon))
            continue

        if best_dist > 1.0:
            logger.warning("Geocoded '%s' differs from YAML by %.4f deg — using geocoded result", name, best_dist)

        logger.info("Matched '%s' → %s, %s (id=%d, dist=%.4f deg)", name, best_match["location_name"], best_match.get("state") or "?", best_match["location_id"], best_dist)
        registry_records.append(best_match)

    logger.info("Migration: %d records ready for registry", len(registry_records))
    return registry_records


# ═════════════════════════════════════════════════════════════════════
#  REGISTRY MANAGEMENT  (requires a Spark session)
# ═════════════════════════════════════════════════════════════════════

def create_registry_table(spark) -> None:
    """Create the weather_api.reference schema and location_registry Delta table."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {REGISTRY_SCHEMA}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
            location_id          BIGINT,
            location_name        STRING NOT NULL,
            latitude             DOUBLE NOT NULL,
            longitude            DOUBLE NOT NULL,
            state                STRING,
            state_code           STRING,
            country              STRING,
            country_code         STRING,
            timezone             STRING,
            elevation            DOUBLE,
            population           BIGINT,
            feature_code         STRING,
            admin2               STRING,
            is_active            BOOLEAN NOT NULL,
            weather_enabled      BOOLEAN NOT NULL,
            air_quality_enabled  BOOLEAN NOT NULL,
            created_at           TIMESTAMP NOT NULL,
            updated_at           TIMESTAMP NOT NULL
        )
        USING DELTA
        COMMENT 'Location registry for weather and air-quality ingestion. Populated by location_discovery.py via Open-Meteo Geocoding API.'
    """)
    logger.info("Registry table ensured: %s", REGISTRY_TABLE)


def merge_locations(spark, locations: list[dict[str, Any]]) -> None:
    """Idempotently MERGE location records into the Delta registry.

    Rules:
    * New locations are inserted with is_active/weather_enabled/air_quality_enabled = true.
    * Existing locations are updated with refreshed metadata.
    * created_at is preserved on updates.
    * updated_at is refreshed on every update.
    * is_active, weather_enabled, air_quality_enabled are NOT reset on update.
    * Locations absent from input are NOT deleted.
    * Duplicate location_id values in input are deduplicated (last wins).
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        BooleanType, TimestampType, LongType,
    )
    from delta.tables import DeltaTable

    if not locations:
        logger.warning("No locations to merge — skipping")
        return

    create_registry_table(spark)

    # Deduplicate input by location_id (or by name+coords for fallbacks)
    seen: set = set()
    unique: list[dict[str, Any]] = []
    for loc in locations:
        key = loc.get("location_id")
        if key is None:
            key = ("__fallback__", loc["location_name"], loc["latitude"], loc["longitude"])
        if key not in seen:
            seen.add(key)
            unique.append(loc)
    if len(unique) < len(locations):
        logger.info("Removed %d duplicate(s) from input", len(locations) - len(unique))

    now = datetime.now(timezone.utc)
    normalized = []
    for loc in unique:
        normalized.append({
            "location_id": loc.get("location_id"),
            "location_name": loc["location_name"],
            "latitude": float(loc["latitude"]),
            "longitude": float(loc["longitude"]),
            "state": loc.get("state"),
            "state_code": loc.get("state_code"),
            "country": loc.get("country"),
            "country_code": loc.get("country_code"),
            "timezone": loc.get("timezone"),
            "elevation": loc.get("elevation"),
            "population": loc.get("population"),
            "feature_code": loc.get("feature_code"),
            "admin2": loc.get("admin2"),
            "is_active": loc.get("is_active", True),
            "weather_enabled": loc.get("weather_enabled", True),
            "air_quality_enabled": loc.get("air_quality_enabled", True),
            "created_at": now,
            "updated_at": now,
        })

    schema = StructType([
        StructField("location_id",          LongType(),     True),
        StructField("location_name",        StringType(),   False),
        StructField("latitude",             DoubleType(),   False),
        StructField("longitude",            DoubleType(),   False),
        StructField("state",                StringType(),   True),
        StructField("state_code",           StringType(),   True),
        StructField("country",              StringType(),   True),
        StructField("country_code",        StringType(),   True),
        StructField("timezone",             StringType(),   True),
        StructField("elevation",            DoubleType(),   True),
        StructField("population",           LongType(),     True),
        StructField("feature_code",         StringType(),   True),
        StructField("admin2",              StringType(),   True),
        StructField("is_active",            BooleanType(),  False),
        StructField("weather_enabled",      BooleanType(),  False),
        StructField("air_quality_enabled",  BooleanType(),  False),
        StructField("created_at",           TimestampType(), False),
        StructField("updated_at",           TimestampType(), False),
    ])

    source_df = spark.createDataFrame(normalized, schema=schema)
    target = DeltaTable.forName(spark, REGISTRY_TABLE)

    # Merge: match on location_id, or on name+coords when location_id is NULL
    merge_condition = (
        "(t.location_id IS NOT NULL AND s.location_id IS NOT NULL "
        "  AND t.location_id = s.location_id) "
        "OR "
        "(t.location_id IS NULL AND s.location_id IS NULL "
        "  AND t.location_name = s.location_name "
        "  AND t.latitude = s.latitude AND t.longitude = s.longitude)"
    )

    (
        target.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(set={
            "location_name":  "s.location_name",
            "latitude":       "s.latitude",
            "longitude":      "s.longitude",
            "state":           "s.state",
            "state_code":      "s.state_code",
            "country":         "s.country",
            "country_code":    "s.country_code",
            "timezone":        "s.timezone",
            "elevation":       "s.elevation",
            "population":      "s.population",
            "feature_code":    "s.feature_code",
            "admin2":          "s.admin2",
            "updated_at":      "s.updated_at",
        })
        .whenNotMatchedInsert(values={
            "location_id":          "s.location_id",
            "location_name":        "s.location_name",
            "latitude":             "s.latitude",
            "longitude":            "s.longitude",
            "state":                "s.state",
            "state_code":           "s.state_code",
            "country":              "s.country",
            "country_code":         "s.country_code",
            "timezone":             "s.timezone",
            "elevation":            "s.elevation",
            "population":           "s.population",
            "feature_code":         "s.feature_code",
            "admin2":               "s.admin2",
            "is_active":            "s.is_active",
            "weather_enabled":      "s.weather_enabled",
            "air_quality_enabled":  "s.air_quality_enabled",
            "created_at":           "s.created_at",
            "updated_at":           "s.updated_at",
        })
        .execute()
    )

    total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE}").collect()[0]["cnt"]
    logger.info("Registry merge complete. Total records: %d", total)


def initialize_registry(spark, yaml_path: str | Path | None = None, api_url: str | None = None) -> None:
    """Full one-shot initialization: create table + migrate from YAML + merge."""
    logger.info("=== Location Registry Initialization ===")
    create_registry_table(spark)
    locations = migrate_from_yaml(yaml_path=yaml_path, api_url=api_url)
    merge_locations(spark, locations)
    logger.info("=== Registry Initialization Complete ===")


def load_active_locations(spark, source: str = "all") -> list[dict[str, Any]]:
    """Load active locations from the registry for ingestion.

    Args:
        spark: Spark session.
        source: "weather", "air_quality", or "all". Determines enabled filter.

    Returns:
        List of dicts with keys: location_id, name, latitude, longitude,
        state, state_code, country, country_code, timezone.
        Returns [] if the table does not exist.
    """
    if not spark.catalog.tableExists(REGISTRY_TABLE):
        logger.error("Registry table %s does not exist — run init_registry first", REGISTRY_TABLE)
        return []

    query = f"SELECT * FROM {REGISTRY_TABLE} WHERE is_active = true"
    if source == "weather":
        query += " AND weather_enabled = true"
    elif source == "air_quality":
        query += " AND air_quality_enabled = true"

    rows = spark.sql(query).collect()
    locations = []
    for row in rows:
        locations.append({
            "location_id": row["location_id"],
            "name": row["location_name"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "state": row["state"],
            "state_code": row["state_code"],
            "country": row["country"],
            "country_code": row["country_code"],
            "timezone": row["timezone"],
        })
    logger.info("Loaded %d active locations from registry (source=%s)", len(locations), source)
    return locations