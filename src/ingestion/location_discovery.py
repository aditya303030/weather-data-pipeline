"""Location Discovery — GeoNames-based location registry management.

Manages a Delta-based location registry in Unity Catalog
(``weather_api.reference.location_registry``) populated from the
GeoNames US.txt dataset at
``/Volumes/weather_api/reference/location_files/US.txt``.

Registry initialization is an explicit, separate operation — call
``initialize_registry_geonames(spark)`` to (re)populate the registry.
Normal ingestion runs read active locations via ``load_active_locations``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

# ── Logging ──
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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

# Reverse mapping: 2-letter state code -> full state name (for GeoNames admin1_code)
US_CODE_TO_STATE: dict[str, str] = {v: k for k, v in US_STATE_CODES.items()}


# ═════════════════════════════════════════════════════════════════════
#  REGISTRY MANAGEMENT  (requires a Spark session)
# ═════════════════════════════════════════════════════════════════════

def create_registry_table(spark) -> None:
    """Create the weather_api.reference schema and location_registry Delta table."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {REGISTRY_SCHEMA}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
            location_id          BIGINT,
            geonames_id           BIGINT,
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
            location_source      STRING,
            modification_date    STRING,
            is_active            BOOLEAN NOT NULL,
            weather_enabled      BOOLEAN NOT NULL,
            air_quality_enabled  BOOLEAN NOT NULL,
            created_at           TIMESTAMP NOT NULL,
            updated_at           TIMESTAMP NOT NULL
        )
        USING DELTA
        COMMENT 'Location registry for weather and air-quality ingestion. Populated from GeoNames US.txt dataset.'
    """)
    # Add new columns if table already existed with old schema
    existing_cols = [r["col_name"] for r in spark.sql(f"DESCRIBE TABLE {REGISTRY_TABLE}").collect()]
    for col_name, col_type in [
        ("geonames_id", "BIGINT"),
        ("location_source", "STRING"),
        ("modification_date", "STRING"),
    ]:
        if col_name not in existing_cols:
            spark.sql(f"ALTER TABLE {REGISTRY_TABLE} ADD COLUMN {col_name} {col_type}")
    logger.info("Registry table ensured: %s", REGISTRY_TABLE)


def merge_locations(spark, locations: list[dict[str, Any]]) -> None:
    """Idempotently MERGE location records into the Delta registry.

    Rules:
    * New locations are inserted with is_active=true but weather_enabled/air_quality_enabled=false by default.
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
            "weather_enabled": loc.get("weather_enabled", False),
            "air_quality_enabled": loc.get("air_quality_enabled", False),
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
        rd = row.asDict()
        locations.append({
            "location_id": rd.get("location_id"),
            "geonames_id": rd.get("geonames_id"),
            "location_source": rd.get("location_source"),
            "name": rd.get("location_name"),
            "latitude": rd.get("latitude"),
            "longitude": rd.get("longitude"),
            "state": rd.get("state"),
            "state_code": rd.get("state_code"),
            "country": rd.get("country"),
            "country_code": rd.get("country_code"),
            "timezone": rd.get("timezone"),
        })
    logger.info("Loaded %d active locations from registry (source=%s)", len(locations), source)
    return locations


def enable_locations_for_ingestion(
    spark,
    *,
    location_ids: list[int] | None = None,
    limit: int | None = None,
    weather: bool = True,
    air_quality: bool = True,
    distributed: bool = False,
) -> dict:
    """Enable specific locations for weather and/or air-quality ingestion.

    By default, all imported GeoNames locations have weather_enabled=false
    and air_quality_enabled=false to prevent accidental mass API requests.
    This function selectively enables locations.

    Args:
        spark: Spark session.
        location_ids: Specific GeoNames IDs to enable.  If None, uses *limit*.
        limit: If location_ids is None, enable up to this many locations.
            If distributed=True, selects one per state until limit reached.
        weather: Set weather_enabled=true for selected locations.
        air_quality: Set air_quality_enabled=true for selected locations.
        distributed: If True with limit, select geographically distributed
            locations (one per state, prioritising higher population).

    Returns:
        Dict with counts of enabled locations.
    """
    if not spark.catalog.tableExists(REGISTRY_TABLE):
        raise RuntimeError(f"Registry table {REGISTRY_TABLE} does not exist")

    if location_ids is not None:
        id_list = ",".join(str(i) for i in location_ids)
        where_clause = f"location_id IN ({id_list})"
    elif limit is not None:
        if distributed:
            # Select one highest-population location per state, up to limit
            where_clause = (
                "location_id IN ("
                "  SELECT location_id FROM ("
                "    SELECT location_id, state, population,"
                "      ROW_NUMBER() OVER (PARTITION BY state ORDER BY population DESC NULLS LAST) AS rn"
                f"    FROM {REGISTRY_TABLE} WHERE is_active = true"
                "  ) WHERE rn = 1"
                f"  LIMIT {limit}"
                ")"
            )
        else:
            where_clause = f"is_active = true ORDER BY population DESC NULLS LAST LIMIT {limit}"
    else:
        raise ValueError("Either location_ids or limit must be provided")

    set_clauses = []
    if weather:
        set_clauses.append("weather_enabled = true")
    if air_quality:
        set_clauses.append("air_quality_enabled = true")
    set_clauses.append("updated_at = current_timestamp()")

    update_sql = (
        f"UPDATE {REGISTRY_TABLE} SET {', '.join(set_clauses)} WHERE {where_clause}"
    )
    spark.sql(update_sql)

    # Report counts
    weather_cnt = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE} WHERE weather_enabled = true"
    ).collect()[0]["cnt"]
    aq_cnt = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE} WHERE air_quality_enabled = true"
    ).collect()[0]["cnt"]
    total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE}").collect()[0]["cnt"]

    result = {
        "total_registry": total,
        "weather_enabled": weather_cnt,
        "air_quality_enabled": aq_cnt,
    }
    logger.info("Activation update: %s", result)
    return result


# ====================================================================
#  GEONAMES DATASET PROCESSING
# ====================================================================
# The GeoNames US.txt file is the authoritative source for the location
# registry.  The functions below read, filter, validate, and load the
# dataset, replacing any previous registry contents after a safe backup.
# GeoNames is the sole source for the location registry.  No external
# geocoding API calls are made during registry initialization.

GEONAMES_FILE_PATH = "/Volumes/weather_api/reference/location_files/US.txt"
STAGING_TABLE = "weather_api.reference.location_registry_staging"
BACKUP_TABLE = "weather_api.reference.location_registry_backup"

# GeoNames feature codes to EXCLUDE from the registry.
#   PPLQ  – abandoned populated place
#   PPLH  – historical populated place
#   PPLW  – destroyed populated place
#   PPLCH – church-related populated place (not a settlement)
#   STLMT – settlement (Israeli convention, not applicable to US)
EXCLUDED_FEATURE_CODES = frozenset(["PPLQ", "PPLH", "PPLW", "PPLCH", "STLMT"])


def read_geonames_file(spark, file_path: str = GEONAMES_FILE_PATH):
    """Read the GeoNames US.txt tab-separated file as a PySpark DataFrame.

    Uses the official 19-column GeoNames layout with explicit data types.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, LongType, IntegerType,
    )
    schema = StructType([
        StructField("geonameid",          LongType(),     True),
        StructField("name",               StringType(),   True),
        StructField("asciiname",          StringType(),   True),
        StructField("alternatenames",     StringType(),   True),
        StructField("latitude",           DoubleType(),   True),
        StructField("longitude",          DoubleType(),   True),
        StructField("feature_class",      StringType(),   True),
        StructField("feature_code",       StringType(),   True),
        StructField("country_code",       StringType(),   True),
        StructField("cc2",                StringType(),   True),
        StructField("admin1_code",        StringType(),   True),
        StructField("admin2_code",        StringType(),   True),
        StructField("admin3_code",        StringType(),   True),
        StructField("admin4_code",        StringType(),   True),
        StructField("population",         LongType(),     True),
        StructField("elevation",          DoubleType(),   True),
        StructField("dem",                IntegerType(),  True),
        StructField("timezone",           StringType(),   True),
        StructField("modification_date",  StringType(),   True),
    ])
    return (
        spark.read
        .option("header", "false")
        .option("sep", "\t")
        .schema(schema)
        .csv(file_path)
    )


def filter_populated_places(df):
    """Filter to valid, currently inhabited populated places.

    Keeps feature_class = 'P' and excludes abandoned, historical,
    destroyed, and other non-inhabited feature codes.
    """
    from pyspark.sql.functions import col
    return df.filter(
        (col("feature_class") == "P")
        & col("feature_code").isNotNull()
        & ~col("feature_code").isin(list(EXCLUDED_FEATURE_CODES))
    )


def validate_geonames_locations(df):
    """Validate required fields and coordinate ranges.

    Each record must have a valid geonameid, non-empty name, valid
    coordinates, and US country code.
    """
    from pyspark.sql.functions import col
    return df.filter(
        col("geonameid").isNotNull() & (col("geonameid") > 0)
        & col("name").isNotNull() & (col("name") != "")
        & col("latitude").isNotNull() & col("longitude").isNotNull()
        & (col("latitude") >= -90.0) & (col("latitude") <= 90.0)
        & (col("longitude") >= -180.0) & (col("longitude") <= 180.0)
        & (col("country_code") == "US")
    )


def deduplicate_geonames_locations(df):
    """Deduplicate by geonameid, keeping the first occurrence."""
    return df.dropDuplicates(["geonameid"])


def prepare_registry_records(df, spark):
    """Transform GeoNames DataFrame into the location registry schema.

    Maps GeoNames columns to registry columns, resolves full state names
    from 2-letter admin1 codes, and sets activation flags.
    """
    from pyspark.sql.functions import col, lit, when, current_timestamp, coalesce

    state_expr = lit(None).cast("string")
    for code, name in sorted(US_CODE_TO_STATE.items()):
        state_expr = when(col("admin1_code") == code, name).otherwise(state_expr)

    return df.select(
        col("geonameid").alias("location_id"),
        col("geonameid").alias("geonames_id"),
        col("name").alias("location_name"),
        col("latitude"),
        col("longitude"),
        state_expr.alias("state"),
        col("admin1_code").alias("state_code"),
        lit("United States").alias("country"),
        col("country_code").alias("country_code"),
        col("timezone"),
        coalesce(col("elevation"), col("dem").cast("double")).alias("elevation"),
        col("population"),
        col("feature_code"),
        col("admin2_code").alias("admin2"),
        lit("geonames").alias("location_source"),
        col("modification_date"),
        lit(True).alias("is_active"),
        lit(False).alias("weather_enabled"),
        lit(False).alias("air_quality_enabled"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
    )


def backup_registry(spark) -> str:
    """Create a versioned backup of the current location registry.

    Uses a timestamped backup table name to avoid overwriting previous backups.
    Returns the backup table name.  If the registry does not exist,
    returns a name without creating a table.
    """
    if not spark.catalog.tableExists(REGISTRY_TABLE):
        logger.info("Registry table does not exist — no backup needed")
        return BACKUP_TABLE
    backup_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    versioned_backup = f"{BACKUP_TABLE}_{backup_ts}"
    spark.sql(f"CREATE TABLE {versioned_backup} AS SELECT * FROM {REGISTRY_TABLE}")
    cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {versioned_backup}").collect()[0]["cnt"]
    logger.info("Registry backed up to %s (%d rows)", versioned_backup, cnt)
    return versioned_backup


def replace_registry(spark, new_df) -> int:
    """Replace the location registry with new GeoNames-derived data.

    Safe staging-and-validation approach:
    1. Write new data to a staging table.
    2. Validate staging (no null IDs, no duplicates).
    3. Back up the current registry.
    4. Atomically replace the production table.
    5. Drop the staging table.
    """
    # 1. Write staging table
    logger.info("Writing staging table: %s", STAGING_TABLE)
    new_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGING_TABLE)
    staging_cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {STAGING_TABLE}").collect()[0]["cnt"]
    logger.info("Staging table has %d rows", staging_cnt)

    # 2. Validate staging
    null_ids = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {STAGING_TABLE} WHERE location_id IS NULL"
    ).collect()[0]["cnt"]
    if null_ids > 0:
        raise ValueError(f"Staging table has {null_ids} null location_ids — aborting")
    dup_cnt = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM ("
        f"  SELECT location_id FROM {STAGING_TABLE} GROUP BY location_id HAVING COUNT(*) > 1"
        f")"
    ).collect()[0]["cnt"]
    if dup_cnt > 0:
        raise ValueError(f"Staging table has {dup_cnt} duplicate location_ids — aborting")

    # 3. Backup current registry
    backup_registry(spark)

    # 4. Atomically replace production table
    logger.info("Replacing production registry: %s", REGISTRY_TABLE)
    spark.sql(f"CREATE OR REPLACE TABLE {REGISTRY_TABLE} AS SELECT * FROM {STAGING_TABLE}")

    # 5. Clean up staging
    spark.sql(f"DROP TABLE IF EXISTS {STAGING_TABLE}")

    final_cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE}").collect()[0]["cnt"]
    logger.info("Registry replaced: %d rows in %s", final_cnt, REGISTRY_TABLE)
    return final_cnt


def initialize_registry_geonames(
    spark,
    file_path: str = GEONAMES_FILE_PATH,
) -> dict:
    """Full GeoNames-based registry initialization.

    Steps: read -> filter -> validate -> deduplicate -> prepare ->
    backup + replace -> validate final.

    Returns a dict with import statistics.
    """
    logger.info("=== GeoNames Registry Initialization ===")

    # 1. Read source
    logger.info("Step 1/7: Reading GeoNames file: %s", file_path)
    raw_df = read_geonames_file(spark, file_path)
    total_source = raw_df.count()
    logger.info("  Total rows in source: %d", total_source)

    # 2. Filter populated places
    logger.info("Step 2/7: Filtering to populated places")
    class_p_cnt = raw_df.filter("feature_class = 'P'").count()
    filtered_df = filter_populated_places(raw_df)
    filtered_cnt = filtered_df.count()
    excluded_cnt = class_p_cnt - filtered_cnt
    logger.info("  Class P: %d, After filter: %d (excluded %d)",
                class_p_cnt, filtered_cnt, excluded_cnt)

    # 3. Validate
    logger.info("Step 3/7: Validating location records")
    validated_df = validate_geonames_locations(filtered_df)
    validated_cnt = validated_df.count()
    invalid_cnt = filtered_cnt - validated_cnt
    logger.info("  Valid: %d, Invalid: %d", validated_cnt, invalid_cnt)

    # 4. Deduplicate
    logger.info("Step 4/7: Deduplicating by geonameid")
    deduped_df = deduplicate_geonames_locations(validated_df)
    deduped_cnt = deduped_df.count()
    dup_cnt = validated_cnt - deduped_cnt
    logger.info("  After dedup: %d (removed %d duplicates)", deduped_cnt, dup_cnt)

    # 5. Prepare registry records
    logger.info("Step 5/7: Preparing registry records")
    registry_df = prepare_registry_records(deduped_df, spark)

    # 6. Backup and replace
    logger.info("Step 6/7: Backing up and replacing registry")
    final_cnt = replace_registry(spark, registry_df)

    # 7. Validate final
    logger.info("Step 7/7: Validating final registry")
    final_stats = _validate_final_registry(spark)

    stats = {
        "source_total": total_source,
        "class_p_total": class_p_cnt,
        "after_filter": filtered_cnt,
        "excluded_by_filter": excluded_cnt,
        "after_validation": validated_cnt,
        "invalid_records": invalid_cnt,
        "after_dedup": deduped_cnt,
        "duplicates_removed": dup_cnt,
        "final_registry_count": final_cnt,
        **final_stats,
    }
    logger.info("=== GeoNames Registry Initialization Complete: %d locations ===", final_cnt)
    return stats


def _validate_final_registry(spark) -> dict:
    """Validate the final registry table after replacement."""
    stats = {}
    stats["total"] = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE}"
    ).collect()[0]["cnt"]
    stats["unique_location_ids"] = spark.sql(
        f"SELECT COUNT(DISTINCT location_id) AS cnt FROM {REGISTRY_TABLE}"
    ).collect()[0]["cnt"]
    stats["unique_geonames_ids"] = spark.sql(
        f"SELECT COUNT(DISTINCT geonames_id) AS cnt FROM {REGISTRY_TABLE}"
    ).collect()[0]["cnt"]
    stats["null_location_ids"] = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE} WHERE location_id IS NULL"
    ).collect()[0]["cnt"]
    stats["invalid_coords"] = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE} "
        f"WHERE latitude IS NULL OR longitude IS NULL "
        f"OR latitude < -90 OR latitude > 90 "
        f"OR longitude < -180 OR longitude > 180"
    ).collect()[0]["cnt"]
    stats["non_us"] = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE} WHERE country_code != 'US'"
    ).collect()[0]["cnt"]
    stats["states"] = spark.sql(
        f"SELECT COUNT(DISTINCT state) AS cnt FROM {REGISTRY_TABLE} WHERE state IS NOT NULL"
    ).collect()[0]["cnt"]
    stats["inactive_count"] = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE} WHERE is_active = false"
    ).collect()[0]["cnt"]
    logger.info(
        "Registry validation: %d locations, %d states, %d null IDs, %d invalid coords",
        stats["total"], stats["states"], stats["null_location_ids"], stats["invalid_coords"],
    )
    return stats