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
from src.config import load_config, SOURCES

# ── Logging ──
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── Delta table constants ──

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

def create_registry_table(spark, config=None) -> None:
    """Create the weather_api.reference schema and location_registry Delta table."""
    cfg = (config or load_config()).validate()
    REGISTRY_TABLE = cfg.registry
    REGISTRY_SCHEMA = f"{cfg.catalog}.{cfg.reference_schema}"
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


def merge_locations(spark, locations: list[dict[str, Any]], config=None) -> None:
    """Idempotently MERGE location records into the Delta registry.

    Rules:
    * New locations are inserted with is_active=true but weather_enabled/air_quality_enabled=false by default.
    * Existing locations are updated with refreshed metadata.
    * created_at is preserved on updates.
    * updated_at is refreshed on every update.
    * is_active, weather_enabled, air_quality_enabled are NOT reset on update.
    * Locations absent from input are NOT deleted.
    * Duplicate location_id values in input are deduplicated (first wins).
    """
    cfg = (config or load_config()).validate()
    REGISTRY_TABLE = cfg.registry
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        BooleanType, TimestampType, LongType,
    )
    from delta.tables import DeltaTable

    from src.validation.location_registry import validate_merge_input
    validate_merge_input(locations)

    create_registry_table(spark, config=cfg)

    # Stable IDs only; historical null IDs require explicit migration.
    seen: set = set()
    unique: list[dict[str, Any]] = []
    for loc in locations:
        key = loc.get("location_id")
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
            "geonames_id": loc.get("geonames_id"),
            "location_source": loc.get("location_source"),
            "modification_date": loc.get("modification_date"),
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
        StructField("geonames_id", LongType(), True),
        StructField("location_source", StringType(), True),
        StructField("modification_date", StringType(), True),
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
    from src.validation.location_registry import validate_registry
    validate_registry(source_df)
    target = DeltaTable.forName(spark, REGISTRY_TABLE)

    merge_condition = "t.location_id = s.location_id"

    (
        target.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(set={
            "geonames_id": "s.geonames_id",
            "location_source": "s.location_source",
            "modification_date": "s.modification_date",
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
            "geonames_id": "s.geonames_id",
            "location_source": "s.location_source",
            "modification_date": "s.modification_date",
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


def load_active_locations(spark, source: str = "all", config=None) -> list[dict[str, Any]]:
    """Load active locations from the registry for ingestion.

    Args:
        spark: Spark session.
        source: "weather", "air_quality", or "all". Determines enabled filter.

    Returns:
        List of dicts with keys: location_id, name, latitude, longitude,
        state, state_code, country, country_code, timezone.
        Returns [] if the table does not exist.
    """
    cfg = (config or load_config()).validate()
    REGISTRY_TABLE = cfg.registry
    from src.validation.common import require
    require(source in (*SOURCES, "all"), "Unsupported registry source", stage="registry")
    if not spark.catalog.tableExists(REGISTRY_TABLE):
        logger.error("Registry table %s does not exist — run init_registry first", REGISTRY_TABLE)
        return []

    query = f"SELECT * FROM {REGISTRY_TABLE} WHERE is_active = true"
    if source == "weather":
        query += " AND weather_enabled = true"
    elif source == "air_quality":
        query += " AND air_quality_enabled = true"

    frame = spark.sql(query)
    from src.validation.location_registry import validate_registry
    validate_registry(frame)
    rows = frame.collect()
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
    config=None,
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
    cfg = (config or load_config()).validate()
    REGISTRY_TABLE = cfg.registry
    if not spark.catalog.tableExists(REGISTRY_TABLE):
        raise RuntimeError(f"Registry table {REGISTRY_TABLE} does not exist")

    from src.validation.location_registry import validate_activation_arguments
    validate_activation_arguments(location_ids, limit, weather, air_quality, distributed)
    if location_ids is None:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        candidates = spark.table(REGISTRY_TABLE).filter("is_active = true")
        if distributed:
            candidates = candidates.withColumn("_rank", F.row_number().over(
                Window.partitionBy("state_code").orderBy(F.col("population").desc_nulls_last(), F.col("location_id"))
            )).filter("_rank = 1")
        location_ids = [r.location_id for r in candidates.orderBy(
            F.col("population").desc_nulls_last(), F.col("location_id")
        ).limit(limit).select("location_id").collect()]
    from src.validation.location_registry import validate_activation_selection
    validate_activation_selection(spark.table(REGISTRY_TABLE), location_ids)
    id_list = ",".join(str(i) for i in sorted(set(location_ids)))
    where_clause = f"location_id IN ({id_list})"

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


# GeoNames feature codes to EXCLUDE from the registry.
#   PPLQ  – abandoned populated place
#   PPLH  – historical populated place
#   PPLW  – destroyed populated place
#   PPLCH – church-related populated place (not a settlement)
#   STLMT – settlement (Israeli convention, not applicable to US)
EXCLUDED_FEATURE_CODES = frozenset(["PPLQ", "PPLH", "PPLW", "PPLCH", "STLMT"])


def read_geonames_file(spark, file_path=None, config=None):
    """Read the GeoNames US.txt tab-separated file as a PySpark DataFrame.

    Uses the official 19-column GeoNames layout with explicit data types.
    """
    cfg = (config or load_config()).validate()
    file_path = file_path or cfg.registry_source
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, LongType, IntegerType,
    )
    from src.validation.location_registry import validate_geonames_file
    validate_geonames_file(spark, file_path)
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


def filter_valid_geonames_locations(df):
    """Validate required fields and coordinate ranges.

    Each record must have a valid geonameid, non-empty name, valid
    coordinates, and US country code.
    """
    from src.validation.location_registry import valid_geonames_predicate
    return df.filter(valid_geonames_predicate())


def deduplicate_geonames_locations(df):
    """Choose latest modification date, with a deterministic full-record tie break."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    order = Window.partitionBy("geonameid").orderBy(
        F.col("modification_date").desc_nulls_last(),
        F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)]))
    )
    return df.withColumn("_rank", F.row_number().over(order)).filter("_rank = 1").drop("_rank")


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


def backup_registry(spark, config=None) -> str:
    """Create a versioned backup of the current location registry.

    Uses a timestamped backup table name to avoid overwriting previous backups.
    Returns the backup table name.  If the registry does not exist,
    returns a name without creating a table.
    """
    cfg = (config or load_config()).validate()
    REGISTRY_TABLE = cfg.registry
    BACKUP_TABLE = cfg.table(cfg.reference_schema, cfg.registry_backup_prefix)
    if not spark.catalog.tableExists(REGISTRY_TABLE):
        logger.info("Registry table does not exist — no backup needed")
        return BACKUP_TABLE
    backup_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    versioned_backup = f"{BACKUP_TABLE}_{backup_ts}"
    spark.sql(f"CREATE TABLE {versioned_backup} AS SELECT * FROM {REGISTRY_TABLE}")
    cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {versioned_backup}").collect()[0]["cnt"]
    logger.info("Registry backed up to %s (%d rows)", versioned_backup, cnt)
    return versioned_backup


def replace_registry(spark, new_df, config=None) -> int:
    """Replace the location registry with new GeoNames-derived data.

    Safe staging-and-validation approach:
    1. Write new data to a staging table.
    2. Validate staging (no null IDs, no duplicates).
    3. Back up the current registry.
    4. Atomically replace the production table.
    5. Drop the staging table.
    """
    cfg = (config or load_config()).validate()
    REGISTRY_TABLE = cfg.registry
    STAGING_TABLE = cfg.registry_staging
    from src.validation.location_registry import validate_registry
    existing = spark.table(REGISTRY_TABLE) if spark.catalog.tableExists(REGISTRY_TABLE) else None
    if existing is not None:
        validate_registry(existing)
        new_df = preserve_registry_settings(new_df, existing)
    prepared_result = validate_registry(new_df, existing)
    new_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGING_TABLE)
    staged_result = validate_registry(spark.table(STAGING_TABLE), existing)
    from src.validation.common import reconcile
    reconcile(staged_result.checks["rows"], prepared_result.checks["rows"], "registry staging")

    # 3. Backup current registry
    backup_registry(spark, config=cfg)

    # 4. Atomically replace production table
    logger.info("Replacing production registry: %s", REGISTRY_TABLE)
    spark.sql(f"CREATE OR REPLACE TABLE {REGISTRY_TABLE} AS SELECT * FROM {STAGING_TABLE}")

    # 5. Clean up staging
    spark.sql(f"DROP TABLE IF EXISTS {STAGING_TABLE}")

    final_cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {REGISTRY_TABLE}").collect()[0]["cnt"]
    reconcile(final_cnt, staged_result.checks["rows"], "registry publication")
    logger.info("Registry replaced: %d rows in %s", final_cnt, REGISTRY_TABLE)
    return final_cnt


def initialize_registry_geonames(
    spark,
    file_path=None,
    config=None,
) -> dict:
    """Full GeoNames-based registry initialization.

    Steps: read -> filter -> validate -> deduplicate -> prepare ->
    backup + replace -> validate final.

    Returns a dict with import statistics.
    """
    cfg = (config or load_config()).validate()
    REGISTRY_TABLE = cfg.registry
    logger.info("=== GeoNames Registry Initialization ===")

    # 1. Read source
    logger.info("Step 1/7: Reading GeoNames file: %s", file_path)
    raw_df = read_geonames_file(spark, file_path, config=cfg)
    from src.validation.location_registry import validate_geonames_input
    validate_geonames_input(raw_df)
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
    validated_df = filter_valid_geonames_locations(filtered_df)
    validated_cnt = validated_df.count()
    # Retain rejected input for inspection; filtering is explicit and reconciled.
    from pyspark.sql import functions as F
    from src.validation.location_registry import valid_geonames_predicate
    rejected_df = filtered_df.filter(~F.coalesce(valid_geonames_predicate(), F.lit(False)))
    rejection_table = cfg.table(cfg.reference_schema, cfg.registry_rejected_prefix) + "_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    rejected_df.write.format("delta").saveAsTable(rejection_table)
    logger.info("Rejected GeoNames records retained in %s", rejection_table)
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
    final_cnt = replace_registry(spark, registry_df, config=cfg)

    # 7. Validate final
    logger.info("Step 7/7: Validating final registry")
    from src.validation.location_registry import validate_registry, validate_registry_counts
    validate_registry(spark.table(REGISTRY_TABLE))
    final_stats = {}

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
    validate_registry_counts(stats)
    logger.info("=== GeoNames Registry Initialization Complete: %d locations ===", final_cnt)
    return stats


def preserve_registry_settings(new_df, existing):
    """Refresh metadata while preserving settings and creation time for known IDs."""
    from pyspark.sql import functions as F
    settings = ("is_active", "weather_enabled", "air_quality_enabled", "created_at")
    joined = new_df.alias("n").join(existing.alias("o"), "location_id", "left")
    return joined.select(*[
        (F.when(F.col("o.location_id").isNotNull(), F.col("o."+c)).otherwise(F.col("n."+c)).alias(c)
         if c in settings else F.col("n."+c).alias(c)) for c in new_df.columns
    ])

def select_ingestion_locations(spark,source,limit,slot,config=None):
    """Validate active registry before HTTP; select the complete enabled set.

    Every enabled location for the source is selected, ordered by location_id:
    rotation subsetting across runs is forbidden so a partial set can never
    publish an apparently complete run, and weather/air_quality always process
    the same enabled identity set. max_locations below the enabled count fails
    loudly instead of silently retaining a smaller limit.
    """
    from pyspark.sql import functions as F
    from src.validation.common import require
    from src.validation.location_registry import validate_registry, validate_selection_input, validate_selection_completeness
    cfg=(config or load_config()).validate()
    require(source in ('weather','air_quality'),'Unsupported location source',stage='registry')
    validate_selection_input([],limit,slot)
    require(limit<=cfg.max_manifest_files,"Selection exceeds configured manifest bound",stage="registry")
    active=spark.table(cfg.registry).filter(F.col('is_active')==True)
    # Validate active candidates, including all activation flags, before source selection.
    validate_registry(active)
    enabled=active.filter(F.col(source+'_enabled')==True)
    count=enabled.count()
    require(count>0,'No enabled locations for '+source,stage='registry')
    require(count<=limit,'Enabled location count (%d) exceeds max_locations (%d); refusing silent rotation subsetting'%(count,limit),stage='registry',check='selection_completeness')
    rows=enabled.select('location_id',F.col('location_name').alias('name'),'latitude','longitude').orderBy('location_id').collect()
    locations=[r.asDict() for r in rows]
    validate_selection_input(locations,limit,slot)
    validate_selection_completeness([loc['location_id'] for loc in locations],[r.location_id for r in enabled.select('location_id').collect()])
    return locations
