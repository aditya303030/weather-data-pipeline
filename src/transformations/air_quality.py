"""Pure Spark transformations extracted from the original notebook."""
def transform(spark, input_df):
    # ════════════════════════════════════════════════════════════════════
    # AIR QUALITY SILVER — TRANSFORM
    # ════════════════════════════════════════════════════════════════════
    from pyspark.sql.functions import col, explode, to_timestamp, arrays_zip

    # ─── Schema & Config ────────────────────────────────────────────────


    # ─── Read Bronze Source ────────────────────────────────────────────
    bronze = input_df

    _location_id_expr = col("metadata.location_id").cast("long").alias("location_id")

    # ─── Transform: Current Observations ───────────────────────────────
    # One row per location per API snapshot. Extracts the real-time air
    # quality readings from raw_response.current and flattens metadata.

    # Pollen and ammonia fields are string-typed in Bronze (often null due
    # to seasonality). We cast them to double; nulls are preserved.
    silver_current = bronze.select(
        # Lineage
        col("metadata.location_name"),
        _location_id_expr,
        col("metadata.latitude"),
        col("metadata.longitude"),
        col("raw_response.elevation").alias("elevation_m"),
        to_timestamp(col("metadata.retrieval_timestamp_utc")).alias("snapshot_at"),
        to_timestamp(col("raw_response.current.time")).alias("observed_at"),
        col("metadata.source"),
        col("metadata.api_endpoint"),
        col("_source_file_path").alias("source_file_path"),
        col("_ingest_timestamp").alias("ingested_at"),
        col("_rescued_data"),
        # Pollutants (μg/m³) — already double in Bronze
        col("raw_response.current.pm2_5"),
        col("raw_response.current.pm10"),
        col("raw_response.current.carbon_monoxide"),
        col("raw_response.current.nitrogen_dioxide"),
        col("raw_response.current.sulphur_dioxide"),
        col("raw_response.current.ozone"),
        col("raw_response.current.dust"),
        col("raw_response.current.aerosol_optical_depth"),
        # AQI indices (composite scores, cast to int for consistency with hourly)
        col("raw_response.current.european_aqi").cast("int").alias("european_aqi"),
        col("raw_response.current.us_aqi").cast("int").alias("us_aqi"),
        col("raw_response.current.us_aqi_carbon_monoxide").cast("int").alias("us_aqi_carbon_monoxide"),
        col("raw_response.current.us_aqi_nitrogen_dioxide").cast("int").alias("us_aqi_nitrogen_dioxide"),
        col("raw_response.current.us_aqi_ozone").cast("int").alias("us_aqi_ozone"),
        col("raw_response.current.us_aqi_pm10").cast("int").alias("us_aqi_pm10"),
        col("raw_response.current.us_aqi_pm2_5").cast("int").alias("us_aqi_pm2_5"),
        col("raw_response.current.us_aqi_sulphur_dioxide").cast("int").alias("us_aqi_sulphur_dioxide"),
        # Measurement interval (seconds the current reading represents)
        col("raw_response.current.interval").cast("int").alias("interval_seconds"),
        # UV index (dimensionless)
        col("raw_response.current.uv_index"),
        col("raw_response.current.uv_index_clear_sky"),
        # Pollen & ammonia (string in Bronze → cast to double, nulls preserved)
        col("raw_response.current.alder_pollen").cast("double").alias("alder_pollen"),
        col("raw_response.current.ammonia").cast("double").alias("ammonia"),
        col("raw_response.current.birch_pollen").cast("double").alias("birch_pollen"),
        col("raw_response.current.grass_pollen").cast("double").alias("grass_pollen"),
        col("raw_response.current.mugwort_pollen").cast("double").alias("mugwort_pollen"),
        col("raw_response.current.olive_pollen").cast("double").alias("olive_pollen"),
        col("raw_response.current.ragweed_pollen").cast("double").alias("ragweed_pollen"),
    )


    # ─── Transform: Hourly Forecast ────────────────────────────────────
    # Explodes the hourly arrays into one row per location, forecast hour,
    # and snapshot. Uses arrays_zip to align all measurement arrays with the
    # time array by position, then explodes.

    # Hourly field specs: (bronze_name, silver_name, spark_type)
    HOURLY_SPECS = [
        ("time",                       "forecast_for",                "timestamp"),
        ("pm2_5",                      "pm2_5",                      "double"),
        ("pm10",                       "pm10",                       "double"),
        ("carbon_monoxide",            "carbon_monoxide",            "double"),
        ("nitrogen_dioxide",           "nitrogen_dioxide",           "double"),
        ("sulphur_dioxide",            "sulphur_dioxide",            "double"),
        ("ozone",                      "ozone",                      "double"),
        ("dust",                       "dust",                       "double"),
        ("aerosol_optical_depth",      "aerosol_optical_depth",      "double"),
        ("european_aqi",               "european_aqi",               "int"),
        ("us_aqi",                     "us_aqi",                     "int"),
        ("us_aqi_carbon_monoxide",     "us_aqi_carbon_monoxide",     "int"),
        ("us_aqi_nitrogen_dioxide",    "us_aqi_nitrogen_dioxide",    "int"),
        ("us_aqi_ozone",               "us_aqi_ozone",               "int"),
        ("us_aqi_pm10",                "us_aqi_pm10",                "int"),
        ("us_aqi_pm2_5",               "us_aqi_pm2_5",               "int"),
        ("us_aqi_sulphur_dioxide",     "us_aqi_sulphur_dioxide",     "int"),
        ("uv_index",                   "uv_index",                   "double"),
        ("uv_index_clear_sky",         "uv_index_clear_sky",         "double"),
        ("alder_pollen",               "alder_pollen",               "double"),
        ("ammonia",                    "ammonia",                    "double"),
        ("birch_pollen",               "birch_pollen",               "double"),
        ("grass_pollen",               "grass_pollen",               "double"),
        ("mugwort_pollen",             "mugwort_pollen",             "double"),
        ("olive_pollen",               "olive_pollen",               "double"),
        ("ragweed_pollen",             "ragweed_pollen",             "double"),
    ]

    def _hourly_select(spec):
        """Build a select expression for one hourly field after arrays_zip."""
        bronze_name, silver_name, dtype = spec
        base = col(f"hourly.{bronze_name}")
        if dtype == "timestamp":
            return to_timestamp(base).alias(silver_name)
        elif dtype == "int":
            return base.cast("int").alias(silver_name)
        else:
            return base.cast("double").alias(silver_name)

    # Zip all hourly arrays into an array of structs, then explode
    hourly_zip_cols = [col(f"raw_response.hourly.{s[0]}") for s in HOURLY_SPECS]

    silver_hourly = bronze.select(
        # Lineage
        col("metadata.location_name"),
        _location_id_expr,
        col("metadata.latitude"),
        col("metadata.longitude"),
        col("raw_response.elevation").alias("elevation_m"),
        to_timestamp(col("metadata.retrieval_timestamp_utc")).alias("snapshot_at"),
        col("metadata.source"),
        col("metadata.api_endpoint"),
        col("_source_file_path").alias("source_file_path"),
        col("_ingest_timestamp").alias("ingested_at"),
        col("_rescued_data"),
        # Explode zipped hourly arrays
        explode(arrays_zip(*hourly_zip_cols)).alias("hourly"),
    ).select(
        # Lineage passthrough
        "location_name", "location_id", "latitude", "longitude", "elevation_m",
        "snapshot_at", "source", "api_endpoint", "source_file_path",
        "ingested_at", "_rescued_data",
        # Hourly measurement fields
        *[_hourly_select(s) for s in HOURLY_SPECS],
    )


    return {'current':silver_current,'hourly':silver_hourly}
