"""Pure Spark transformations extracted from the original notebook."""
def transform(spark, input_df):
    # ════════════════════════════════════════════════════════════════════
    # WEATHER SILVER — TRANSFORM
    # ════════════════════════════════════════════════════════════════════
    from pyspark.sql.functions import col, explode, to_timestamp, to_date, arrays_zip

    # ─── Schema & Config ────────────────────────────────────────────────


    # ─── Read Bronze Source ────────────────────────────────────────────
    bronze = input_df

    _location_id_expr = col("metadata.location_id").cast("long").alias("location_id")

    # ─── Transform: Current Observations ───────────────────────────────
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
        # Current weather measurements
        col("raw_response.current.temperature_2m"),
        col("raw_response.current.apparent_temperature"),
        col("raw_response.current.relative_humidity_2m").cast("int").alias("relative_humidity_2m"),
        col("raw_response.current.is_day").cast("boolean").alias("is_day"),
        col("raw_response.current.precipitation"),
        col("raw_response.current.rain"),
        col("raw_response.current.snowfall"),
        col("raw_response.current.weather_code").cast("int").alias("weather_code"),
        col("raw_response.current.cloud_cover").cast("int").alias("cloud_cover"),
        col("raw_response.current.pressure_msl"),
        col("raw_response.current.surface_pressure"),
        col("raw_response.current.wind_speed_10m"),
        col("raw_response.current.wind_direction_10m").cast("int").alias("wind_direction_10m"),
        col("raw_response.current.wind_gusts_10m"),
        # Measurement interval (seconds the current reading represents)
        col("raw_response.current.interval").cast("int").alias("interval_seconds"),
    )

    # ─── Transform: Hourly Forecast ────────────────────────────────────
    HOURLY_SPECS = [
        ("time",                      "forecast_for",                "timestamp"),
        ("temperature_2m",            "temperature_2m",            "double"),
        ("relative_humidity_2m",      "relative_humidity_2m",      "int"),
        ("apparent_temperature",      "apparent_temperature",       "double"),
        ("precipitation",             "precipitation",             "double"),
        ("rain",                      "rain",                      "double"),
        ("snowfall",                  "snowfall",                  "double"),
        ("snow_depth",                "snow_depth",                "double"),
        ("weather_code",             "weather_code",             "int"),
        ("pressure_msl",             "pressure_msl",             "double"),
        ("surface_pressure",         "surface_pressure",         "double"),
        ("cloud_cover",              "cloud_cover",              "int"),
        ("cloud_cover_low",          "cloud_cover_low",          "int"),
        ("cloud_cover_mid",          "cloud_cover_mid",          "int"),
        ("cloud_cover_high",         "cloud_cover_high",         "int"),
        ("dew_point_2m",             "dew_point_2m",             "double"),
        ("is_day",                   "is_day",                   "boolean"),
        ("shortwave_radiation",      "shortwave_radiation",      "double"),
        ("direct_radiation",         "direct_radiation",         "double"),
        ("direct_normal_irradiance", "direct_normal_irradiance", "double"),
        ("diffuse_radiation",        "diffuse_radiation",        "double"),
        ("terrestrial_radiation",   "terrestrial_radiation",   "double"),
        ("sunshine_duration",        "sunshine_duration",        "double"),
        ("uv_index",                 "uv_index",                 "double"),
        ("uv_index_clear_sky",       "uv_index_clear_sky",       "double"),
        ("vapour_pressure_deficit",  "vapour_pressure_deficit",  "double"),
        ("visibility",               "visibility",               "double"),
        ("wet_bulb_temperature_2m",  "wet_bulb_temperature_2m",  "double"),
        ("wind_speed_10m",           "wind_speed_10m",           "double"),
        ("wind_speed_80m",           "wind_speed_80m",           "double"),
        ("wind_direction_10m",       "wind_direction_10m",       "int"),
        ("wind_direction_80m",       "wind_direction_80m",       "int"),
        ("wind_gusts_10m",           "wind_gusts_10m",           "double"),
        ("soil_temperature_0cm",     "soil_temperature_0cm",     "double"),
        ("soil_temperature_6cm",     "soil_temperature_6cm",     "double"),
        ("soil_temperature_18cm",    "soil_temperature_18cm",    "double"),
        ("soil_temperature_54cm",    "soil_temperature_54cm",    "double"),
        ("soil_moisture_0_to_1cm",   "soil_moisture_0_to_1cm",   "double"),
        ("soil_moisture_1_to_3cm",    "soil_moisture_1_to_3cm",   "double"),
        ("soil_moisture_3_to_9cm",    "soil_moisture_3_to_9cm",   "double"),
        ("soil_moisture_9_to_27cm",   "soil_moisture_9_to_27cm",   "double"),
        ("soil_moisture_27_to_81cm", "soil_moisture_27_to_81cm", "double"),
    ]

    def _select_field(spec):
        """Build a select expression for one zipped field."""
        bronze_name, silver_name, dtype = spec
        base = col(f"hourly.{bronze_name}")
        if dtype == "timestamp":
            return to_timestamp(base).alias(silver_name)
        elif dtype == "boolean":
            return base.cast("boolean").alias(silver_name)
        elif dtype == "int":
            return base.cast("int").alias(silver_name)
        else:
            return base.cast("double").alias(silver_name)

    hourly_zip_cols = [col(f"raw_response.hourly.{s[0]}") for s in HOURLY_SPECS]

    silver_hourly = bronze.select(
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
        explode(arrays_zip(*hourly_zip_cols)).alias("hourly"),
    ).select(
        "location_name", "location_id", "latitude", "longitude", "elevation_m",
        "snapshot_at", "source", "api_endpoint", "source_file_path",
        "ingested_at", "_rescued_data",
        *[_select_field(s) for s in HOURLY_SPECS],
    )

    # ─── Transform: Daily Forecast ─────────────────────────────────────
    DAILY_SPECS = [
        ("time",                      "forecast_date",             "date"),
        ("temperature_2m_max",       "temperature_2m_max",       "double"),
        ("temperature_2m_min",       "temperature_2m_min",       "double"),
        ("apparent_temperature_max", "apparent_temperature_max", "double"),
        ("apparent_temperature_min", "apparent_temperature_min", "double"),
        ("sunrise",                   "sunrise",                   "timestamp"),
        ("sunset",                   "sunset",                   "timestamp"),
        ("daylight_duration",        "daylight_duration",        "double"),
        ("sunshine_duration",        "sunshine_duration",        "double"),
        ("uv_index_max",             "uv_index_max",             "double"),
        ("precipitation_sum",        "precipitation_sum",        "double"),
        ("rain_sum",                 "rain_sum",                 "double"),
        ("shortwave_radiation_sum",  "shortwave_radiation_sum",  "double"),
        ("snowfall_sum",             "snowfall_sum",             "double"),
        ("precipitation_hours",      "precipitation_hours",      "double"),
        ("precipitation_probability_max", "precipitation_probability_max", "int"),
        ("weather_code",             "weather_code",             "int"),
        ("wind_speed_10m_max",       "wind_speed_10m_max",       "double"),
        ("wind_gusts_10m_max",       "wind_gusts_10m_max",       "double"),
        ("wind_direction_10m_dominant", "wind_direction_10m_dominant", "int"),
        ("et0_fao_evapotranspiration", "et0_fao_evapotranspiration", "double"),
    ]

    def _daily_select(spec):
        """Build a select expression for one zipped daily field."""
        bronze_name, silver_name, dtype = spec
        base = col(f"daily.{bronze_name}")
        if dtype == "date":
            return to_date(base).alias(silver_name)
        elif dtype == "timestamp":
            return to_timestamp(base).alias(silver_name)
        elif dtype == "int":
            return base.cast("int").alias(silver_name)
        else:
            return base.cast("double").alias(silver_name)

    daily_zip_cols = [col(f"raw_response.daily.{s[0]}") for s in DAILY_SPECS]

    silver_daily = bronze.select(
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
        explode(arrays_zip(*daily_zip_cols)).alias("daily"),
    ).select(
        "location_name", "location_id", "latitude", "longitude", "elevation_m",
        "snapshot_at", "source", "api_endpoint", "source_file_path",
        "ingested_at", "_rescued_data",
        *[_daily_select(s) for s in DAILY_SPECS],
    )

    return {'current':silver_current,'hourly':silver_hourly,'daily':silver_daily}
