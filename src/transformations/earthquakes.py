"""Pure Spark transformations extracted from the original notebook."""
def transform(spark, input_df):
    # ════════════════════════════════════════════════════════════════════
    # EARTHQUAKES SILVER — TRANSFORM
    # ════════════════════════════════════════════════════════════════════
    from pyspark.sql.functions import col, explode, to_timestamp, expr, row_number
    from pyspark.sql.window import Window

    # ─── Schema & Config ────────────────────────────────────────────────


    # ─── Read Bronze Source ────────────────────────────────────────────
    bronze_eq = input_df

    # ─── Transform: Explode Features & Extract Fields ─────────────────
    # Each bronze row is a complete GeoJSON FeatureCollection with an array
    # of earthquake features. We explode into one row per event and extract
    # all fields from the nested struct into typed columns.

    # Epoch millisecond timestamps are converted to Spark timestamps by
    # timestamp_millis, retaining the original epochs for precision validation.
    silver_eq = bronze_eq.select(
        # Lineage
        col("_source_file_path"),
        col("_ingest_timestamp"),
        col("_rescued_data"),
        to_timestamp(col("metadata.retrieval_timestamp_utc")).alias("snapshot_at"),
        col("metadata.api_endpoint"),
        col("metadata.source"),
        # Feed-level metadata from USGS
        col("raw_response.metadata.api").alias("feed_api_version"),
        col("raw_response.metadata.title").alias("feed_title"),
        col("raw_response.metadata.generated").cast("long").alias("feed_generated_ms"),
        (expr("timestamp_millis(raw_response.metadata.generated)"))
            .cast("timestamp").alias("feed_generated_at"),
        # Explode the features array
        explode(col("raw_response.features")).alias("f"),
    ).select(
        # ── Lineage ──
        col("_source_file_path").alias("source_file_path"),
        col("_ingest_timestamp").alias("ingested_at"),
        col("_rescued_data"),
        col("snapshot_at"),
        col("api_endpoint"),
        col("source"),
        col("feed_api_version"),
        col("feed_title"),
        col("feed_generated_at"),
        col("feed_generated_ms"),
        # ── Event Identity ──
        col("f.id").alias("event_id"),
        col("f.properties.time").cast("long").alias("event_time_ms"),
        col("f.properties.updated").cast("long").alias("updated_at_ms"),
        col("f.properties.code").alias("event_code"),
        # ── Measurements ──
        col("f.properties.mag").alias("magnitude"),
        col("f.properties.magType").alias("magnitude_type"),
        col("f.properties.place").alias("place_description"),
        col("f.properties.title").alias("event_title"),
        col("f.properties.type").alias("event_type"),
        col("f.properties.status").alias("review_status"),
        col("f.properties.alert").alias("alert_level"),
        col("f.properties.sig").alias("significance"),
        col("f.properties.tsunami").cast("boolean").alias("tsunami_flag"),
        col("f.properties.net").alias("seismic_network"),
        col("f.properties.nst").alias("station_count"),
        col("f.properties.gap").alias("azimuthal_gap"),
        col("f.properties.dmin").alias("distance_to_nearest_station_deg"),
        col("f.properties.rms").alias("rms_travel_time_residual"),
        col("f.properties.felt").alias("felt_report_count"),
        col("f.properties.cdi").alias("community_intensity"),
        col("f.properties.mmi").alias("mercalli_intensity"),
        col("f.properties.tz").alias("timezone_offset"),
        # ── URLs ──
        col("f.properties.url").alias("event_page_url"),
        col("f.properties.detail").alias("detail_api_url"),
        # ── ID lists (comma-delimited → ARRAY<STRING>, empty elements removed) ──
        expr("filter(split(f.properties.ids, ','), x -> x != '')").alias("all_event_ids"),
        expr("filter(split(f.properties.sources, ','), x -> x != '')").alias("all_sources"),
        expr("filter(split(f.properties.types, ','), x -> x != '')").alias("available_product_types"),
        # ── Timestamps (epoch ms → UTC timestamp) ──
        (expr("timestamp_millis(f.properties.time)"))
            .cast("timestamp").alias("event_time"),
        (expr("timestamp_millis(f.properties.updated)"))
            .cast("timestamp").alias("updated_at"),
        # ── Geometry ──
        col("f.geometry.coordinates")[0].alias("longitude"),
        col("f.geometry.coordinates")[1].alias("latitude"),
        col("f.geometry.coordinates")[2].alias("depth_km"),
        col("f.geometry.type").alias("geometry_type"),
    )



    dedup_window = Window.partitionBy("event_id").orderBy(
        col("updated_at").desc(),
        col("snapshot_at").desc()
    )
    silver_eq_latest = (
        silver_eq
        .withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    from src.contracts import output_types
    from src.validation.common import normalize_output
    return {'latest':normalize_output(silver_eq_latest,output_types('earthquakes','latest')),'history':normalize_output(silver_eq,output_types('earthquakes','history'))}
