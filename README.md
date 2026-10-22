# Weather data pipeline

This repository implements a Unity Catalog raw → Bronze → Silver pipeline. Gold notebooks remain empty placeholders. No jobs or schedules are created by this repository change.

## Architecture and execution order

1. `src/ingestion/location_discovery.py` reads GeoNames `US.txt`, filters inhabited US populated places, validates input/output, retains rejected records, preserves existing activation settings and creation timestamps, stages and backs up the registry, then replaces it.
2. Explicitly enable selected registry IDs for weather and/or air quality. New IDs default to disabled; refresh does not reset existing flags. Activation uses deterministic population order with location ID as the tie breaker; distributed mode selects one per state before limiting.
3. `src/ingestion/save_raw_data.ipynb` calls `src.ingestion.pipeline.run_ingestion`. Weather and air quality use enabled registry locations; USGS is a global feed. API transport stays in the API modules and responses are validated before saving.
4. Each source's Bronze notebook calls Auto Loader, preserves the original response plus ingestion/file lineage, then reconciles the completed ingestion manifest against the table.
5. Each Silver notebook validates and transforms pending Bronze files into typed outputs, validates all source outputs before writing, performs idempotent MERGEs, validates persisted scope, and only then records completed files.

| Source | Bronze table under `weather_api.bronze` | Silver tables under `weather_api.silver` |
| --- | --- | --- |
| Weather | `weather_raw` | `weather_current`, `weather_hourly`, `weather_daily` |
| Air quality | `air_quality_raw` | `air_quality_current`, `air_quality_hourly` |
| USGS | `earthquakes_raw` | `earthquakes`, `earthquakes_event_history` |

Silver identities are `(location_id, snapshot_at)`, plus `forecast_for` or `forecast_date` for forecasts. Earthquake identities are `event_id` for latest and `(event_id, snapshot_at)` for history. Location names are descriptive attributes, never identity keys. Epoch milliseconds use `timestamp_millis`; Spark sessions explicitly use UTC. Regional/seasonal null measurements are permitted. A valid zero-event earthquake feed is accepted, including an initial empty feed through the explicit Bronze GeoJSON schema.

## Required setup (manual; not performed locally)

Provide a Databricks runtime with Spark, Delta Lake, Auto Loader and Unity Catalog Volume support, plus the dependencies in `requirements.txt`. Run from a checkout, set `WEATHER_PROJECT_ROOT` for copied notebooks, or install this repository as a Python package (`pip install -e .`). Notebook bootstrap discovers the repository root from its ancestors; no personal workspace path is embedded.

Required objects and permissions:

- Catalog: `weather_api`.
- Schemas: `reference`, `raw`, `bronze`, `silver`, `pipeline_state`. Pipeline code can create schemas if authorized; catalog/volumes must already exist.
- Source volume: `/Volumes/weather_api/reference/location_files/`, containing the official 19-column tab-delimited `US.txt`.
- Raw volume: `/Volumes/weather_api/raw/raw_files/` (override ingestion via `RAW_VOLUME_ROOT` if needed).
- State volume: `/Volumes/weather_api/pipeline_state/autoloader_state/` for `_schemas/<source>` and `_checkpoints/<source>`.
- Registry: `weather_api.reference.location_registry`; created by registry initialization in an existing reference schema.
- Completion ledger: `weather_api.pipeline_state.silver_completed_files`; created on the first Silver run.
- Table read/write and schema creation permissions as appropriate; raw and state Volume file permissions.

Ingestion parameters: `source` defaults reliably to `all`; accepted values also include `weather`, `air_quality`, `earthquakes`, `init_registry`, and `enable_locations`. `max_locations` defaults to 20. `enable_limit`, `enable_weather`, `enable_air_quality`, and `distributed` configure explicit activation. Registry initialization is a separate operation, never part of normal ingestion.

`coverage_slot` defaults to the UTC date ordinal. Locations are sorted by stable ID and divided into blocks of `max_locations`; slot N selects block N modulo the number of blocks. Successive daily slots cover all enabled IDs in a stable registry. Set an explicit monotonically increasing slot for other cadences; reuse the same slot to retry the same coverage selection. Registry membership changes restart the block layout. Failed slots must be retried for complete coverage; the default is a rotation policy, not an automatic successful-coverage guarantee.

`.env` remains ignored/untracked. API overrides are optional; public endpoint defaults work without secrets. Userinfo, query strings and fragments are removed before endpoint metadata is saved. Transport exceptions are redacted rather than printing potentially credential-bearing URLs. Do not embed credentials in URL paths; use a separately managed authentication mechanism if adding authenticated providers. Do not commit `.env`, credentials or populated notebooks.

## Runtime validation

Validation implementations live in `src/validation/`. Existing notebooks contain only portable imports and orchestration calls; no inline assertions, null/range/schema checks or validation reports remain there. Ingestion API clients call the same response validators used during file replay.

| Module | Responsibility |
| --- | --- |
| `__init__.py` | Lightweight package initialization; no Spark startup |
| `common.py` | Structured results, `ValidationError`, schema/type/key/timestamp/range checks, duplicate detection and scoped target reconciliation |
| `location_registry.py` | GeoNames structure and valid-record predicates, nonempty/stable registry, activation flags/settings preservation and count reconciliation |
| `ingestion.py` | Source-specific API shapes, required requested fields, timestamps, parallel-array lengths and GeoJSON structure; no HTTP |
| `raw.py` | Current-run wrapper/JSON/path/file-count and payload validation |
| `bronze.py` | Nested schema, lineage, source/location identity, arrays, rescued-data policy and expected file representation |
| `weather_silver.py` | Three weather outputs, stable keys, measurements and source-derived hourly/daily counts |
| `air_quality_silver.py` | Two air-quality outputs, stable keys, measurements, nullable regional data and source-derived hourly counts |
| `earthquakes_silver.py` | Latest/history identities, coordinate/domain checks, millisecond precision, latest selection and older-version protection |
| `run_validation.py` | Read-only standalone stage dispatch and structured reporting |

Blocking failures raise `ValidationError` (or a clear Spark schema/transport exception) and fail the invocation. Printing is never the enforcement mechanism. Failed required sources fail both single-source and `all` ingestion. Successfully written partial-run snapshots may remain for audit/recovery, but no successful manifest is published. API validation failures propagate immediately; transport/storage failures have bounded retries and result in blocking reconciliation failure. Invalid Silver scopes are rejected as a whole before MERGE; no invalid records are silently filtered. GeoNames invalid input rows are retained in timestamped `location_registry_rejected_*` tables and reported in import reconciliation; intentional feature filtering and duplicate reduction are separately counted.

Bronze preserves rescued data faithfully and reports a nonblocking warning after writing. Rescue mode **does not promote new fields into the expected schema**. Silver treats rescued data as blocking until an explicit schema remediation/backfill; it does not discard it. Null seasonally unavailable measurements alone are not warnings or failures. Compatible Spark null/range/timestamp checks share an aggregate; key uniqueness is checked separately.

### Inline and standalone use

Inline validation runs automatically at ingestion, staging, Bronze and Silver boundaries. The ingestion result prints its `run_id` and completed `manifest_path` under `raw_files/_runs/`. Pass that path as the Bronze notebook's `manifest_path` job parameter/widget. Bronze reconciliation includes already checkpointed files: a rerun must find exactly one table row per expected file even when the stream adds zero new rows.

Offline persisted-file validation needs no Spark and never refetches APIs:

```sh
python -m src.validation.run_validation --stage raw --source all --manifest /path/to/run.json
```

With an existing Databricks Spark session, use:

```python
from src.validation.run_validation import main
main(["--stage", "all", "--source", "all", "--manifest", manifest_path], spark=spark)
```

Stages are `registry`, `ingestion` (replay response validation from saved raw files), `raw`, `bronze`, `silver`, and explicit `all`. Source selection applies to Bronze/Silver; raw validation checks the complete required-source manifest so it cannot hide a failed required source. Registry is source independent. CLI errors exit nonzero. There is no implicit latest-run or whole-history validation mode.

Persisted raw files plus a manifest suffice for raw/API replay; Bronze checks need the manifest and tables; Silver checks reconstruct only manifest-scoped transforms and compare persisted targets without writing or advancing state. Registry validation can rerun from its persisted table; pass `--registry-backup BACKUP_TABLE` to verify activation/creation preservation against pre-refresh data. Original GeoNames filtering/rejection count reconciliation needs the initialization's in-memory import statistics; prepared/staged checks need those DataFrames at publication time. These checks cannot be reconstructed from only a final registry. HTTP responses that were never saved cannot be replayed. The standalone runner does not refetch them.

## Incremental processing, retries and recovery

Silver uses a durable completed-file ledger rather than an event-time or ingestion-time watermark. It projects Bronze file paths and anti-joins completed paths, then transforms only pending files. Late arrivals remain eligible regardless of timestamp. The pending path list is materialized for the batch; new arrivals during a run wait for the next run. The scoped Bronze and reused Silver outputs are cached and unpersisted in `finally` blocks.

All source outputs validate before the first MERGE. Completion is recorded only after every output write and persistence check succeeds. If a write or validation fails, completed state is not advanced. Some target tables may already contain the batch: rerunning the source repeats those same stable-key MERGEs and finishes remaining writes. A crash during ledger publication may replay additional files, which is safe. Earthquake latest MERGE updates only if `updated_at` is newer, or equal with an equal/newer snapshot; an older pending feed cannot roll back a newer event. History remains append/upsert by event and snapshot. An empty feed can complete with zero event rows.

Use **one writer per source**, including registry refresh/activation, and do not overlap schema migrations with ingestion. Cross-table writes are not a transaction; completion is the retry boundary, not cross-table atomic visibility. Downstream consumers must wait for successful source completion. Never mutate raw files after publication; filenames include stable IDs, unique run IDs and unique file IDs. Do not remove completed-file state, raw files, Bronze history or Auto Loader checkpoints as routine recovery. To rebuild Silver, back up targets/state, explicitly reconcile the rebuild scope and clear only the corresponding ledger entries in a controlled maintenance operation.

Temporary files live in `raw_files/_staging/`, outside all watched source directories, with unique `.tmp` names. Auto Loader also filters `*.json`. Final publication uses filesystem rename, but **atomic UC Volume rename behavior has not been verified and is not claimed**. Gate Bronze after ingestion and successful manifest validation, with no concurrent writer publishing raw files while Bronze discovers files. Auto Loader scans the source directory (including valid files left by prior partial runs); manifest reconciliation validates the selected completed run. This ordering is required until Volume publication behavior is verified in the target runtime.

Future scheduled task dependencies must explicitly gate raw → Bronze → Silver → downstream on successful completion, including successful validation. Running a standalone script cannot stop unrelated tasks. Configure those dependencies and single-writer limits when scheduling later; this task does not deploy or schedule anything.

## Historical data and schema migration

No routine Silver `DROP TABLE` or automatic destructive schema reset remains. Existing target schemas are checked before MERGE; differences fail with explicit migration instructions. Before adopting this version:

1. Back up existing registry, Bronze, Silver, checkpoints and processing state.
2. Audit historical raw/Bronze wrappers: `run_id` and stable `location_id` are now required. Backfill IDs only using an unambiguous, reviewed match against original location metadata/coordinates. Do not fabricate IDs or match solely on a repeated location name. Quarantine unresolved records outside the processing scope until resolved; they intentionally block default Silver processing.
3. Normalize historical file lineage if it uses a different URI spelling. New Bronze paths normalize `dbfs:/` and `file:/` prefixes to Volume paths. Raw manifests and persisted Bronze must refer to the same canonical paths.
4. Migrate existing Silver schemas and keys to the new transform schema, including `location_id`, correct numeric/boolean types, and earthquake epoch-millisecond lineage. Rebuild/backfill from retained Bronze in a controlled operation where incompatible types prevent a safe in-place change. No automatic type narrowing or history deletion is attempted.
5. Initialize/adopt the completed-file ledger only after validating existing targets against the intended historical scope. An empty ledger intentionally treats all retained Bronze files as pending. If validation fails, state remains unchanged.
6. If a registry refresh must be rolled back, restore its timestamped `location_registry_backup_*` snapshot after inspection. Deliberate activation resets are separate explicit administration, not part of refresh. Keep registry refreshes serialized because the staging table has a fixed name.

## Local verification

```sh
venv/bin/python -m unittest discover -s tests -v
python3 -m compileall -q src tests
```

Tests use synthetic data, mocked API clients, and injected persistence failures; HTTP access is blocked in the offline test fixture. They cover malformed/error responses, mismatched arrays, raw identity/files, activation arguments/SQL, deterministic coverage, failure propagation, notebook extraction, stable keys, retry/idempotency orchestration and older-event guards. Optional `test_spark_boundaries.py` tests run with locally installed PySpark and a compatible JVM; they exercise empty-registry rejection, activation preservation/selection, final location-ID projections and millisecond/zero-event transforms without Databricks.

PySpark and Delta are not installed in the current local environment. Spark integration, real Delta MERGE behavior, UC permissions, Volume publication, Auto Loader inference/checkpoints and live table migrations therefore still require verification in an isolated Databricks environment. No live API or Databricks execution was performed for this change.
