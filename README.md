# Weather data pipeline

Raw → Bronze → Silver environmental-data pipeline. Gold notebooks remain empty. No jobs, deployments, schedules, live API checks, or catalog changes are performed by local tests.

## Responsibilities and entry points

| File/directory | Responsibility |
| --- | --- |
| `src/config.py` | Complete nonsecret destination configuration and validation |
| `src/contracts.py` | API variable lists, required/optional field semantics, stable Silver column types |
| `src/ingestion/*_api.py` | HTTP transport and response validation; diagnostic `main()` entry points fetch/print but are not scheduled ingestion |
| `src/ingestion/pipeline.py` | Persisted request plans, extraction, explicit recovery and successful manifest publication |
| `src/ingestion/databricks.py` | Small task-value handoff adapter; job publication errors propagate |
| `src/ingestion/diagnostics.py` | Redacts diagnostic exception chains; stored endpoint metadata contains no URL |
| `src/ingestion/location_discovery.py` | GeoNames registry transformations/refresh, activation and Spark-based deterministic request selection |
| `src/ingestion/adoption.py` | Explicit reviewed historical-file adoption; preserves originals and records provenance |
| `src/transformations/schemas.py` | Explicit nullable Bronze envelopes; no optional-field inference dependency |
| `src/transformations/weather.py`, `air_quality.py`, `measurements.py` | Pure source flattening, array expansion and normalized output types |
| `src/transformations/earthquakes.py` | Feature expansion, millisecond timestamps and latest-event selection |
| `src/transformations/bronze.py`, `scope.py` | Auto Loader orchestration and exact committed-membership Spark joins |
| `src/transformations/silver.py`, `incremental.py` | Source validation, all-target preflight, idempotent writes, reconciliation and completion |
| `src/transformations/recovery.py` | Explicit bounded committed-manifest replay; never scans Bronze history |
| `src/validation/common.py` | Structured results/errors, reusable aggregate checks, schema/key checks, checked type normalization and persistence reconciliation |
| `src/validation/location_registry.py` | Input shape, valid IDs/names/coordinates/flags, refresh preservation, activation membership and counts |
| `src/validation/ingestion.py` | Pure API contract checks; no HTTP |
| `src/validation/raw.py` | Request-plan/commit membership, file digests, source/run/location coverage and wrapper/payload validation |
| `src/validation/bronze.py` | Explicit committed scope, schema/lineage/array checks and rescue policy |
| `src/validation/weather_silver.py`, `air_quality_silver.py`, `earthquakes_silver.py` | Source-key uniqueness, types/domains/timestamps, source-derived counts and persisted-target reconciliation |
| `src/validation/run_validation.py` | Executable `python -m` validation interface, or `main(argv, spark=spark)` inside Databricks |
| `notebooks/bronze/`, `notebooks/silver/`, `src/ingestion/save_raw_data.ipynb` | Thin executable notebook adapters; validation implementations stay in validation modules |
| `tests/` | Offline synthetic/mocked tests and optional real Spark/Delta integration tests |

Other Python modules are imported helpers, not independently scheduled tasks. Importing them does not launch API requests, Spark, streams, migrations, or persistence. `config/locations.yaml` is explicitly legacy and unused. `databricks.yml` currently defines only a development workspace target, with no jobs or schedules.

## Complete configuration and isolated setup

Default objects remain in catalog `weather_api`: schemas `reference`, `raw`, `bronze`, `silver`, `pipeline_state`. Registry defaults to `weather_api.reference.location_registry`; source is `/Volumes/weather_api/reference/location_files/US.txt`. Raw Volume defaults to `/Volumes/weather_api/raw/raw_files`; Auto Loader state Volume defaults to `/Volumes/weather_api/pipeline_state/autoloader_state`.

`PipelineConfig` covers every catalog/schema, registry/staging/backup/rejection name, source path, raw/staging/manifest/request-plan root, Auto Loader schema/checkpoint root, ledger name and per-manifest bound. Bronze tables use `<catalog>.<bronze_schema>.<source>_raw`; Silver suffixes retain the seven existing names. Defaults are backward-compatible object names, not an automatic historical migration.

Use `config/isolated.example.json` as a **complete** nonsecret configuration for catalog `weather_api_test`. Provision its schemas and Volumes first; do not assume the example exists in your workspace. Pass `config_path` to every notebook, or set `WEATHER_PIPELINE_CONFIG`, or pass a validated config object to Python helpers:

```python
from src.config import load_config
cfg = load_config('/Workspace/path/to/config/isolated.example.json')
```

An explicit JSON config must contain exactly all `PipelineConfig` fields. Missing fields fail rather than falling back to production. Identifiers, paths, separate schemas/control roots and bounds are checked before writes. `/Volumes/...` paths must agree with the configured catalog and reference/raw/state schema. Fingerprints bind each plan/manifest to its exact configuration; retain that config when retrying an old run. A per-manifest limit defaults to 1000 files (maximum configurable 10000).

`RAW_VOLUME_ROOT` remains a legacy raw-storage-only override. **It does not isolate the whole pipeline**; use a complete config for testing. Never point a test config at production state, targets or checkpoints.

Required permissions: catalog/schema usage, registry/source reads, Volume read/write, table create/DML, registry replacement/staging cleanup and backup creation. Runtime needs Unity Catalog Volumes, Auto Loader, Spark, Delta Lake (`delta.tables`) and timestamp/try-cast SQL functions. `requirements.txt` contains the remaining external Python dependency, `requests`; Spark/Delta come from the runtime. Python >=3.10 is required.

Imports use repository-root discovery, `WEATHER_PROJECT_ROOT`, or an installed package (`pip install -e .`). Secrets must be injected through process environment or your runtime's secret mechanism. Code no longer automatically reads `.env`; it remains ignored/untracked and must never be committed. URL overrides are `WEATHER_API_URL`, `AIR_QUALITY_API_URL`, `USGS_API_URL`. Persisted `api_endpoint` is the literal `external-api`, with source recorded separately: no hostname, path, query, userinfo or fragment is stored from an override. Diagnostics redact exception chains. Do not print raw HTTP exception objects elsewhere.

## Source contract and stable output schemas

`src/contracts.py` is the source of truth. Client request lists, validators, Bronze schemas and Weather/AQ transforms share its specifications. All current/hourly/daily Silver columns have explicit output types, independent of integer/float JSON representation.

| Source | Required | Optional/nullable |
| --- | --- | --- |
| Weather/AQ | Valid response grid latitude/longitude; UTC offset 0; current section and timestamp; hourly section and nonempty unique time array; weather also daily section/time array | All measurements, elevation, current.interval; optional measurement arrays may be absent or null |
| Earthquakes | FeatureCollection, metadata.generated/count, features list; each feature has nonempty unique event ID, Feature/Point structure, integer time/updated epoch milliseconds and three valid geometry coordinates | Other modeled event properties including magnitude; null measurements remain null |

Present measurement arrays must align with their time arrays. Optional absent arrays are expanded to same-length typed-null arrays before `arrays_zip`; they must not cause the entire forecast row set to disappear. Optional fields absent from **every initial file** still exist through the explicit Bronze schema. Original raw JSON is never augmented or rewritten to fill optional values. Unexpected fields are preserved in rescued data rather than silently promoted.

Continuous Silver values are double; stable IDs and original epochs are long; categorical/integer measurements are int; flags are boolean; forecast dates/timestamps use date/timestamp. Non-null invalid casts and fractional integer values block processing. Required identities/timestamps cannot become null. Casting checks combine compatible conditions in an aggregate. Bronze/Silver orchestration sets the Spark session to UTC; ingestion generates UTC retrieval timestamps. `timestamp_millis` and retained epoch columns preserve earthquake precision. Standalone pure transform callers must also set UTC.

API grid coordinates need not exactly match requested coordinates. Regional/seasonal unavailable measurements are permitted. A zero-event earthquake feed is valid, including as the first feed. Invalid records block the processing scope; they are not silently dropped. GeoNames rejected records are separately retained and reconciled.

## Registry setup, refresh and coverage

Run `src/ingestion/save_raw_data.ipynb` with `source=init_registry` as an explicit setup/refresh operation. Supply the official 19-column GeoNames tab-delimited file at the configured source path. The configured reference schema must already exist. Filtering retains populated US places and excludes the documented abandoned/historical codes. Input field counts, stable IDs, coordinates and nonempty prepared/staged output are validated. Refresh preserves flags and creation times for existing IDs, retains rejected rows, makes a timestamped backup, replaces the registry and cleans staging. IDs absent from the new dataset are absent from the refreshed registry. Refresh/activation must not overlap ingestion.

Then use `source=enable_locations` with `enable_limit` (default 20), `enable_weather=true`, `enable_air_quality=true`, and optionally `distributed=true`. Only `true` means true. Direct activation helpers accept explicit IDs instead of a limit; missing/inactive requested IDs block and report a bounded sample rather than silently succeeding. Activation enables selected flags; it is not a disabling interface.

Before HTTP, normal ingestion validates active registry candidates (IDs, names, coordinates, flags, timestamps, duplicate identities) with Spark. Source-enabled records are ranked by stable ID; only the selected bounded block is collected to Python. `max_locations` defaults to 20, and `coverage_slot` defaults to the UTC date ordinal. Block N is selected modulo the number of blocks. Reuse the same slot for retries; supply a monotonically increasing slot for other cadences. Membership changes alter the block layout. Failed slots must be recovered/retried for complete coverage.

## Successful-run publication: the eligibility boundary

Normal ingestion persists a v2 request plan **before requests**:

```text
<attempts_root>/<run_id>.json                        status=planned
<raw_root>/<source>/<run_id>/<location_id|global>.json
<manifest_root>/<run_id>.json                        status=committed
<staging_root>/<unique>.tmp                          never watched
```

The plan defines required sources and exact requested location identities, coordinates/names, expected per-source counts and output paths. One location/source appears at most once per run; one global earthquake feed is expected. Every requested file must validate before a successful manifest can publish. The committed manifest records the complete immutable membership plus SHA-256 per file and config fingerprint. Every downstream invocation rereads that exact record and verifies payloads/digests. A file's presence, a plan, or a status print is never proof of eligibility.

**New uncommitted files are excluded from Bronze entirely.** Auto Loader reads only the selected committed run's source directory, then inner-joins exact manifest membership. It cannot discover another failed run from a broad source-root scan. Unlisted files inside a committed run are also excluded. Silver likewise joins the exact committed manifest against Bronze; it never discovers all unfinished Bronze history. Old uncommitted Bronze records are retained but are not selected by normal manifest-scoped processing.

Manifest validation rejects unsupported sources, duplicate files/identities, missing coverage, foreign run IDs, file source/location mismatch, inappropriate roots, mutated files and legacy/uncommitted records. Raw storage directories alone cannot authorize processing. There is no “latest manifest” lookup.

No atomic multi-file or Volume rename guarantee is claimed. Files are staged separately and renamed one at a time; a commit is eligible only after the actual serialized commit record and every listed file revalidate. Interrupted/corrupt destinations block. Never delete partial data automatically. Preserve/archive an incomplete destination under an audit name, inspect/repair it explicitly, then resume its saved plan. An interruption before commit leaves no eligible run. An interruption after a valid commit allows idempotent reuse of the exact same manifest.

## Exact invocation and future task handoff

Run the ingestion notebook with:

```text
config_path = /Workspace/.../config/isolated.example.json
source = all                       # weather / air_quality / earthquakes also supported
max_locations = 20
coverage_slot = <optional explicit nonnegative slot>
job_execution = true               # recommended explicit job parameter; auto is default
```

The returned structure contains `run_id`, `manifest_path`, `status=committed`, `saved_files`, and `sources`. The adapter publishes task values named **`run_id`** and **`manifest_path`** only after durable commit validation. Interactive execution needs no job context. Auto-detection uses the notebook context's jobId; use `job_execution=true` on job tasks if context inspection is restricted. Task-value write failures are not suppressed. The committed run is recoverable even if handoff then fails.

Future downstream notebook parameters, assuming the ingestion task key is `ingest_all`:

```text
manifest_path = {{tasks.ingest_all.values.manifest_path}}
config_path = <same complete configuration path used by ingestion>
```

Pass these to **both Bronze and Silver**. Bronze adapters call `run_bronze(spark, dbutils, source)`. Silver adapters read the parameters and call:

```python
run_silver(spark, source, manifest_path, config)
```

The run ID is available as `{{tasks.ingest_all.values.run_id}}` for diagnostics; scope comes from the explicitly supplied manifest, not that ID alone. Registry initialization/activation return setup statistics and do not publish ingestion task values.

Future task order:

```text
manual registry_initialize -> manual registry_enable
recurring ingest_all
  -> bronze_weather     -> silver_weather
  -> bronze_air_quality -> silver_air_quality
  -> bronze_earthquakes -> silver_earthquakes
```

Use **Run if: All succeeded** on every downstream dependency. All-source consumers depend on all three successful Silver branches. Branches exchange persisted files/tables and explicit manifest paths, never shared Python variables/DataFrames. Separate per-source ingestion tasks may have independent manifests. A validation script cannot stop unrelated tasks unless task dependencies enforce it. No jobs or schedules are created here.

## Incremental scope, preflight and failure recovery

Normal runs process exactly one bounded committed manifest and selected source. State is `<catalog>.<state_schema>.<ledger_name>`, with `(source,path)` identity plus run ID, batch ID and completion time. There is no historical Bronze-path discovery, timestamp watermark or unbounded backlog collection. Auto Loader schema/checkpoint paths are `<configured_root>/<source>/<run_id>`, isolating retries and test environments.

Silver validates Bronze membership and every transformed output, then preflights **all existing targets** for exact schema compatibility, relevant-key uniqueness and Delta-table accessibility before the first output write. Predictable later-target incompatibility prevents earlier writes. Missing targets are created only in write mode. Permissions and runtime failures can still occur after preflight; no cross-table transaction is claimed.

After writes, scoped target reconciliation must succeed for every output before completion is marked. Unexpected partial writes leave state unadvanced; rerunning the same manifest replays its immutable source scope with stable-key MERGEs. Weather/AQ keys use location ID + snapshot + forecast key. Earthquake latest uses event ID and updates only for a newer update timestamp, or equal update with an equal/newer snapshot. History uses event ID + snapshot. A completed manifest replay revalidates target persistence without repeating output writes. Cached frames are unpersisted in finally blocks.

`mark_completed=False` explicitly writes/reconciles outputs but leaves completion state unchanged. `validate_only=True` creates no schemas/tables/state and writes no output; it validates source/preflight and checks existing persisted targets. On an initial run with absent targets it can only validate proposed outputs, not prove persistence. The removed `commit=False` interface is unsupported.

**Start with one active pipeline run, one writer per source, and no overlapping registry refresh/migrations.** No lock service or distributed concurrency guarantee is implemented. State schema/conflicting run membership and relevant duplicate targets are checked, but simultaneous first writers cannot be reliably prevented by these checks. Configure one active job run when scheduling later.

Explicit recovery APIs:

```python
from src.ingestion.pipeline import recover_partial_run
# All files exist: commit without refetching.
recover_partial_run(cfg.attempt_path(run_id), cfg)
# Operator explicitly permits requests for missing files only:
recover_partial_run(cfg.attempt_path(run_id), cfg, fetch_missing=True, spark=spark)
```

The ingestion notebook also supports `recovery_run_id=<saved plan ID>` to resume missing requests and republish task values. Missing Weather/AQ requests also revalidate the current registry against the saved IDs/coordinates/names and activation flags before HTTP. Existing valid files are reused; invalid existing files block and require reviewed repair. Retries preserve original saved snapshots and request membership. Provider responses fetched on recovery may have later retrieval times, which remain explicit per-file lineage.

```python
from src.transformations.recovery import recover_committed
recover_committed(spark, [exact_manifest_path_1, exact_manifest_path_2],
                  'weather', cfg, max_runs=10)
```

Recovery lists must be explicit, unique and bounded (default 10 runs, maximum 100), processed in deterministic run-ID order. Each manifest is independently file-bounded. Newly committed late runs are processed when explicitly supplied; no event-time watermark can skip them. No filesystem “latest” lookup or global backlog scan is added.

## Historical adoption and schema migration (manual approval/decisions required)

Normal runs never select historical Bronze merely because the ledger is empty. Legacy manifests are rejected. Do not mark old paths completed to bypass missing IDs or validation. Keep original raw files, Bronze tables and checkpoints intact.

Before adoption, back up registry/targets/state and decide mappings for historical snapshots. Names alone cannot identify repeated locations. Unresolved IDs, missing retrieval timestamps, coordinate conflicts and incompatible target schemas require operator decisions; default execution fails clearly rather than guessing.

An executable adoption procedure is available, without APIs:

```python
from src.ingestion.adoption import adopt_reviewed_files
result = adopt_reviewed_files('weather', [
    {'legacy_path': '/Volumes/.../archived/weather_old.json',
     'location': {'location_id': 123, 'name': 'Reviewed location',
                  'latitude': 40.0, 'longitude': -90.0}},
], cfg)
```

The operator must verify that mapping against registry IDs/original coordinates. Each cohort permits at most one snapshot per location/source; split multiple snapshots into separate cohorts. For a missing original retrieval timestamp, supply an explicit `reviewed_retrieval_timestamp` with offset after investigating provenance; do not substitute the current time. Earthquake adoption needs one global feed and no location mapping. Originals are never modified. Adoption copies unchanged raw responses into modern wrappers, records original path/hash and reviewed mapping in the plan/manifest, and publishes a committed run **without marking Silver completion**. Process that exact manifest through Bronze/Silver normally. If adoption publication is interrupted, resume its plan.

For old populated targets/empty ledgers, normal scoped MERGEs and reconciliation verify adopted rows before state can advance. Existing Silver schemas must first be explicitly migrated to contract types/keys and epoch lineage (`event_time_ms`, `updated_at_ms`, `feed_generated_ms`). No automatic DROP TABLE, type narrowing, ID guessing, or whole-history rebuild occurs. Data-preserving ALTER/backfill or a reviewed new target schema/catalog is required; preflight supplies failure diagnostics. Do not overwrite old Auto Loader state with new input roots: v2 run-qualified checkpoints/schemas are intentionally separate. Existing Bronze table schemas may also need explicit migration or isolated new targets before v2 append can work.

Registry refresh still deliberately overwrites its staging table, backs up/replaces the registry, and drops only staging. Those setup operations require serialization and adequate permissions. Backups/rejection tables require an operator-defined retention policy; no cleanup job is created.

## Runtime validation and standalone checks

Validation returns structured checks with stage, check name, severity, pass/fail, affected rows where applicable, details and run/source context. `ValidationError.result` carries a failed structured result. Compatible Spark checks share aggregates; duplicate keys use a grouped aggregate. Orchestration prints final success only after required boundaries succeed. Warnings have warning severity and do not silently become success enforcement.

Bronze preserves rescued data and reports a warning after persistence; Silver blocks rescued scopes until reviewed remediation. Required structure/membership errors block even with Bronze's warning policy. Rescue mode does not promote unknown fields.

Inline pre-write validation is authoritative for in-memory outputs and staged registry data. Persisted committed manifests/raw files allow independent API/raw replay; Bronze membership and Silver reconstruction/target checks require Spark. Original GeoNames filtering statistics and pre-refresh preservation require in-memory import context or an explicitly supplied backup; a final registry alone cannot reconstruct every initialization check.

```sh
python -m src.validation.run_validation --stage raw --source all \
  --manifest /Volumes/.../_runs/EXACT_RUN_ID.json \
  --config /Workspace/.../config/isolated.example.json --run-id EXACT_RUN_ID
```

```python
from src.validation.run_validation import main
main(['--stage','all','--manifest',manifest_path,'--config',config_path], spark=spark)
```

Supported stages: registry, ingestion (saved-payload replay), raw, bronze, silver, all. Sources: weather, air_quality, earthquakes, all (default). `--manifest` is required except registry; `--registry-backup` is optional for preservation checks. `--run-id` checks the selected record. `--config` uses the same strict configuration. Raw checks validate the entire required-source manifest. Silver standalone mode is read-only and requires every persisted target to exist; it fails rather than validating only proposed outputs. Direct `run_silver(validate_only=True)` may validate proposed outputs before initial publication as described above. CLI validation failures exit nonzero with structured output. No API calls, target writes or completion updates occur in standalone validation.

## Local checks and precise isolated integration testing

```sh
PYTHONDONTWRITEBYTECODE=1 venv/bin/python -B -m unittest discover -s tests -v
python -m src.validation.run_validation --help
git diff --check
```

Tests block live HTTP and use synthetic files/mocked task plumbing. They cover failed-run exclusion, interrupted commit publication, explicit partial recovery, config isolation, identity/digest mismatch, task values, reviewed adoption, validation propagation, all-output preflight ordering and partial-write replay. Mocked tests do not establish Auto Loader or Delta guarantees.

PySpark and Delta are unavailable in this local environment. Optional Spark tests exercise actual schemas/transforms, integer/float/all-null/missing optional fields, invalid casts, registry preservation/activation, zero-event feeds and millisecond precision. Optional Delta tests call the **production `merge_output` SQL path**, including older-version protection and repeated writes after a partial multi-table publication. They require preinstalled local JVM jars/extensions; tests never download dependencies/Maven artifacts.

Before live scheduling, in an isolated Databricks environment:

1. Provision the example test catalog/schemas/Volumes; verify every configured path and permission. Install this checkout/requests and make helpers importable.
2. Create synthetic raw wrappers/request plans using `tests/helpers.py` concepts or `make_plan`/`save_json`/`publish_run`; no API is needed. Include a failed uncommitted plan and a later committed run.
3. Run each real Bronze adapter with exact manifest/config parameters. Confirm only committed listed files appear, then rerun the same manifest and confirm checkpointed files reconcile without duplicates. Confirm failed/unlisted files stay excluded.
4. Run Silver first with `validate_only=True`, then normally on scratch targets. Compare schemas across integer/float/null fixtures, assert millisecond precision and a valid zero-event result.
5. Seed a later output with an incompatible schema: run Silver and verify earlier output counts/content and completion state are unchanged. Correct it with a reviewed migration and retry.
6. Inject a failure after an early table MERGE but before later writes; confirm no completion rows, then rerun the same manifest and verify every output and ledger.
7. Feed an older and then newer earthquake version through production `merge_output`; verify the older cannot overwrite the target and repeated versions do not duplicate events.
8. Verify automatic/explicit job context task values, Volume publication behavior, and task parameter wiring with a small manually started test run. A blocking upstream validation must leave downstream All-succeeded tasks unexecuted.

These Databricks steps were not run. No successful live execution is claimed.
