# Weather data pipeline

Raw → Bronze → Silver → Gold environmental-data pipeline. Gold is an explicit, unscheduled three-notebook release pipeline restricted to explicitly approved catalogs: `weather_api.gold` (production) and `weather_api_test.gold` (test). No jobs, deployments, schedules, live API checks, or catalog changes are performed by local tests.

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

`config/production.json` is the complete nonsecret production configuration for catalog `weather_api` (same schema/volume layout, no secret values). Gold deployment catalogs are an explicit allowlist (`APPROVED_GOLD_CATALOGS` in `src/validation/gold.py`): `weather_api` must use the canonical `gold` schema, while `weather_api_test` additionally permits disposable `gold_verify_<12 lowercase hex>` schemas. All three Gold notebooks take an explicit `catalog` widget with a blank default — a blank catalog fails validation instead of silently defaulting to the test catalog, and the Gold target catalog must equal the configured source (Silver/registry/ledger) catalog; mixed-catalog input/output is rejected. The first production Gold initialization is the `FIRST_RUN_EMPTY_STATE` provisioning plan (`migration_plan`/`_legacy_migration_plan` with `session=None`); the V1→V2 migration applies to the test deployment only. Changing the Gold notebooks or `src/validation/gold.py` changes the notebook contract hash: the existing `weather_api_test.gold` deployment keeps its deployed contract until an explicit reviewed re-migration is run; it is never silently overwritten.

Seismic country attribution: `seismic_events_latest` (and its `_history` view) carry `country_code` (ISO 3166-1 alpha-3) and `country_name`, attributed via a precomputed `<catalog>.reference.country_h3_cells` table — H3 resolution-6 polyfill of the public-domain Natural Earth 110m admin-0 boundaries (v5.1.2; provenance and license recorded in `reference.country_boundaries`). Events with no land cell (offshore or outside coverage) carry explicit `UNASSIGNED` values, never NULL; boundary-shared cells are deduplicated deterministically (`min(country_code)`). `src.validation.gold.country_cells_relation` fails closed when the reference table is missing, and `validate_country_attribution` enforces the code format before any write.

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

## Targeted reconciliation and repair regressions

The changes below tighten validation without changing committed membership, stable
keys, output schemas, configuration fingerprints, or the optional-field policy.

### Earthquake version and full-content reconciliation

`validate_earthquake_target()` uses the same ordering as production MERGE:
`updated_at`, then `snapshot_at`. A missing event or an older target fails. For an
identical version it compares **every expected persisted column**, including nullable
measurements, coordinates, status, arrays and lineage, using null-safe equality.
Plausible but changed content can no longer pass on timestamp ordering alone.

A genuinely newer target is retained during older-manifest replay. Its content must
match the exact newer history version. Persisted history is only a locator: the
orchestrator's `verified_newer_history()` locates its committed run from file lineage,
checks manifest membership/digests and Bronze membership, rebuilds from digest-verified raw JSON (using Bronze only for validated ingestion lineage),
reruns the production transform, reconciles persisted history with that independent result, and supplies
that reconstructed evidence explicitly to latest-target validation. The target is
never its own reference. Missing/inconsistent evidence fails with an instruction
to select the relevant newer committed scope. Evidence reconstruction is read-only
and bounded to ten newer source files/runs per invocation; larger scopes require
explicit recovery of the relevant newer runs first. No live APIs are refetched.

This means old replay now requires retained raw files, their commit records, Bronze
lineage and exact newer event history. Legacy uncommitted lineage must be explicitly
adopted. Corruption in both latest and persisted history is not accepted merely
because the two agree: history is checked against committed inputs.

### Default replay versus explicit scoped repair

Default replay still skips writes for completed files and validates persisted
outputs. Deletion/corruption fails; it is not automatically rewritten. Deliberate
repair uses the exact same trusted manifest/configuration and all source validation,
all-output preflight, production MERGE and post-write reconciliation:

```python
from src.transformations.silver import run_silver
result = run_silver(spark, 'weather', manifest_path, cfg, repair=True)
```

The Silver notebook adapters also accept `repair=true` (default `false`) with the
same `manifest_path` and `config_path`. Standalone `run_validation` remains read-only
and exposes no repair flag. `repair=True, validate_only=True` is rejected before
processing. Repair does not remove or clear ledger rows. Results explicitly report
`repair_requested`, `written`, `reconciled`, recreated tables and restored run scope.
A partial repair can leave some writes in place; explicit retry replays stable keys.
There is no cross-table rollback or transaction guarantee.

Same-version earthquake corruption is replaced from the selected committed input.
An older scope cannot overwrite a newer event. If that newer target is corrupt,
older repair fails before writes and asks for the newer scope; it does not guess
or roll back the event. Select and repair the newer committed manifest, then retry
the old replay if needed.

Missing historical tables require a separate deliberate reconstruction mode:

```python
from src.transformations.recovery import recover_committed
results = recover_committed(
    spark, [exact_manifest_path_1, exact_manifest_path_2], 'weather', cfg,
    repair=True, rebuild_missing=True, max_runs=10,
)
```

Before a missing table is created, the supplied bounded, unique, committed scopes
must cover **all completion-ledger paths/run IDs for that source**, and the current
manifest must be included. A single manifest cannot recreate a multi-run table if
other completed scopes exist. Missing or unverified completion history blocks
reconstruction; use reviewed adoption/new targets rather than bypassing it. Tables
can be partially reconstructed if a later scope fails; rerun the complete explicit
list under the single-writer rule. Reports identify selected/restored run IDs and
always avoid claiming universal full-history restoration. The coverage boundary is
verified ledger history, not unknown legacy rows outside that history. Preserve
backups and reconcile any legacy population separately. No ledger clearing is used.

### Timestamp acceptance before commit

Accepted forms are explicit and Spark-compatible:

| Field | Accepted format |
| --- | --- |
| Retrieval timestamp | `YYYY-MM-DDTHH:MM[:SS[.ffffff]]` with `Z` or `+/-HH:MM` required |
| Current/hourly | Same extended datetime syntax; offset optional |
| Daily time | Exactly `YYYY-MM-DD` |
| Optional sunrise/sunset | Same extended datetime syntax as current/hourly, or null |

Fractional seconds contain 1–6 digits and require a seconds component. Naive API
datetimes mean UTC. Supported offset-bearing strings are interpreted as their UTC
instant, not rewritten in raw JSON. Offsets are limited to Java/Spark-compatible
`-18:00..+18:00`, with minutes <60 and no minutes beyond 18 hours. Impossible dates,
empty/null required values, compact ISO (`20261021T000000`), excessive fractional
precision, and unsupported formats fail. Pipeline-generated UTC retrieval timestamps
remain valid. Daily dates cannot carry times/offsets.

Hourly uniqueness uses normalized UTC instants: `Z`, `+00:00`, naive UTC and a
nonzero offset representing the same instant are duplicates and fail **before
commit**. Daily uniqueness uses parsed dates. Silver key validation remains a second
gate. No duplicate is dropped or arbitrarily selected. Original raw JSON is preserved.

### Shared domains, completeness and row metrics

`AQI_FIELDS` derives supported fields from the shared contract. All six US component
indices (`us_aqi_pm2_5`, `us_aqi_pm10`, `us_aqi_ozone`, `us_aqi_nitrogen_dioxide`,
`us_aqi_sulphur_dioxide`, `us_aqi_carbon_monoxide`) plus aggregate indices must be
nonnegative when present, in current/hourly API data and Silver. Zero and null are
valid. No upper bound or nonexistent European component columns were added.

Both raw daily arrays and Silver daily rows enforce `temperature_2m_min <=
temperature_2m_max` and `apparent_temperature_min <= apparent_temperature_max` when
both values exist. Equal values and either/both null are valid. Structured errors
name the failed relationship; values are never swapped or corrected.

`aggregate_rules()` combines compatible rules in one aggregate:

- `affected_rows`: distinct rows failing at least one rule, null predicates treated
  as false unless the rule explicitly checks null presence.
- Per-rule counts remain available in `per_check` failure details.
- `failed_check_occurrences`: sum of per-rule failures; may exceed affected rows.
- Cast checks use the same definition; two bad casts in one row affect one row.
- Duplicate-key checks separately count every row in a repeated-key group (not only
  the excess rows). Different key groups are disjoint, so this count does not overlap.
- Empty frames have zero affected rows; source-specific nonempty policies still apply.

All optional measurements remain optional. `measurement_completeness()` emits a
nonblocking warning/count of rows missing core measurements: weather current/hourly
temperature or daily min/max; AQ PM2.5, PM10 and aggregate US AQI. A measurement-empty
output can have valid structure. Other useful measurements may still be present;
this is a core-completeness indicator, not a universal usefulness judgment. Regional
and seasonal nulls remain valid. A future configurable blocking coverage threshold
would belong in source validation plus explicit versioned configuration, not an
implicit ingestion or notebook filter. No such threshold is imposed here.

Previously committed invalid AQI, contradictory temperatures or newly unsupported
timestamp formats now fail replay gates. No files, manifests, tables or state are
rewritten automatically. Preserve originals and perform reviewed replacement/adoption
where needed. No new Silver schema migration is introduced by these targeted fixes.

### Separate Databricks / serverless Spark Connect runner

Local tests remain local. The separate thin entry point is
`notebooks/integration/regressions.ipynb`, importing `src.integration.runner` and using
the existing `spark` session:

```python
from src.integration.runner import run_regressions
integration_result = run_regressions(
    spark, '/Workspace/.../config/isolated.example.json'
)
```

Or manually execute that notebook with widget/job parameter `config_path` set to
the complete isolated JSON file. This runner is **not scheduled or executed by local
checks**. It refuses missing configs, the default production catalog, catalogs
without a test/sandbox marker, nonmatching Volume roots and existing suite tables
or schemas. Before its first write, it derives/checks unique suite-specific Bronze/
Silver schemas, ledger name, raw/control/state subdirectories inside the explicit
test catalog. Provision the base test Volumes/schemas first. Base existing test
pipeline tables and checkpoints are not reused. Inputs/configuration must still be
reviewed by the operator; naming guards are not an authorization system.

Fixtures are synthetic and HTTP is explicitly blocked. All Spark fixture creation
uses the supplied session (`createDataFrame` with explicit schemas / `range`);
there is no local-session construction, `sparkContext`, shared-session stop or
automatic cleanup. The runner calls production transforms, validation, Auto Loader
and MERGE. Test-only mocks inject early-write and post-write failures; there is no
production failure-injection switch.

It covers full-content/version relationships, distinct-row metrics, timestamp
parsing, all supported AQI components, daily relationships and optional completeness;
two committed runs with fewer locations in the second; older replay/newer evidence;
checkpoint replay; failed uncommitted input exclusion; preflight schema mismatch;
partial-write and post-write failure/retry; completed-row repair; zero events; and
bounded missing-table reconstruction. Exact synthetic inputs/plans, assertion
outcomes, expected/unexpected exceptions, effective config and before/after table/
ledger snapshots are saved under the returned `artifact_root` (`results.json`,
`effective-config.json`). Test-created mismatch/backup tables and raw artifacts are
retained for inspection. An interrupted report is not a successful test result.
Cleanup requires a separate explicit operator action, limited to the recorded suite.

Run locally:

```sh
PYTHONDONTWRITEBYTECODE=1 venv/bin/python -B -m unittest discover -s tests -v
```

`tests/test_targeted.py` exercises pure pre-commit contracts. Optional
`tests/test_targeted_spark.py` uses the same Spark Connect-compatible regression
cases with a local session. Existing local Spark/Delta tests remain separate.
Skipped Spark tests are not passed tests; mocks do not establish Databricks behavior.
Do not run the local-session tests on the shared serverless session.

A separate **manual, unscheduled job-gating test** still must be performed later:
configure ingestion → each Bronze → corresponding Silver with the exact task-value
manifest/config wiring and **Run if: All succeeded**. Use only isolated targets,
start one small run manually, verify `run_id`/`manifest_path` task values and checkpoint
replay, then introduce a controlled invalid synthetic fixture in a test-only adapter
and confirm downstream tasks are skipped. Keep this independent of the integration
notebook's in-process mocks. Do not create a schedule, do not use production data,
and do not treat the in-process runner as proof of job dependency behavior.


## Gold: notebook-owned data products and hardening v2

**Deployment status:** the repository now requires an explicitly reviewed v2 control/view
migration. The existing test-catalog deployment is still v1. This hardening pass did
not migrate or publish another release in weather_api_test.gold, or change existing jobs.
Disposable-schema execution is recorded separately below. Do not execute the
new write paths against v1 controls: they deliberately fail before persistent writes.

### Ownership and retained products

Transformations and orchestration remain in the three notebooks, never in a `src/gold`
package. Gold validation stays in `src/validation/gold.py`. Tests execute the actual
notebook definitions through the canonical bootstrap, excluding widget/publication cells.

| Owner | Existing public products | Grain/business key |
| --- | --- | --- |
| Weather | `dim_location` | location_id |
| Weather | `weather_current_latest` | location_id |
| Weather | `weather_hourly_latest` | location_id, full UTC forecast_for |
| Weather | `weather_daily_latest` | location_id, UTC forecast_date |
| AQ | `air_quality_current_latest` | location_id |
| AQ | `air_quality_hourly_latest` | location_id, full UTC forecast_for |
| AQ | `weather_aq_coverage`, `weather_aq_freshness` | source, identity_key |
| AQ | `environment_hourly` | location_id, full UTC forecast_for |
| AQ | `location_current` | location_id |
| AQ | `sampled_location_hourly_summary` | full UTC forecast_for |
| Earthquakes | `seismic_events_latest` | event_id |
| Earthquakes | `earthquake_snapshot_audit` | event_id, snapshot_at |
| Earthquakes | `seismic_coverage`, `seismic_freshness`, `freshness_coverage` | source, identity_key |
| Earthquakes | `seismic_daily_summary` | UTC occurrence date, event_type, magnitude_type |
| Earthquakes | `seismic_grid_daily` | daily key plus grid_cell_id |
| Earthquakes | `location_seismic_nearby` | location_id, event_id |
| Earthquakes | `publication_status` | release_id |

All public products are ordinary views. Ten selected products have release-qualified
`_data_<name>` Delta copies: dimension, three Weather, two AQ, Weather/AQ coverage,
latest seismic, snapshot audit, seismic coverage. Physical logical keys include
`_release_id`; runtime validation enforces uniqueness, not database primary-key constraints.
Other views derive results from those copies. No new measurement products were added.

V2 adds `<product>_history` views for all **19 business products** above (everything
except publication_status), plus `published_releases`. These expose existing retained
copies without another data copy. History grain includes `_release_id` plus the business
key. Original active views/column contracts remain available. These new interfaces are
implemented in code and tested only in disposable schemas; **the existing gold deployment remains unmigrated**.

### Successful-source eligibility and upstream trust

Inputs remain only `weather_api_test`: seven Silver tables, reference.location_registry,
and pipeline_state.silver_completed_files. Default output is weather_api_test.gold; disposable verification may explicitly use weather_api_test.gold_verify_<12 lowercase hex>.
PipelineConfig's exact fields/fingerprint are unchanged. Gold-specific parameters are
separate. Weather pins nine Delta versions; later stages use the recorded pins. The
latest earthquake table is pinned for stability but latest event state is reconstructed
from eligible history, ordered by updated_at DESC, snapshot_at DESC, source_file_path DESC.
This preserves an earlier eligible version shadowed by an incomplete latest-table write.

Each ledger-referenced run's exact manifest must match the configuration fingerprint.
Every file across all sources declared by a manifest must be completed under that run
before the cohort is eligible. Source-specific and multi-source manifests both work;
a three-source run is one discovered run, while three source-specific runs consume three.
Partial cohorts are excluded and reported. Duplicate/orphan/conflicting completion
membership blocks. Completely ledger-unreferenced manifests are not discovered; monitor
upstream completion separately. Gold does not infer completion from Silver row existence.

Exact raw digests, wrappers/API structures, unit metadata, retrieval/location/file
membership, source-array expansion counts, Silver schema/domains/keys are checked.
**A raw digest proves raw integrity, not that every Silver business value equals raw.**
Ledger completion and correct upstream transformations remain trust assumptions. Gold
counts/domains do not establish every value's fidelity. Independent known-value fixtures
check representative identity/time/weather/pollutant/event values across the raw/Silver
boundary and Gold selection. They do not execute or independently prove the upstream
PySpark transformations. No full duplicate Silver transformation was introduced.

All eligible retained valid hours/dates remain. Latest-per-hour selection can mix
retrieval vintages; timestamps/lineage stay visible. Duplicate logical source versions,
including identical duplicates, block before row_number selection.

### Bootstrap, parameters, and task handoff

Execute **weather_gold -> air_quality_gold -> earthquakes_gold** sequentially.

The Weather notebook's `gold_bootstrap` cell defines the canonical loader. Parameter
cells locate that source, validate the checkout, and put its root on sys.path. Later
notebooks load only predecessor `gold_definitions` cells. Missing/malformed JSON,
wrong tag counts/types, duplicate definitions/loading, unexpected predecessor requests,
and executable top-level definition statements fail with actionable errors.

| Parameter | Default / interpretation |
| --- | --- |
| project_root | Blank: discover upward from current directory; override must be absolute |
| config_path | config/isolated.example.json; relative to root, or absolute |
| validate_only | true; false enables guarded persistence |
| catalog / gold_schema | weather_api_test / gold by default; only gold_verify_<12 lowercase hex> is additionally allowed for isolated verification; set the same schema on all three notebooks |
| nearby_radius_km | 250; positive finite, <=20000 |
| alignment_seconds | 900; positive finite |
| grid_degrees | 2; positive <=30, divides180 |
| stale_seconds | 10800; positive finite |
| max_runs | 100; deliberate integer1..1000 |
| job_mode | Weather only: auto/true/false; default auto detects jobId |
| release_id | AQ/Earthquakes required exact Weather result; no latest fallback |

AQ/Earthquake settings are loaded from the immutable candidate receipt. Weather returns
structured results and, **only after persistence/reconciliation/VALIDATED receipt**, sets
`dbutils.jobs.taskValues.set(key='release_id', value=...)` in job mode. Exceptions and a
False return propagate as failures, following the existing ingestion handoff convention.
Interactive execution requires no job context; set job_mode=false when appropriate.
Auto detection errors propagate rather than disguising a job handoff failure.

Initial validate_only Weather produces no persisted receipt and publishes no ready task
value. Result.ready_for_next_stage=false. Dependent validation requires an already
persisted successful predecessor, so an all-validation-only initial chain is unsupported.
Validation-only may use session temporary views and SET TIME ZONE, never persistent writes.

Future job parameters (documentation only; no job files changed):

- Weather: job_mode=true, validate_only=false, explicit root/config.
- AQ: release_id=`{{tasks.weather_gold.values.release_id}}`, validate_only=false.
- Earthquakes: the **same** Weather task value, validate_only=false.
- All required Silver tasks All-succeeded -> Weather -> AQ -> Earthquakes.
- Gate dashboard refresh on final successful publication; concurrency1 for the sequence.

Use a checkout with readable .ipynb **JSON source files**, including all three notebooks
and src. Some Databricks Git-folder notebook representations do not expose that JSON
at the assumed path: sync/export a real source checkout and set project_root. Root
resolution was tested locally from repository, tests, and notebooks/gold directories;
actual Databricks Git-folder/widget execution remains unverified. No personal paths or
test-module imports are used at runtime.

SQLExecutor accepts either an existing SQL Connector connection or Spark .sql().collect().
SparkExecutor is an alias of the same adapter, not a second engine. No SparkSession
creation/stopping or sparkContext is used. Mock adapter tests do not prove remote Spark.
For local SQL tools only, optionally install `.[gold-sql]` in a local virtualenv.
Do not install SQL Connector into Databricks runtime merely to run existing Spark SQL.
The personal connection-check script, .env, and .venv-databricks remain ignored.

### Fenced writer and publication ordering

Controls must be provisioned/migrated explicitly; regular stages never bootstrap a
singleton with a racy check-then-insert. First-run validation can operate without state;
first persistent use requires the reviewed empty-state provisioning plan.

V2 control additions:

- _gold_active: generation BIGINT, writer_token STRING, writer_stage STRING,
  writer_acquired_at TIMESTAMP, deployment_contract STRING.
- _gold_releases: expected_predecessor STRING, expected_generation BIGINT,
  evidence_id STRING, publication_state STRING, health STRING, verified_at TIMESTAMP.
- _gold_stages: output_versions STRING (JSON product -> persisted Delta version).

A complete control snapshot is validated before acquiring a writer and again after
acquisition/before activation. Exactly one singleton=1, valid identities, recognized
stage names/statuses, unique receipts, matching config/pin identities, predecessor
references and validated published stages are required. A reconciled legacy active release may retain UNKNOWN health while remaining explicitly unverified; it is excluded from published history. Schema/
ownership verification is reused within one invocation; it is not cached across runs.

Each write-enabled invocation acquires one **non-expiring** guard with a conditional
Delta UPDATE of the singleton WHERE writer_token IS NULL, then verifies its random
ownership token. This is an atomic conditional update, not check-then-insert. It is held
through the stage, including data writes/publication/reconciliation, and released by
an owner-conditioned UPDATE. Busy writers fail; there is no unsafe automatic expiry,
background takeover, or promise of distributed safety from mocked tests.

Normal mutation paths check the held token before submitting SQL; the pointer UPDATE
also checks it atomically. A token in one table does **not** atomically fence writes to
other Delta tables, cancel submitted queries, or constrain manual/external writers.
Therefore recovery requires confirmed owner termination and terminal state for **all**
submitted statements. The guard protects cooperating invocations only while operators
respect that rule. Ownership ends at each notebook boundary, not at the end of the whole
three-notebook release; predecessor/generation and immutable release-qualified rows make
interleaved candidates fail closed at activation. Deployment concurrency1 remains defense
in depth. Isolated SQL contention is tested separately from actual hard-process termination.

Candidate product data is insert-once in **one Delta transaction per product**. Existing
rows are reconciled exactly, never DELETE/reinserted. Empty outputs are harmlessly
rechecked. Active copies and validated active stage receipts are never rewritten.
An incomplete/mismatching inactive copy fails closed; do not change its data under
the same release identity. Investigate/quarantine and prepare a new fenced candidate.

Weather captures predecessor ID **and generation** under the guard. They are persisted
and included with evidence_id in the release identity. Same-input/configuration
candidates are still ordered by this fence. Later stages cannot replace that expectation
with the current pointer. Activation compares ID, generation, writer token and increments
generation. Rollback increments it again; an away-and-back pointer cannot resurrect an
obsolete candidate. Same-evidence already-active replay is allowed and checks copies.
No-op replay still acquires/releases the guard and performs reads/TEMP DDL; do not claim
all control Delta versions remain unchanged under v2.

Before activation: complete source/pin/prerequisite checks, latest/domain/key checks,
all-copy reconciliation, and copy-bound derived reconciliation. The predecessor's
copies are independently compared with their recorded output Delta versions before
it can be authorized as a rollback destination. Versions/raw evidence must still exist.

Final state transitions:

- Candidate stage BUILDING -> VALIDATED or FAILED; retries revalidate immutable copies.
- Release CANDIDATE -> READY after required copies/stages validate.
- Pointer switches with expected predecessor/generation/token.
- Consumer verification succeeds -> PUBLISHED with published_at/verified_at.
- Ordinary activation verification exception -> conditional rollback only to the checked
  predecessor; failed candidate marked unhealthy/failed.
- A timestamp alone never makes a release published.

This is **not multi-table atomicity**. Active views can expose READY data briefly before
postactivation verification. History views expose only HEALTHY/PUBLISHED releases with
three distinct recognized validated stages; failed/incomplete/unhealthy/withdrawn rows
are excluded. publication_status distinguishes ACTIVE_PENDING_VERIFICATION,
ACTIVE_VALIDATED_RELEASE, HISTORICALLY_PUBLISHED, FAILED, STAGED_INCOMPLETE,
UNHEALTHY_ACTIVE/UNHEALTHY, and WITHDRAWN.

### Crash recovery and health

| Failure point | Expected state / safe action |
| --- | --- |
| During product INSERT | Single Delta statement commits completely or not; no DELETE/INSERT gap |
| After persistence, before validated receipt | Guard may remain after hard kill; retry reconciles existing identical rows |
| After three validated stages, before pointer | Old active release remains; resume only if predecessor/generation still match |
| READY before pointer | Not published history; resume after evidence/fence checks |
| After pointer, before verification/publication metadata | READY active, not published history; inspect guard, revalidate same release, then acknowledge PUBLISHED |
| Ordinary verification failure | Conditional rollback to independently checked predecessor; candidate failed/unhealthy |
| Failure after activation acknowledgement begins | Task fails; health/status reflects unsuccessful verification/acknowledgement; inspect rather than assume success |
| Active replay detects mismatching copies | Explicit UNHEALTHY result/receipt, no data rewrite or silent replacement |

A hard process termination cannot run finally/rollback. To recover a guard: stop and
**confirm termination of the owning process**, inspect writer_stage/token/acquired_at,
then have an administrator conditionally clear precisely that token. Never use elapsed
age alone. For a resume of the same candidate keep the generation; if abandoning/fencing
candidates, advance generation as an explicit operator action. No stale writer may
remain able to submit SQL. Clearing a live guard invalidates the safety contract.

The administrative helper is `recover_writer(ctx, expected_token=...,
confirmed_terminated=True, termination_evidence='incident/query-history evidence reference')`.
It validates complete controls, matches the precise owner and generation, conditionally
clears that owner, and verifies the result. It preserves generation for same-candidate
resume. Its confirmation is an operator attestation, **not** automated proof of death.
An unverified call fails before SQL; stale require_writer calls fail after recovery.
The returned evidence reference should be archived in the incident record.

Manual hard-kill acceptance, not performed by the session-close test: use a newly initialized
`gold_verify_<12 hex>` schema, acquire a guard in process A, record its PID/token and every
SQL operation ID, and verify process B cannot acquire or release A's guard. Terminate A
with SIGKILL in that disposable environment. Verify A is gone and warehouse query history
shows every submitted operation terminal/cancelled; a disconnected client alone is not
proof. Confirm recovery without attestation fails, then perform the explicit helper with
the evidence reference. B must acquire/release normally, and a context carrying A's old
token must fail require_writer. Repeat around product INSERT, receipt, READY, pointer and
publication acknowledgement, verifying candidate rows/receipts and previous published
copies each time. If submitted SQL termination cannot be established, leave the guard held
and escalation manual. No timeout-based takeover or cross-table fencing guarantee follows.

Unhealthy releases are filtered out of v2 business/history read interfaces; the pointer
and immutable copies are retained for diagnosis. Health is recorded validation, **not
continuous monitoring**, and an error does not prove every value is corrupt. There is
no automatic destructive repair or rollback to an unchecked previous release.

Safe recovery/withdrawal procedure (not executed): stop all workers/consumers; inspect
controls and the original error; compare suspect copies with recorded output versions
and committed evidence. Mark the suspect release WITHDRAWN/UNHEALTHY explicitly under
administrator control. Restore no bytes under its identity. Build a new release, or
independently revalidate a retained published release before a reviewed pointer change
with generation increment. Preserve incident evidence and resume readers only after
checks. A release restored from source as different data needs a new identity.

### Public release-read interface and dashboard contract

Convenience active views follow the current pointer. Stable multi-query reads use
published_releases plus **explicit release filtering on the _history family**:

```sql
-- Controller obtains one HEALTHY/PUBLISHED ID, then passes it unchanged to all tiles.
SELECT r.release_id
FROM weather_api_test.gold.published_releases r
JOIN weather_api_test.gold.publication_status p USING (release_id)
WHERE p.is_active_release;

SELECT location_id, forecast_for, temperature_2m, snapshot_at, _release_id
FROM weather_api_test.gold.weather_hourly_latest_history
WHERE _release_id = :gold_release_id AND location_id = :location_id;

SELECT coalesce(w.location_id,a.location_id) location_id,
       coalesce(w.forecast_for,a.forecast_for) forecast_for, w.temperature_2m, a.us_aqi
FROM weather_api_test.gold.weather_hourly_latest_history w
FULL OUTER JOIN weather_api_test.gold.air_quality_hourly_latest_history a
  ON w._release_id=a._release_id AND w.location_id=a.location_id
 AND w.forecast_for=a.forecast_for
WHERE coalesce(w._release_id,a._release_id)=:gold_release_id;
```

Do not filter each side before a join inconsistently or omit release identity. All
built-in historical derived joins are release-qualified. A previously selected HEALTHY/
PUBLISHED release remains readable after a newer activation while its copies are retained.
A withdrawn/unhealthy/deleted release intentionally stops resolving; consumers must show
an unavailable state and deliberately select another verified ID, not silently substitute.

A dashboard/controller should select an ID once per refresh and bind the same parameter
for every related tile. Change it only as a coordinated refresh, and check availability.
Ordinary independent tiles do not coordinate automatically. No dashboard/controller or
platform-specific parameter wiring was built/tested here. Historical retention is not
an eternal snapshot guarantee; no multi-request transaction is claimed.

Correct nearby ranking **after** selected time/type filtering:

```sql
SELECT *,row_number() OVER (
  PARTITION BY _release_id,location_id ORDER BY distance_km,event_id
) selected_window_distance_rank
FROM weather_api_test.gold.location_seismic_nearby_history
WHERE _release_id=:gold_release_id AND location_id=:location_id
  AND event_type='earthquake' AND event_time>=:start_utc AND event_time<:end_utc;
```

Stored distance_rank is across all retained types/times inside radius; filtering does
not rerank it. Empty results mean no matches in retained coverage/radius, not safety.

### Preserved measurement/geographic semantics

snapshot_at is retrieval, not model issue time. observed_at is model valid time, not
verified station observation. Full UTC timestamps are join keys. Local display uses
IANA timezone, TIMESTAMP_NTZ and UTC offset; fall-DST repeats remain distinct through UTC,
and spring gaps are not filled. Zone identifier validity does not prove seeded geographic
assignment. Daily source calendar is UTC, not local-day totals.

All optional missing measurements stay null; zero is real. Null source AQI maps to Unknown.
The null-safe validator uses <=> and rejects wrong/null labels and negative/nonfinite
values. US upper-inclusive thresholds50/100/150/200/300 map Good/Moderate/Unhealthy for
sensitive groups/Unhealthy/Very unhealthy, then Hazardous. EU20/40/60/80/100 map
Good/Fair/Moderate/Poor/Very poor, then Extremely poor. No thresholds changed. Categories
are source-index labels; maximum_hourly_source_us_aqi is not official daily/regional AQI.

Units: temperatures°C, wind km/h, pressure hPa, rain/precipitation mm, **snowfall cm**,
**snow depth m**, hourly radiation W/m², daily radiation MJ/m². Derived known-interval
energy is power×3600/1e6 only where consecutive selected hourly timestamps establish
that interval; first/gaps stay null. It is not a complete daily total. Sum only compatible
nonoverlapping selected precipitation intervals; never add precipitation to components.
Do not average wind bearings arithmetically. Means use DECIMAL(30,12) inputs then DOUBLE;
this quantizes mean inputs but not detail/extrema.

References: [Weather definitions](https://open-meteo.com/en/docs),
[AQ definitions](https://open-meteo.com/en/docs/air-quality-api),
[USGS GeoJSON](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php).

Weather/AQ use requested registry coordinates, not returned grid coordinates. Keep API
and registry elevations distinct. Only two represented sampled locations were audited.
Maps use markers; no interpolation or statewide claims. Seeded state/country/population
are unverified; no exposure weighting or invented jurisdiction assignment. The sampled
summary covers represented locations only and exposes known-value denominators.

Seismic points/grid retain global and non-earthquake events. Grid wraps +180 to -180 and
clamps the north-pole terminal cell; degree cells differ in physical area. Counts are
not equal-area density, hazard or complete historical rates. CONUS/Alaska/Hawaii/territory
map windows are not authoritative boundaries. No boundary download/dependency introduced.
Magnitude method stays in summary grain; numeric bands are not interchangeable scales.
Negative magnitudes/depths are preserved; tsunami_flag is not a public warning.

Snapshot audit now canonicalizes only USGS identifier/network/product-list arrays as
unordered sets **for hashes**; output arrays remain faithful. Other field ordering is
preserved. Retrieval/ingestion/feed/version-only differences are excluded from substantive
hashes. Source-version, broad-attribute and measurement/geometry changes remain distinct.
First snapshots have null comparisons. No medical/seismic safety advice is produced.

### Growth, performance, and retention

max_runs is a fail-closed bound on **all ledger-referenced runs**, lexically ordered by ID,
not latest-N. Overflow errors include discovered count and configured bound. Ledger rows
also have an explicit bound. Increase max_runs deliberately up to1000 after checking cost;
never truncate old eligible weather hours or event history. Completely unreferenced
manifests require separate upstream monitoring.

Results report discovered/eligible/excluded runs, ledger/files/unit counts, per-query
operation/timestamp/status/elapsed seconds and per-stage query/elapsed totals. SQL text
and credentials are not logged. Inspect a slow operation before retrying; do not launch
concurrent duplicate batches. Local test driver has a300-second batch alarm and60-second
transport timeout; Spark/runtime statement timeouts require platform configuration.

Within one invocation source evidence is reused and state schema/ownership checks are
cached after verification. No stale cross-run cache skips evidence or predecessor checks.
The guard allows lightweight ownership checks between writes; complete controls are
rechecked before activation. Full retained-history refresh and independent stage evidence
revalidation remain intentional. Performance at large historical scale is unverified.

Operational visibility: _gold_releases.report and publication_status retain candidate
states; _gold_stages counts/output_versions record products; read-only GROUP BY _release_id
on backing tables measures retained rows per candidate. Exact ledger/run limits expose
scope size. These are not a full manifest inventory or continuous integrity service.

Retention requirements, no automatic cleanup:

- Active/published copies and receipts must outlive every consumer's selected-release SLA.
- Keep failed/incomplete copies/receipts through incident diagnosis; never delete a running
  candidate or the checked rollback target.
- Keep source Delta pinned versions (including output_versions) beyond replay/recovery SLA;
  coordinate VACUUM/time travel explicitly.
- Keep raw files/manifests for the same evidence/replay SLA. Digest checks need original bytes.
- Before reviewed deletion, exclude active/running/pinned-consumer/rollback references,
  verify an alternative recovery point, archive required evidence, then withdraw historical
  read eligibility explicitly. Do not leave published metadata advertising missing copies.

### Migration plan: prepared, never applied automatically

The Weather definition `migration_plan(session, project_root=..., config_path=...)`
performs **reads only** and returns exact SQL strings, captured baseline Delta versions,
new view names, and saved legacy view DDL. It verifies the existing33-object v1 inventory,
common ownership, active validated receipts, all ten active public/backing contents/keys,
and unchanged baseline versions. It rejects unexpected inventories. A matching durable migration journal resumes a partial upgrade; a pristine V1 inventory is not required again. It does not
recompute all raw-to-Silver business values. Run a fresh plan against final reviewed code;
never apply an earlier plan after changing the notebook/validation contract.

Load definitions without entry cells, using the real bootstrap (no test runtime dependency):

```python
import json, sys, types
from pathlib import Path
root=Path('/absolute/path/to/checkout')
nb=json.loads((root/'notebooks/gold/weather_gold.ipynb').read_text())
module=types.ModuleType('gold_review');sys.modules[module.__name__]=module
scope=vars(module)
boot=next(c for c in nb['cells'] if c.get('metadata',{}).get('tags')==['gold_bootstrap'])
exec(''.join(boot['source']),scope)
scope['load_gold_definitions'](root,['weather','air_quality','earthquakes'],scope)
plan=scope['migration_plan'](existing_sql_connection,project_root=root)
# Review plan['sql']; this call NEVER executes those proposed statements.
```

The consolidated V1 plan has **59 core DDL/DML statements**, independent of candidate
count: three ADD COLUMNS, three set-based backfills, 13 managed-property updates,
20 active/public view replacements, and 20 historical/publication view definitions.
There are no new SQL CHECK constraints: complete control/key/schema/content invariants
are enforced by blocking validation before planning and at completion. Control columns
are deliberately nullable for ALTER compatibility; outside/manual writers are not fenced.
Compared with the former 68-statement plan, per-release/per-stage UPDATEs are replaced
by one CASE UPDATE per control table. All failed/interrupted receipts and original
IDs, contract hashes, source fingerprints and timestamps remain. Backing measurement
rows are never rewritten. Generation starts at zero at the maintenance cutover.

An old active release becomes **LEGACY_ACTIVE / UNKNOWN**, not HEALTHY/PUBLISHED.
Matching active/public copies does not establish complete V2 raw evidence, prerequisite
validation, or substantive derived reconciliation. Its active data stays readable, with
LEGACY_ACTIVE_UNVERIFIED status, but it is excluded from published_releases and history.
Build a new V2 release to enter the verified historical interface. Earlier candidate
provenance stays FAILED or CANDIDATE. Old contract IDs cannot be replayed as V2 IDs.

Execution prerequisites and concrete procedure (existing deployment remains unapplied):

1. Freeze the final notebook/validation checkout and archive the generated JSON plan,
   original view DDL, control versions/schemas, owners and grants. Confirm source/evidence
   availability and sufficient time-travel retention. Stop **all** Gold writers and readers;
   V1 and V2 writers cannot coexist during this schema/view cutover. Silver need not change.
2. Generate the read-only plan with the bootstrap example above. Review groups, baseline,
   admission policy and migration_id. Regenerate if code, baseline or metadata changed.
3. In a separately authorized maintenance window only, call
   `apply_migration_plan(connection, plan, gold=GoldConfig(),
   approved_existing_target='weather_api_test.gold')`. This task does **not** authorize or
   perform that call. The default executor refuses the existing target without that explicit
   argument. An argument is an execution safeguard, not a substitute for operator approval.
4. The executor creates `_gold_migration` and inserts one journal record (two additional
   setup statements outside the 59), then checkpoints every core statement. Plan/expected
   JSON uses hex-encoded UTF-8 SQL expressions and INSERT ... SELECT so embedded escapes survive SQL parsing (the warehouse rejects these expressions in an inline VALUES table).
   Owner/checkpoint/status UPDATEs are additional operational writes, not business migration
   statements. There is no multi-object transaction or zero-downtime claim.
5. Each step checks all 13 tracked versions and object/schema/view metadata. Its transition
   may change only its intended object; unexpected other-object changes or unexpected version
   increments fail closed. Plan digest and mutation destinations are checked as well.
6. For an ordinary interruption at a completed checkpoint, reload `migration_plan` with the
   same code/target; it returns the stored original plan. Retry `apply_migration_plan`.
   Never reset recorded versions or automatically accept a changed baseline. Restore an
   accidentally changed view only from reviewed fully qualified original DDL; changes that
   advanced Delta versions require explicit review, not rebasing.
7. An empty journal after a CREATE-success/INSERT-failure is not automatically adopted.
   Verify zero journal rows, no submitted operation still running, unchanged original core
   versions/metadata and V1 shapes; an administrator may remove only that empty operational
   journal and regenerate the plan. This is not permission to drop business tables or reset
   Delta baselines. Never remove a journal with a recorded plan/checkpoint.
8. A crash after SQL submission but before checkpoint acknowledgement leaves inflight_index
   and NEEDS_REVIEW (or APPLYING after hard kill). Do not replay blindly. Confirm the admin
   owner and all submitted statements terminated, inspect the exact statement and recorded
   before/actual versions/metadata, and obtain a reviewed resolution. Automatic ambiguous
   acknowledgement recovery is intentionally unsupported. A hard-killed journal owner also
   requires explicit confirmed-dead administrative recovery; timeout is insufficient.
9. On COMPLETE, validate controls and active copies, then inspect all public views/status,
   retained candidates and historical exclusion. Run Weather -> AQ -> Earthquakes with a new
   V2 release. Resume readers after the intended legacy/new-publication state is verified.

Background platform maintenance counts as a baseline change. During verification an
existing seismic backing table advanced from version 4 to 6 through another principal's
SET TBLPROPERTIES / COMPUTE STATS operations. Schema and full multiset rows reconciled
exactly at 4 and 6; the unapplied older plan was archived and a new read-only plan generated.
No existing-deployment migration or restoration was executed. Do not silently reset a
partially applied journal's expected versions for a statistics-only write. Quiesce other
writers and background maintenance before cutover, or expect the version gate to abort.

Rollback is a reviewed maintenance operation, not an automatic inverse. Before any V2
publication, keep readers/writers stopped, restore saved **fully qualified** legacy view
DDL, remove only the newly created historical interfaces, and restore V1 table properties.
Added control columns/backfills are not undone by view restoration; V1 exact-schema writers
must remain stopped until the three controls are separately restored from archived versions
with verified absence of intervening writes. Preserve the journal/incident record. RESTORE
itself advances Delta versions; never pretend the old version numbers still describe current
state. Metadata/grants need separate review. After a V2 publication, do not RESTORE controls
or reverse the pointer blindly: that could orphan published data and erase provenance.
Take a new reviewed recovery decision, preserving all copies and receipts. VACUUM or lost
source/raw evidence can make historical restoration impossible. Later V2 code changes need a
reviewed contract/view migration; this planner is specifically V1-to-V2.

For a genuinely empty isolated deployment, migration_plan(session=None,gold=GoldConfig(schema=... ),...) returns exact
CREATE SCHEMA/three control CREATE TABLE/single singleton INSERT statements, without
executing them. Require all controls absent and a single administrator. Do not use this
empty-state plan over existing objects.

### Controlled persistent verification contract

Persistent verification uses gold_verify_<12 lowercase hex> schemas in weather_api_test. Controls, backing tables, views, historical views and migration journal all use the selected schema. No fixture writes Silver/reference/raw/ledger resources. The existing gold schema is measured read-only before/after verification. A separate opt-in test verifies SIGKILL after all owner SQL completes. Killing with SQL in flight and actual scheduled-job tests remain manual checks.

1. Verify actual Git-folder source JSON/bootstrap from each notebook and explicit root.
2. Run Weather validate_only; confirm no receipt/ready task value. Run guarded write mode;
   verify taskValues handoff in a small manual job (no schedule), then exact AQ/EQ parameters.
3. Verify first publication and same-release replay, histories across a second publication,
   same-input/different-settings ordering, disabled/missing sources and empty event feed.
4. In that disposable deployment only, terminate a process before/after INSERT, receipt,
   READY, pointer and final verification. Inspect controls and retained data; confirm guard
   stays held on hard kill, then perform documented dead-owner recovery/reconciliation.
5. Run two publishers and duplicate stages; require one owner, no active-copy rewrite,
   rejected stale generation/ABA candidate, intact published-history reads.
6. Inject isolated copy/control/view corruption; require explicit unhealthy/fail-closed
   output, no automatic destructive repair, and no unchecked rollback.
7. Verify SQL metadata preflight, actual Spark adapter/widget execution, task-value False/
   exception failures and coordinated dashboard parameter prototypes before rollout.

Offline mocks and TEMP-only SQL tests do not prove persistent Delta/concurrency/crash outcomes. The isolated persistent test is explicitly opt-in; it does not establish hard-process-kill, scheduled-task or remote PySpark behavior.

### Focused tests and execution evidence

```sh
.venv-databricks/bin/python -B -m unittest tests.test_gold tests.test_gold_sql -v

# Opt-in focused warehouse tests; TEMP VIEWs only, not persistent fault injection.
.venv-databricks/bin/python -B -u -m tests.test_gold_sql \
  --connection-settings check_databricks_connection.py \
  --case test_null_safe_category_validation_rejects_corrupt_labels \
  --case test_retained_public_reads_exclude_failed_incomplete_and_unhealthy \
  --case test_historical_environment_join_is_release_qualified \
  --case test_unordered_source_arrays_do_not_create_false_revisions \
  --case test_known_raw_boundary_values_survive_gold \
  --case test_radiation_uses_known_interval_and_preserves_units \
  --case test_nearby_candidate_filter_equals_all_pairs_including_dateline_and_poles
```

Explicit persistent verification (only creates random allowlisted disposable schemas):

```sh
.venv-databricks/bin/python -B -u -m tests.test_gold_sql \
  --connection-settings check_databricks_connection.py --isolated-persistent \
  --timeout-seconds 4500 --case test_isolated_delta_migration_writer_and_history

# Substitute the successful retained gold_verify_<12 hex> schema from its progress log.
.venv-databricks/bin/python -B -u -m tests.test_gold_sql \
  --connection-settings check_databricks_connection.py \
  --retained-schema gold_verify_<12_hex> --isolated-persistent --timeout-seconds 900 \
  --case test_retained_isolated_independent_expectations \
  --case test_retained_isolated_retry_handoff_and_pointer_failure

# Prefer a separate freshly initialized disposable schema for this independent guard test.
.venv-databricks/bin/python -B -u -m tests.test_gold_sql \
  --connection-settings check_databricks_connection.py --isolated-persistent \
  --retained-schema gold_verify_<12_hex> --timeout-seconds 360 \
  --case test_retained_isolated_hard_kill_recovery
```

The persistent fixtures are synthetic Silver-shaped **session inputs**, with actual notebook
transformation SQL and real isolated Delta/control/view publication helpers. They do not
refetch APIs, write Silver, or prove real manifest-cohort discovery, Spark notebook widgets,
or job taskValues wiring. The full integration test captures schemas and canonical complete
row content for all 19 history products, excluding dynamic age/status/publication fields
from the frozen comparison. Independent checks cover explicit changed measurements/settings,
release-qualified joins, publication timestamp identity and bounded query-time ages. The
follow-up retry checks that existing Weather backing versions stay unchanged on duplicate
active product persistence; a mocked taskValues failure occurs while a real guard is held
and cleanup permits the next invocation. A separate injected pointer-verification failure
checks real conditional rollback/generation advance and FAILED/UNHEALTHY exclusion; it is a
control-plane failure test, not an entire failed run_* adapter. The separate hard-kill test
spawns its own Python owner, waits for a terminal-SQL handshake, kills only that child with
SIGKILL, reaps it, verifies the retained guard, rejects an unattested takeover, and recovers
using confirmed termination. It verifies stale-token rejection and a subsequent writer.
It does not kill in-flight SQL or prove cross-table fencing.

At the hardening checkpoint, seven focused SQL cases passed,97 statements,57.758 seconds
(2026-10-25 04:43:26–04:44:24 UTC). They used only session temporary views/reads. The existing
14-case full warehouse suite was not rerun. Offline tests cover complete controls, stale/
ABA fences, writer acquisition, handoff, root/tag failures, adapters, truthful failure states,
immutable write structure, legacy provisioning plan, and original eligibility tests.

Earlier, superseded read-only migration-plan generation completed at04:49:28 UTC:116 queries,74.941 seconds,
68 proposed statements,20 saved legacy view definitions,20 proposed new interfaces. All13
Gold Delta versions were unchanged; active ID remained35803cd98b5117449ecc9c2a660e73a5.
This was a planning checkpoint, not an applied migration; regenerate after final source
review because source changes alter the deployment contract hash.

Earlier v1 record: the existing active release was published through SQL/Delta with three
validated stages and five committed files/246 verified unit fields. The prior18 offline/
14 SQL pass record and final-stage no-op are historical v1 evidence, not proof of v2 runtime.
Those earlier records did not execute V2 publication; see the isolated-verification record below. Remote Spark, UI/widgets, scheduled handoff, crashes with SQL in flight and dashboards remain separate checks.

### Dashboard handoff remains data-only

Overview: location_current, sampled hourly summary, seismic daily/grid, freshness_coverage.
Weather: source current/hourly/daily, full valid-time selectors, advanced soil/radiation/
visibility and units. AQ: separate US/EU categories, pollutants/components and availability.
Earthquakes: points/grid/time/type/method/review filters, detail links, nearby and revision
audit. Location detail: independent current cards, environment timelines, UTC daily weather,
nearby events/timezone/missingness. Freshness: retrieval versus Gold build/publication,
represented locations, known-value denominators, explicit publication/health state.
For coordinated history, use the matching _history interfaces and one release parameter.
No dashboard is built; climatology, independent forecast accuracy, complete seismic rates,
verified population exposure and causal/health/safety conclusions remain unsupported.

Final hardening verification (2026-10-25 UTC):34 offline tests passed (0.350s at05:07:42);
the opt-in SQL class was skipped in offline discovery. Nine distinct warehouse cases
passed across the focused7-case batch and two targeted follow-ups. The revision follow-up
passed at04:57:49 (19 statements,6.961s); the new diagnostic-status case initially failed
because duplicate release receipts produced duplicate diagnostic rows, then passed after
its row-number filter was corrected at05:06:00 (13 statements,4.435s). These are distinct
cases, not ten passes; the failed attempt is retained here as execution evidence.
The final read-only metadata preflight passed against the deployed v1 view definition/
owner; at04:58:02 all13 Gold Delta versions still matched the earlier baseline and the
active release ID was unchanged. That earlier hardening checkpoint did not execute persistent V2 operations; later isolated evidence is reported separately.

Provision controls/views under one reviewed ownership principal; initial view creation
must use that principal (or ownership must be explicitly reconciled during maintenance).
Schema/SQL/owner preflight includes historical and predecessor views. INVALID_CONTROL_STATE
is a diagnostic status; duplicate pointer/release controls do not multiply diagnostic
rows, and complete control validation still blocks writes. Stale/malformed invocation
errors do not mark an otherwise published historical release unhealthy. A genuine
predecessor-copy mismatch also records that predecessor as suspect/unhealthy rather than
leaving it advertised as a safe rollback destination. If source pins advance during
hard-crash recovery, normal same-release acknowledgement fails closed; operator review
or a new fenced build is required, not an automatic bypass of pin checks.

### V1-to-V2 verification record (2026-10-25, isolated destinations)

The existing `weather_api_test.gold` deployment was **not migrated**. Its original active
ID remains `35803cd98b5117449ecc9c2a660e73a5`, with the same 33-object inventory and
schema/view/owner/comment fingerprints. Twelve original Delta versions stayed fixed.
The seismic latest backing table advanced externally from 4 to 6 through SET TBLPROPERTIES
and COMPUTE STATS under a principal different from the querying account; full schema and
multiset rows reconciled exactly between 4 and 6. The stale, unapplied plan was archived;
the final read-only 59-statement plan was regenerated against version 6. No recorded
migration checkpoint was rebased, and no existing-deployment write was submitted.

Successful retained destinations, all under `weather_api_test`:

- `gold_verify_a43d184fc2dc`: published A (`aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`) and B
  (`bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb`), plus an excluded FAILED/UNHEALTHY C fault candidate.
- `gold_verify_b84c6c709a2f`: migrated V1 fixture, retained legacy A and failed/interrupted
  F/E provenance, then published new V2 D (`dddddddddddddddddddddddddddddddd`).
- `gold_verify_f9d43ea25e75`: final-code fresh initialization and real SIGKILL guard test;
  guard empty afterward, no business publication in this independent destination.

The V1 fixture used pinned versions of a quiescent **isolated** A release, rechecked after
copying; it did not clone the existing deployment. The fixture migration paused after two
core statements, rejected a changed view baseline, accepted its exact DDL restoration,
and resumed the original persisted plan through checkpoint 59/COMPLETE. This migration/
fault-check portion used 2,049 SQL calls over 1,351.465 seconds. Original copy data stayed
readable, published history stayed empty for unverified legacy A, and original candidate
fields/stage records survived. The new V2 D publication and a separate read-only 15-query,
13.155-second provenance/copy reconciliation passed afterward.

A-to-B checks passed for all **19 history products**, comparing schemas and complete
canonical stable row content, then verifying active B, published-state exclusions and
release-specific settings (radius 250/75 km, grid 2/1 degrees, alignment 900/60 seconds,
staleness 10800/60 seconds). Independent measurements/counts/join expectations, query-time
ages and publication timestamp identity passed. The source/migration/history scenario
finished across explicitly resumed verification runs; its final continuation reached the
history checkpoint at 2,530.350 seconds. Two final SQL cases passed in 119.297 seconds:
active Weather retry preserved backing versions, taskValues failure propagated with guard
cleanup, and a real injected pointer-verification error rolled back to B, advanced its
generation twice, and excluded the failed C candidate.

Two genuinely concurrent SQL sessions produced one guard winner and one
DELTA_CONCURRENT_APPEND.ROW_LEVEL_CHANGES rejection. Unattested recovery and stale ownership
were rejected; losing acquisition could not clear the winner. The separate real child-process
SIGKILL test passed in 27.593 seconds, after a synchronous terminal-SQL handshake, proving
retained ownership, confirmed-dead recovery and a subsequent owner. This covers **no SQL in
flight at kill**. In-flight commit/acknowledgement crashes, full notebook/job task-value
wiring and remote PySpark remain unverified; follow the manual procedures above.

The final local Gold-only check passed 39 offline tests; live SQL discovery is explicitly
skipped without a session. Four additional opt-in warehouse cases passed (independent
expectations, retry/handoff/pointer failure, retained legacy provenance/copies, SIGKILL).
These use synthetic Silver-shaped inputs and actual notebook SQL/Delta helpers, not live
APIs or real committed manifest discovery. Existing Silver/reference/raw/manifests/ledger,
source configuration fingerprints, job definitions, schedules and notifications were not
modified. No dashboard or commit push occurred.

Initial verification failures were retained rather than concealed: unqualified SHOW CREATE
rollback DDL, SQL backslash decoding in journal JSON, unsupported VALUES expressions, and
unsupported fixture SELECT * REPLACE syntax were corrected. A temporary continuation driver
also needed its missing ValidationError import. The only later notebook-contract change was
the journal INSERT VALUES-to-SELECT fix; its isolated contract upgrade was explicitly checked
as that one source-line change, preserved A's original receipt hash, and changed no business
rows. Final-code fresh initialization was independently retested in the SIGKILL destination.

Earlier diagnostic schemas `gold_verify_51e93a561bf3` and `gold_verify_c85a5c48adc8` are also
retained, including a corrupt legacy test journal from the first failed serialization attempt.
They are disposable evidence, not supported migration/replay targets. Review/archive evidence
before manually removing only these five owned verification schemas; no cleanup is scheduled.
The next existing-deployment action remains review and a separately authorized maintenance
window, including quiescence of background statistics/optimization writers.
