"""Nonsecret pipeline destinations. Explicit JSON configurations must be complete."""
from dataclasses import dataclass, asdict, fields, replace
from pathlib import Path
import hashlib
import json
import os
import re

SOURCES = ('weather', 'air_quality', 'earthquakes')
SILVER_TABLES = {'weather': {'current': 'weather_current', 'hourly': 'weather_hourly', 'daily': 'weather_daily'}, 'air_quality': {'current': 'air_quality_current', 'hourly': 'air_quality_hourly'}, 'earthquakes': {'latest': 'earthquakes', 'history': 'earthquakes_event_history'}}

@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = 'weather_api'
    reference_schema: str = 'reference'
    raw_schema: str = 'raw'
    bronze_schema: str = 'bronze'
    silver_schema: str = 'silver'
    state_schema: str = 'pipeline_state'
    registry_name: str = 'location_registry'
    registry_staging_name: str = 'location_registry_staging'
    registry_backup_prefix: str = 'location_registry_backup'
    registry_rejected_prefix: str = 'location_registry_rejected'
    ledger_name: str = 'silver_completed_files'
    registry_source: str = '/Volumes/weather_api/reference/location_files/US.txt'
    raw_root: str = '/Volumes/weather_api/raw/raw_files'
    staging_root: str = '/Volumes/weather_api/raw/raw_files/_staging'
    manifest_root: str = '/Volumes/weather_api/raw/raw_files/_runs'
    attempts_root: str = '/Volumes/weather_api/raw/raw_files/_attempts'
    schema_root: str = '/Volumes/weather_api/pipeline_state/autoloader_state/_schemas'
    checkpoint_root: str = '/Volumes/weather_api/pipeline_state/autoloader_state/_checkpoints'
    max_manifest_files: int = 1000

    def validate(self):
        from src.validation.common import require
        for f in fields(self):
            value = getattr(self, f.name)
            if f.name.endswith(('_root', '_source')):
                require(isinstance(value, str) and Path(value).is_absolute() and '..' not in Path(value).parts, 'Configuration paths must be absolute and normalized: '+f.name, stage='configuration')
            elif f.name != 'max_manifest_files':
                require(isinstance(value, str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value) is not None, 'Invalid SQL identifier: '+f.name, stage='configuration')
        require(type(self.max_manifest_files) is int and 0 < self.max_manifest_files <= 10000, 'max_manifest_files must be 1..10000', stage='configuration')
        require(len({self.reference_schema,self.raw_schema,self.bronze_schema,self.silver_schema,self.state_schema})==5, 'Schemas must be distinct', stage='configuration')
        roots = [self.staging_root,self.manifest_root,self.attempts_root,self.schema_root,self.checkpoint_root]
        require(len(set(roots)) == len(roots), 'Control/state roots must be distinct', stage='configuration')
        for source in SOURCES:
            source_root = Path(self.raw_root)/source
            require(all(not Path(r).is_relative_to(source_root) and not source_root.is_relative_to(Path(r)) for r in roots), 'Control/state roots must not overlap watched source directories', stage='configuration')
        for name in ('registry_source','raw_root','staging_root','manifest_root','attempts_root','schema_root','checkpoint_root'):
            path=Path(getattr(self,name))
            if str(path).startswith('/Volumes/'):
                expected_schema=self.reference_schema if name=='registry_source' else (self.state_schema if name in ('schema_root','checkpoint_root') else self.raw_schema)
                require(len(path.parts)>4 and path.parts[2:4]==(self.catalog,expected_schema),'Volume path must match configured catalog/schema: '+name,stage='configuration')
        require(len({self.registry_name,self.registry_staging_name,self.registry_backup_prefix,self.registry_rejected_prefix})==4,'Registry destinations must be distinct',stage='configuration')
        return self

    def table(self, schema, name): return f'{self.catalog}.{schema}.{name}'
    @property
    def registry(self): return self.table(self.reference_schema,self.registry_name)
    @property
    def registry_staging(self): return self.table(self.reference_schema,self.registry_staging_name)
    @property
    def ledger(self): return self.table(self.state_schema,self.ledger_name)
    def bronze(self, source): return self.table(self.bronze_schema,source+'_raw')
    def silver(self, source, kind): return self.table(self.silver_schema,SILVER_TABLES[source][kind])
    def checked_run(self, run_id):
        from src.validation.common import require
        require(isinstance(run_id,str) and re.fullmatch(r'[A-Za-z0-9_-]{1,100}',run_id) is not None,'Invalid run ID',stage='configuration')
        return run_id
    def run_dir(self, source, run_id):
        from src.validation.common import require
        require(source in SOURCES,'Unsupported source',stage='configuration')
        return Path(self.raw_root)/source/self.checked_run(run_id)
    def manifest_path(self, run_id): return Path(self.manifest_root)/(self.checked_run(run_id)+'.json')
    def attempt_path(self, run_id): return Path(self.attempts_root)/(self.checked_run(run_id)+'.json')
    @property
    def fingerprint(self): return hashlib.sha256(json.dumps(asdict(self),sort_keys=True).encode()).hexdigest()

def load_config(path=None):
    path = path or os.getenv('WEATHER_PIPELINE_CONFIG')
    if path:
        from src.validation.common import require
        with Path(path).open() as f: data=json.load(f)
        required={f.name for f in fields(PipelineConfig)}
        require(isinstance(data,dict) and set(data)==required, 'Explicit configuration must supply exactly all PipelineConfig fields; no production fallback', stage='configuration')
        return PipelineConfig(**data).validate()
    cfg=PipelineConfig()
    # Legacy override only changes raw storage; use a complete config for isolation.
    raw=os.getenv('RAW_VOLUME_ROOT')
    if raw:
        cfg=replace(cfg,raw_root=raw,staging_root=str(Path(raw)/'_staging'),manifest_root=str(Path(raw)/'_runs'),attempts_root=str(Path(raw)/'_attempts'))
    return cfg.validate()
