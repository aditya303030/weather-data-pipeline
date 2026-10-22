"""Offline-importable orchestration; HTTP is only performed by explicit run calls."""
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from urllib.parse import urlsplit, urlunsplit
from importlib import import_module
from src.validation.common import require, ValidationError
from src.validation.raw import validate_raw_files, validate_wrapper, read_json_file

DEFAULT_ROOT='/Volumes/weather_api/raw/raw_files'
SOURCES=('weather','air_quality','earthquakes')

def project_root(start=None):
    configured=os.getenv('WEATHER_PROJECT_ROOT')
    if configured: return Path(configured).resolve()
    current=Path(start or Path.cwd()).resolve()
    for p in (current,*current.parents):
        if (p/'src'/'ingestion'/'location_discovery.py').exists(): return p
    # Module location also works when invoked from elsewhere.
    return Path(__file__).resolve().parents[2]

def safe_endpoint(url):
    """Never persist userinfo, query strings, or fragments (may hold credentials)."""
    parts=urlsplit(url)
    host=parts.hostname or ''
    if parts.port: host+=':'+str(parts.port)
    return urlunsplit((parts.scheme,host,parts.path,'',''))

def parameter(dbutils,name,default):
    try: return dbutils.widgets.get(name)
    except Exception: return str(default)

def select_locations(locations,limit,rotation_slot):
    """Stable ID order; slot N selects block N modulo the number of blocks.

    Supply a monotonically increasing coverage_slot externally; default UTC day
    rotates daily. Retrying the same slot selects the same locations.
    """
    require(type(limit) is int and limit>0,'max_locations must be positive')
    require(type(rotation_slot) is int and rotation_slot>=0,'coverage_slot must be nonnegative')
    ordered=sorted(locations,key=lambda x:x['location_id'])
    require(len({x['location_id'] for x in ordered})==len(ordered),'Duplicate registry IDs')
    if not ordered: return []
    blocks=(len(ordered)+limit-1)//limit
    start=(rotation_slot%blocks)*limit
    return ordered[start:start+limit]

def save_json(payload,path,staging_root):
    """Stage outside watched directories, then publish with a unique name.

    UC Volume rename is NOT claimed to be atomic. Bronze must start only after
    ingestion and manifest validation succeed; never run the two concurrently.
    Temporary .tmp files cannot match Bronze's *.json path filter.
    """
    path=Path(path); staging_root=Path(staging_root)
    staging_root.mkdir(parents=True,exist_ok=True); path.parent.mkdir(parents=True,exist_ok=True)
    temp=staging_root/(uuid4().hex+'.tmp')
    try:
        with temp.open('x',encoding='utf-8') as f: json.dump(payload,f,ensure_ascii=False,allow_nan=False)
        read_json_file(temp)
        require(not path.exists(),'Refusing to overwrite existing raw snapshot')
        os.rename(temp,path)
    finally:
        if temp.exists(): temp.unlink()

def run_ingestion(spark,dbutils):
    from dotenv import load_dotenv
    load_dotenv(project_root()/'.env')
    source=parameter(dbutils,'source','all').strip().lower()
    require(source in (*SOURCES,'all','init_registry','enable_locations'),'Unknown ingestion source')
    discovery=import_module('src.ingestion.location_discovery')
    if source=='init_registry': return discovery.initialize_registry_geonames(spark)
    if source=='enable_locations':
        return discovery.enable_locations_for_ingestion(spark,
            limit=int(parameter(dbutils,'enable_limit',20)),
            weather=parameter(dbutils,'enable_weather','true').lower()=='true',
            air_quality=parameter(dbutils,'enable_air_quality','true').lower()=='true',
            distributed=parameter(dbutils,'distributed','false').lower()=='true')
    limit=int(parameter(dbutils,'max_locations',os.getenv('MAX_LOCATIONS','20')))
    slot=int(parameter(dbutils,'coverage_slot',datetime.now(timezone.utc).date().toordinal()))
    retries=int(os.getenv('MAX_RETRIES','3')); delay=float(os.getenv('BATCH_DELAY_SECONDS','0.5'))
    require(retries>0 and delay>=0,'Invalid retry/rate-limit configuration')
    run_id=uuid4().hex
    volume=Path(os.getenv('RAW_VOLUME_ROOT',DEFAULT_ROOT))
    manifest={'run_id':run_id,'coverage_slot':slot,'sources':list(SOURCES) if source=='all' else [source],'expected_files':0,'files':[],'errors':[]}
    for src in manifest['sources']:
        module=import_module('src.ingestion.'+('usgs_api' if src=='earthquakes' else src+'_api'))
        endpoint=os.getenv({'weather':'WEATHER_API_URL','air_quality':'AIR_QUALITY_API_URL','earthquakes':'USGS_API_URL'}[src],module.DEFAULT_API_URL)
        locations=[None] if src=='earthquakes' else select_locations(discovery.load_active_locations(spark,src),limit,slot)
        require(bool(locations),f'No enabled locations for {src}')
        manifest['expected_files']+=len(locations)
        for loc in locations:
            for attempt in range(retries):
                try:
                    fetch=getattr(module,{'weather':'fetch_weather','air_quality':'fetch_air_quality','earthquakes':'fetch_earthquakes'}[src])
                    args=() if loc is None else (loc['latitude'],loc['longitude'])
                    data,_=fetch(*args,api_url=endpoint)
                    meta={'source':src,'run_id':run_id,'retrieval_timestamp_utc':datetime.now(timezone.utc).isoformat(),'api_endpoint':safe_endpoint(endpoint)}
                    if loc is not None: meta.update(location_id=loc['location_id'],location_name=loc['name'],latitude=loc['latitude'],longitude=loc['longitude'])
                    wrapper={'metadata':meta,'raw_response':data}
                    validate_wrapper(wrapper,src,run_id)
                    identity='global' if loc is None else str(loc['location_id'])
                    path=volume/src/f'{src}_{identity}_{run_id}_{uuid4().hex}.json'
                    save_json(wrapper,path,volume/'_staging')
                    manifest['files'].append({'source':src,'path':str(path)})
                    break
                except ValidationError:
                    # Invalid payloads are blocking and must never be swallowed/retried.
                    raise
                except Exception:
                    if attempt+1==retries:
                        # Transport exception strings can contain credential-bearing URLs.
                        manifest['errors'].append({'source':src,'location_id':None if loc is None else loc['location_id'],'error':'Fetch or storage failed; endpoint details redacted'})
                    else: time.sleep(1.5**(attempt+1))
            time.sleep(delay)
    validate_raw_files(manifest)
    manifest_path=volume/'_runs'/f'{run_id}.json'
    save_json(manifest,manifest_path,volume/'_staging')
    print(json.dumps({'run_id':run_id,'manifest_path':str(manifest_path),'saved':len(manifest['files'])}))
    return manifest
