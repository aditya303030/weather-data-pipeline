"""Request plans, resumable extraction and manifest publication. No live work on import."""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from importlib import import_module
from src.config import SOURCES, load_config
from src.validation.common import require, ValidationError, ValidationResult, contextualize
from src.validation.raw import validate_raw_files, validate_manifest, validate_wrapper, read_json_file, file_digest, load_committed
from .databricks import publish_task_values


def project_root(start=None):
    configured=os.getenv('WEATHER_PROJECT_ROOT')
    if configured:return Path(configured).resolve()
    current=Path(start or Path.cwd()).resolve()
    return next((p for p in (current,*current.parents) if (p/'src'/'ingestion'/'location_discovery.py').is_file()),Path(__file__).resolve().parents[2])

def safe_endpoint(url):
    """Persist no URL components: even hosts and paths can carry credentials."""
    return 'external-api' # metadata.source is the useful public source identity

def parameter(dbutils,name,default):
    try:
        value=dbutils.widgets.get(name)
        return str(default) if value is None or value=='' else value
    except Exception as exc:
        # Widgets throw for missing names; only this read has a default. No writes here.
        if 'does not exist' in str(exc).lower() or 'not defined' in str(exc).lower() or isinstance(exc,(KeyError,AttributeError)) or 'missing' in str(exc).lower() or 'inputwidgetnotdefined' in str(exc).lower() or 'no input widget' in str(exc).lower():return str(default)
        raise

def select_locations(locations,limit,rotation_slot):
    from src.validation.location_registry import validate_selection_input
    validate_selection_input(locations,limit,rotation_slot)
    ordered=sorted(locations,key=lambda x:x['location_id'])
    if not ordered:return []
    blocks=(len(ordered)+limit-1)//limit
    start=rotation_slot%blocks*limit
    return ordered[start:start+limit]

def save_json(payload,path,staging_root):
    """One file publication only; no claim of atomic Volume rename or multi-file commit.

    An incomplete destination cannot become eligible: commit rereads/validates and
    fingerprints every file. A recovery operator must repair an invalid destination
    explicitly; valid destinations are immutable and reused.
    """
    path=Path(path);staging_root=Path(staging_root)
    staging_root.mkdir(parents=True,exist_ok=True);path.parent.mkdir(parents=True,exist_ok=True)
    temp=staging_root/(uuid4().hex+'.tmp')
    try:
        with temp.open('x',encoding='utf-8') as f:json.dump(payload,f,ensure_ascii=False,allow_nan=False)
        read_json_file(temp)
        require(not path.exists(),'Destination exists; validate/reuse or explicitly repair interrupted publication',stage='publication')
        os.rename(temp,path)
    finally:
        if temp.exists():temp.unlink()

def make_plan(config,source,locations_by_source,coverage_slot,run_id=None):
    config.validate();run_id=run_id or uuid4().hex
    sources=list(SOURCES) if source=='all' else [source]
    require(all(s in SOURCES for s in sources),'Unsupported source',stage='ingestion')
    entries=[];counts={}
    for src in sources:
        locations=[None] if src=='earthquakes' else locations_by_source.get(src,[])
        if src!='earthquakes':
            from src.validation.location_registry import validate_selection_input
            validate_selection_input(locations,config.max_manifest_files,coverage_slot)
        require(bool(locations),'No enabled locations for '+src,stage='registry')
        counts[src]=len(locations)
        for loc in locations:
            identity=None if loc is None else loc['location_id']
            entries.append({'source':src,'location_id':identity,'location':loc,'path':str(config.run_dir(src,run_id)/((str(identity) if identity is not None else 'global')+'.json'))})
    plan={'version':2,'status':'planned','config_id':config.fingerprint,'run_id':run_id,'coverage_slot':coverage_slot,'sources':sources,'expected_files':len(entries),'expected_by_source':counts,'requests':entries}
    validate_manifest(plan,config,committed=False)
    return plan

def publish_run(plan,config):
    """Recovery can call this without fetching APIs when all planned files exist."""
    validate_raw_files(plan,config,committed=False)
    manifest=dict(plan,status='committed',files=[dict(e,sha256=file_digest(e['path'])) for e in plan['requests']])
    validate_raw_files(manifest,config)
    path=config.manifest_path(plan['run_id'])
    if path.exists():
        existing=load_committed(path,config)
        require(existing==manifest,'Commit record conflicts with immutable run plan',stage='publication')
    else:save_json(manifest,path,config.staging_root)
    # Re-read the actual durable record. A crash during publication never establishes
    # eligibility merely by leaving a destination filename behind.
    load_committed(path,config)
    return {'run_id':plan['run_id'],'manifest_path':str(path),'status':'committed','saved_files':len(manifest['files']),'sources':manifest['sources']}

def execute_plan(plan,config,*,max_retries=3,delay=0.5):
    validate_manifest(plan,config,committed=False)
    require(type(max_retries) is int and max_retries>0 and delay>=0,'Invalid retry configuration',stage='ingestion')
    for entry in plan['requests']:
        path=Path(entry['path']);src=entry['source'];loc=entry['location']
        if path.exists():
            validate_wrapper(read_json_file(path),src,plan['run_id'],entry)
            continue # Explicit recovery reuses valid immutable files.
        module=import_module('src.ingestion.'+('usgs_api' if src=='earthquakes' else src+'_api'))
        endpoint=os.getenv({'weather':'WEATHER_API_URL','air_quality':'AIR_QUALITY_API_URL','earthquakes':'USGS_API_URL'}[src],module.DEFAULT_API_URL)
        fetch=getattr(module,{'weather':'fetch_weather','air_quality':'fetch_air_quality','earthquakes':'fetch_earthquakes'}[src])
        for attempt in range(max_retries):
            try:
                data,_=fetch(*(() if loc is None else (loc['latitude'],loc['longitude'])),api_url=endpoint)
                break
            except ValidationError as exc:raise contextualize(exc,run_id=plan['run_id'],source=src) from None
            except Exception:
                if attempt+1==max_retries:raise ValidationError('API transport failed; endpoint details redacted') from None
                time.sleep(1.5**(attempt+1))
        meta={'source':src,'run_id':plan['run_id'],'retrieval_timestamp_utc':datetime.now(timezone.utc).isoformat(),'api_endpoint':safe_endpoint(endpoint)}
        if loc is not None:meta.update(location_id=loc['location_id'],location_name=loc['name'],latitude=loc['latitude'],longitude=loc['longitude'])
        payload={'metadata':meta,'raw_response':data}
        validate_wrapper(payload,src,plan['run_id'],entry)
        save_json(payload,path,config.staging_root) # Storage/publication errors propagate; not disguised as transport.
        time.sleep(delay)
    return publish_run(plan,config)

def recover_partial_run(plan_path,config=None,*,fetch_missing=False,max_retries=3,delay=0.5,spark=None):
    """Explicit recovery only: publish complete plan or resume its missing requests.

    Invalid existing files block; preserve them for audit and manually repair them.
    This function never invents location IDs, deletes files, or changes membership.
    """
    cfg=config or load_config();plan=read_json_file(plan_path)
    validate_manifest(plan,cfg,committed=False)
    require(Path(plan_path).resolve()==cfg.attempt_path(plan['run_id']).resolve(),'Unexpected plan path',stage='recovery')
    if fetch_missing:
        from src.validation.location_registry import validate_recovery_registry
        validate_recovery_registry(spark,plan,cfg)
        return execute_plan(plan,cfg,max_retries=max_retries,delay=delay)
    return publish_run(plan,cfg)

def run_ingestion(spark,dbutils,config=None):
    cfg=(config or load_config(parameter(dbutils,'config_path','') or None)).validate()
    source=parameter(dbutils,'source','all').strip().lower()
    discovery=import_module('src.ingestion.location_discovery')
    if source=='init_registry':return discovery.initialize_registry_geonames(spark,config=cfg)
    if source=='enable_locations':
        return discovery.enable_locations_for_ingestion(spark,limit=int(parameter(dbutils,'enable_limit',20)),weather=parameter(dbutils,'enable_weather','true').lower()=='true',air_quality=parameter(dbutils,'enable_air_quality','true').lower()=='true',distributed=parameter(dbutils,'distributed','false').lower()=='true',config=cfg)
    require(source in (*SOURCES,'all'),'Unsupported source',stage='ingestion')
    override=parameter(dbutils,'job_execution','auto').lower()
    require(override in ('auto','true','false'),'job_execution must be auto/true/false',stage='handoff')
    retries=int(os.getenv('MAX_RETRIES','3'));delay=float(os.getenv('BATCH_DELAY_SECONDS','0.5'))
    recovery=parameter(dbutils,'recovery_run_id','')
    if recovery:
        plan=read_json_file(cfg.attempt_path(recovery))
        validate_manifest(plan,cfg,committed=False)
        require(source=='all' or plan['sources']==[source],'Recovery source must match saved plan; use source=all for a multi-source plan',stage='recovery')
        result=recover_partial_run(cfg.attempt_path(recovery),cfg,fetch_missing=True,max_retries=retries,delay=delay,spark=spark)
    else:
        limit=int(parameter(dbutils,'max_locations',os.getenv('MAX_LOCATIONS','20')))
        slot=int(parameter(dbutils,'coverage_slot',datetime.now(timezone.utc).date().toordinal()))
        selected={s:discovery.select_ingestion_locations(spark,s,limit,slot,config=cfg) for s in (SOURCES if source=='all' else (source,)) if s!='earthquakes'}
        plan=make_plan(cfg,source,selected,slot)
        save_json(plan,cfg.attempt_path(plan['run_id']),cfg.staging_root)
        print(json.dumps({'status':'planned','run_id':plan['run_id'],'plan_path':str(cfg.attempt_path(plan['run_id']))}))
        result=execute_plan(plan,cfg,max_retries=retries,delay=delay)
    publish_task_values(dbutils,result,None if override=='auto' else override=='true')
    ValidationResult('ingestion',{'committed':True,'saved_files':result['saved_files']},context={'run_id':result['run_id']}).report()
    return result
