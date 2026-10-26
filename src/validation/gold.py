"""Gold runtime validation only. Business SQL and publication live in notebooks.

DB executors expose query(sql)->list[dict]. Compatible rules share aggregates.
Checks fail closed; incomplete cohorts are explicitly excluded, not successful.
"""
import json
import hashlib
import math
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from src.validation.common import require, ValidationResult, ValidationError
from src.validation.raw import validate_manifest, validate_wrapper
from src.contracts import output_types

# Source defaults (Open-Meteo definitions); validate retained metadata when present.
WEATHER_UNITS={'snowfall':'cm','snowfall_sum':'cm','snow_depth':'m','precipitation':'mm','rain':'mm',
 'precipitation_sum':'mm','rain_sum':'mm','shortwave_radiation_sum':'MJ/m²','et0_fao_evapotranspiration':'mm',
 'precipitation_hours':'h','daylight_duration':'s','sunshine_duration':'s','visibility':'m','vapour_pressure_deficit':'kPa',
 'pressure_msl':'hPa','surface_pressure':'hPa','weather_code':'wmo code','is_day':'','uv_index':'','uv_index_max':'','uv_index_clear_sky':''}
for n in ('temperature_2m','apparent_temperature','dew_point_2m','wet_bulb_temperature_2m','temperature_2m_max','temperature_2m_min','apparent_temperature_max','apparent_temperature_min'):
    WEATHER_UNITS[n]='°C'
for n in ('relative_humidity_2m','cloud_cover','cloud_cover_low','cloud_cover_mid','cloud_cover_high','precipitation_probability_max'):WEATHER_UNITS[n]='%'
for n in ('wind_speed_10m','wind_speed_80m','wind_gusts_10m','wind_speed_10m_max','wind_gusts_10m_max'):WEATHER_UNITS[n]='km/h'
for n in ('wind_direction_10m','wind_direction_80m','wind_direction_10m_dominant'):WEATHER_UNITS[n]='°'
for n in ('shortwave_radiation','direct_radiation','direct_normal_irradiance','diffuse_radiation','terrestrial_radiation'):WEATHER_UNITS[n]='W/m²'
for depth in (0,6,18,54):WEATHER_UNITS[f'soil_temperature_{depth}cm']='°C'
for depth in ('0_to_1','1_to_3','3_to_9','9_to_27','27_to_81'):WEATHER_UNITS[f'soil_moisture_{depth}cm']='m³/m³'
AQ_UNITS=dict.fromkeys(('pm2_5','pm10','carbon_monoxide','nitrogen_dioxide','sulphur_dioxide','ozone','dust','ammonia'),'μg/m³')
for n in ('alder_pollen','birch_pollen','grass_pollen','mugwort_pollen','olive_pollen','ragweed_pollen'):AQ_UNITS[n]='grains/m³'
AQ_UNITS.update(aerosol_optical_depth='',uv_index='',uv_index_clear_sky='',european_aqi='EAQI',us_aqi='USAQI')
for n in ('carbon_monoxide','nitrogen_dioxide','ozone','pm10','pm2_5','sulphur_dioxide'):AQ_UNITS['us_aqi_'+n]='USAQI'

def validate_gold_config(settings):
    require(settings['catalog']=='weather_api_test' and (settings['schema']=='gold' or bool(re.fullmatch(r'gold_verify_[0-9a-f]{12}',settings['schema']))),
            'Only weather_api_test.gold or disposable gold_verify_<12 lowercase hex> schemas are supported',stage='gold_config')
    for key in ('nearby_radius_km','alignment_seconds','grid_degrees','stale_seconds'):
        v=settings[key];require(type(v) in (int,float) and math.isfinite(v) and v>0,'Invalid Gold setting: '+key,stage='gold_config')
    require(settings['nearby_radius_km']<=20000 and settings['grid_degrees']<=30 and
            abs(180/settings['grid_degrees']-round(180/settings['grid_degrees']))<1e-9,
            'Unsupported spherical radius/grid; grid must divide 180',stage='gold_config')
    require(type(settings['max_runs']) is int and 0<settings['max_runs']<=1000,'max_runs must be 1..1000',stage='gold_config')

def eligible_manifests(manifests,ledger,config):
    completed={}
    for row in ledger:
        key=(row['source'],row['path'])
        require(key not in completed and row.get('completed_at') is not None,'Invalid/duplicate Silver completion membership',stage='gold_eligibility')
        completed[key]=row['run_id']
    result=[];known=set()
    for m in manifests:
        validate_manifest(m,config)
        wanted=[(e['source'],e['path']) for e in m['files']];known.update(wanted)
        require(all(k not in completed or completed[k]==m['run_id'] for k in wanted),'Conflicting completion run',stage='gold_eligibility')
        if all(completed.get(k)==m['run_id'] for k in wanted):result.append(m)
    require(len({m['run_id'] for m in manifests})==len(manifests),'Duplicate manifests',stage='gold_eligibility')
    require(set(completed)<=known,'Completion row lacks committed manifest',stage='gold_eligibility')
    return result

def raw_evidence(text,digest,entry,manifest):
    require(digest==entry['sha256'],'Committed raw digest mismatch',stage='gold_eligibility')
    data=json.loads(text);validate_wrapper(data,entry['source'],manifest['run_id'],entry)
    source=entry['source'];p=data['raw_response'];defaults=WEATHER_UNITS if source=='weather' else AQ_UNITS
    checked=0;missing=0
    for section in ('current','hourly','daily'):
        if section not in p:continue
        units=p.get(section+'_units',{})
        require(isinstance(units,dict),'Malformed units metadata',stage='gold_units')
        for name in p[section]:
            if name not in defaults:continue
            if name not in units:missing+=1;continue
            norm=lambda v:str(v).replace('µ','μ').replace('seconds','s').replace('hours','h')
            require(norm(units[name])==norm(defaults[name]),f'Unsupported {source}.{section}.{name} units: {units[name]}',stage='gold_units')
            checked+=1
    counts=({'history':len(p['features'])} if source=='earthquakes' else
            {kind:(1 if kind=='current' else len(p[kind]['time'])) for kind in ('current','hourly','daily') if kind in p})
    return {'entry':entry,'run_id':manifest['run_id'],'snapshot_at':data['metadata']['retrieval_timestamp_utc'],
            'counts':counts,'unit_fields_verified':checked,'unit_defaults_used':missing,
            'feed_generated_ms':p.get('metadata',{}).get('generated')}

def validate_timezones(rows):
    for r in rows:
        try:ZoneInfo(r['timezone'])
        except (ZoneInfoNotFoundError,TypeError,ValueError):require(False,'Invalid registry timezone: '+str(r['location_id']),stage='gold_registry')

def schema(db,rel):
    return {r['col_name']:r['data_type'].lower() for r in db.query('DESCRIBE '+rel) if r['col_name'] and not r['col_name'].startswith('#')}

def compatible(db,rel,expected):
    require(schema(db,rel)==expected,'Incompatible Gold schema: '+rel+'; explicit reviewed migration required, never automatic DROP',stage='gold_schema')

def check_relation(db,rel,keys,*,required=(),predicates=None):
    conditions=dict(predicates or {})
    for c in set(keys)|set(required):conditions['missing_'+c]=f'`{c}` IS NULL'
    q='SELECT count(*) AS rows'+''.join(f',count_if({p}) AS c{i}' for i,p in enumerate(conditions.values()))+' FROM '+rel
    r=db.query(q)[0];bad={n:r['c'+str(i)] for i,n in enumerate(conditions) if r['c'+str(i)]}
    require(not bad,'Invalid Gold rows '+rel+': '+json.dumps(bad),stage='gold_data')
    group=','.join('`'+k+'`' for k in keys)
    n=db.query(f'SELECT count(*) n FROM (SELECT {group} FROM {rel} GROUP BY {group} HAVING count(*)>1)')[0]['n']
    require(n==0,'Duplicate logical versions/keys: '+rel,stage='gold_keys',affected_rows=n)
    return r['rows']

def reconcile(db,expected,actual):
    n=db.query(f'SELECT count(*) n FROM ((SELECT * FROM {expected} EXCEPT ALL SELECT * FROM {actual}) UNION ALL (SELECT * FROM {actual} EXCEPT ALL SELECT * FROM {expected}))')[0]['n']
    require(n==0,'Gold persistence/content reconciliation failed: '+actual,stage='gold_persistence',affected_rows=n)

def validate_sources(db,relations,registry,evidence):
    from src.validation.weather_silver import KEYS as W
    from src.validation.air_quality_silver import KEYS as A
    validate_timezones(db.query('SELECT location_id,timezone FROM '+registry))
    require(check_relation(db,registry,['location_id'],required=('location_name','latitude','longitude','timezone'),
        predicates={'id':'location_id<=0','coordinates':'latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180 OR isnan(latitude) OR isnan(longitude)'})>0,
        'Empty registry cannot replace location dimension',stage='gold_registry')
    for (source,kind),rel in relations.items():
        expected=output_types(source,kind);actual=schema(db,rel)
        require(all(actual.get(n)=={'long':'bigint'}.get(t,t) for n,t in expected.items()),'Silver schema migration required: '+rel,stage='gold_schema')
        keys=['event_id','snapshot_at'] if source=='earthquakes' else (W if source=='weather' else A)[kind]
        required=['snapshot_at','source_file_path','ingested_at','latitude','longitude']
        predicates={'rescued':'_rescued_data IS NOT NULL','coordinates':'latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180 OR isnan(latitude) OR isnan(longitude)'}
        if source=='earthquakes':
            required+=['event_time','updated_at','event_time_ms','updated_at_ms','feed_generated_ms','feed_generated_at','event_type']
            predicates.update(empty_id="trim(event_id)=''",epochs='event_time<>timestamp_millis(event_time_ms) OR updated_at<>timestamp_millis(updated_at_ms) OR feed_generated_at<>timestamp_millis(feed_generated_ms)')
        else:
            required+=['location_name']+(['observed_at'] if kind=='current' else [])
            predicates['id']='location_id<=0'
            require(db.query(f'SELECT count(*) n FROM {rel} s LEFT ANTI JOIN {registry} r ON s.location_id=r.location_id')[0]['n']==0,'Silver location missing from registry',stage='gold_registry')
        for n,t in expected.items():
            if t=='double':predicates['finite_'+n]=f"`{n}` IS NOT NULL AND (isnan(`{n}`) OR abs(`{n}`)=cast('Infinity' AS DOUBLE))"
        if source=='air_quality':
            from src.contracts import CURRENT_TYPES
            for n in CURRENT_TYPES[source]:predicates['negative_'+n]=f'`{n}`<0'
        if source=='weather':
            for n in ('precipitation','rain','snowfall','snow_depth','precipitation_sum','rain_sum','snowfall_sum','wind_speed_10m','wind_gusts_10m'):
                if n in expected:predicates['negative_'+n]=f'`{n}`<0'
            for n in ('relative_humidity_2m','cloud_cover','precipitation_probability_max'):
                if n in expected:predicates['percent_'+n]=f'`{n}` NOT BETWEEN 0 AND 100'
            if kind=='daily':predicates['minmax']='temperature_2m_min>temperature_2m_max OR apparent_temperature_min>apparent_temperature_max'
        check_relation(db,rel,keys,required=required,predicates=predicates)
        counts={r['source_file_path']:r['n'] for r in db.query('SELECT source_file_path,count(*) n FROM '+rel+' GROUP BY source_file_path')}
        entries=[e for e in evidence if e['entry']['source']==source]
        require(all(counts.get(e['entry']['path'],0)==e['counts'][kind] for e in entries) and set(counts)<=set(e['entry']['path'] for e in entries),
                'Eligible Silver/file expansion mismatch: '+rel,stage='gold_eligibility')
    return ValidationResult('gold_sources',{'eligible_files':len(evidence),'sources_validated':True})

def no_regression(db,old,new,keys,version=None):
    join=' AND '.join(f'o.`{k}`=n.`{k}`' for k in keys)
    condition=f'n.`{keys[0]}` IS NULL'+(' OR '+version if version else '')
    n=db.query(f'SELECT count(*) n FROM {old} o LEFT JOIN {new} n ON {join} WHERE {condition}')[0]['n']
    require(n==0,'Gold would regress selected versions or lose historical keys',stage='gold_regression')

def validate_publication_state(previous,versions,eligible_paths,contract,*,reviewed_upgrade=False):
    if not previous:return
    require(previous['contract_id']==contract or reviewed_upgrade,'Gold notebook contract changed; explicit view/schema migration required',stage='gold_publication')
    require(all(k in versions and versions[k]>=v for k,v in json.loads(previous['input_versions']).items()),'Input version regression',stage='gold_publication')
    require(set(json.loads(previous['eligible_paths']))<=set(eligible_paths),'Eligible history must not silently shrink',stage='gold_publication')

def validate_stage_dependency(completed,stage):
    required={'weather':set(),'air_quality':{'weather'},'earthquakes':{'weather','air_quality'}}[stage]
    require(required<=set(completed),'Missing successful Gold predecessors: '+','.join(sorted(required-set(completed))),stage='gold_dependency')

def validate_publication_completion(completed):
    require({'weather','air_quality','earthquakes'}<=set(completed),'Gold release is incomplete',stage='gold_publication')

def validate_derived(db,stages,derived):
    if 'environment_hourly' in derived:
        union=f"SELECT location_id,forecast_for FROM {stages['weather_hourly_latest']} UNION SELECT location_id,forecast_for FROM {stages['air_quality_hourly_latest']}"
        expected=db.query('SELECT count(*) n FROM ('+union+')')[0]['n']
        require(db.query('SELECT count(*) n FROM '+derived['environment_hourly'])[0]['n']==expected,'Full outer join lost/multiplied hours',stage='gold_join')
    if 'location_current' in derived:
        require(db.query('SELECT count(*) n FROM '+derived['location_current'])[0]['n']==db.query('SELECT count(*) n FROM '+stages['dim_location'])[0]['n'],'Location join lost/multiplied locations',stage='gold_join')
    for name in ('seismic_daily_summary','seismic_grid_daily'):
        if name in derived:
            require(db.query('SELECT coalesce(sum(event_count),0) n FROM '+derived[name])[0]['n']==db.query('SELECT count(*) n FROM '+stages['seismic_events_latest'])[0]['n'],'Seismic total mismatch: '+name,stage='gold_aggregate')
    if 'location_seismic_nearby' in derived:
        check_relation(db,derived['location_seismic_nearby'],['location_id','event_id'],predicates={'distance':'distance_km<0 OR distance_km>radius_km OR isnan(distance_km)'})
    return ValidationResult('gold_derived',{'joins_and_aggregates':True})

def require_stable_versions(db,versions,read_version):
    require(all(read_version(db,t)==v for t,v in versions.items()),'Pinned inputs changed during Gold release; restart weather_gold for a new release',stage='gold_publication')

def validate_count(observed,expected,label):
    require(observed==expected,label,stage='gold_reconciliation')

def validate_latest(db,source,selected,keys,*,seismic=False):
    join=' AND '.join(f's.`{k}`=t.`{k}`' for k in keys)
    order=('s.updated_at>t.updated_at OR (s.updated_at=t.updated_at AND s.snapshot_at>t.snapshot_at)' if seismic else 's.snapshot_at>t.snapshot_at')
    newer=db.query(f'SELECT count(*) n FROM {source} s JOIN {selected} t ON {join} WHERE {order}')[0]['n']
    validate_count(newer,0,'Gold selected an older eligible version')
    expected=db.query('SELECT count(*) n FROM (SELECT DISTINCT '+','.join(keys)+' FROM '+source+')')[0]['n']
    actual=db.query('SELECT count(*) n FROM '+selected)[0]['n']
    validate_count(actual,expected,'Gold selection lost/multiplied logical keys')

def validate_aqi_categories(db,relation):
    rules=[]
    for col,category,bands,last in [
        ('us_aqi','us_aqi_category',[(50,'Good'),(100,'Moderate'),(150,'Unhealthy for sensitive groups'),(200,'Unhealthy'),(300,'Very unhealthy')],'Hazardous'),
        ('european_aqi','european_aqi_category',[(20,'Good'),(40,'Fair'),(60,'Moderate'),(80,'Poor'),(100,'Very poor')],'Extremely poor')]:
        expected="CASE WHEN "+col+" IS NULL THEN 'Unknown' "+' '.join(f"WHEN {col}<={upper} THEN '{label}'" for upper,label in bands)+f" ELSE '{last}' END"
        rules.append(f"count_if(({col}<0 OR isnan(cast({col} AS DOUBLE)) OR abs(cast({col} AS DOUBLE))=cast('Infinity' AS DOUBLE)) OR NOT ({category} <=> ({expected}))) c{len(rules)}")
    row=db.query('SELECT '+','.join(rules)+' FROM '+relation)[0]
    require(all(not n for n in row.values()),'Invalid AQI value or null/incorrect category',stage='gold_categories')

def reconcile_derived(db,expected,actual,*,freshness=False):
    compatible(db,actual,schema(db,expected))
    if freshness:
        # These query-time values/publication instant legitimately differ across
        # candidate and consumer queries. All underlying timestamps/settings match.
        excluded='retrieval_age_seconds,gold_build_age_seconds,gold_published_at,retrieval_freshness'
        reconcile(db,f'(SELECT * EXCEPT({excluded}) FROM {expected})',f'(SELECT * EXCEPT({excluded}) FROM {actual})')
    else:reconcile(db,expected,actual)

def preflight_views(db,cfg,derived,contract_version):
    """Check every existing derived target before stage data publication."""
    quote=lambda s:"'"+s.replace("'","''")+"'"
    for name,expected in derived.items():
        rows=db.query(f'SELECT table_type,comment FROM {cfg.catalog}.information_schema.tables WHERE table_schema={quote(cfg.schema)} AND table_name={quote(name)}')
        if rows:
            require(len(rows)==1 and rows[0]['table_type']=='VIEW' and (rows[0]['comment'] or '').startswith('weather-pipeline Gold '+contract_version),
                    'Unmanaged derived view collision: '+name,stage='gold_schema')
            compatible(db,cfg.table(name),schema(db,expected))


STAGES={'weather','air_quality','earthquakes'}
STAGE_PRODUCTS={'weather':{'dim_location','weather_current_latest','weather_hourly_latest','weather_daily_latest'},'air_quality':{'air_quality_current_latest','air_quality_hourly_latest','weather_aq_coverage'},'earthquakes':{'seismic_events_latest','earthquake_snapshot_audit','seismic_coverage'}}
PUBLICATION_STATES={'CANDIDATE','READY','PUBLISHED','FAILED','WITHDRAWN','LEGACY_ACTIVE'}

def validate_control_rows(pointer,releases,stages,*,contract=None):
    """Complete control snapshot; no repair. Empty release ID is first-run sentinel."""
    require(len(pointer)==1 and pointer[0].get('singleton')==1,'Exactly one singleton=1 pointer required; no other keys',stage='gold_controls')
    p=pointer[0]
    require(type(p.get('generation')) is int and p['generation']>=0 and isinstance(p.get('release_id'),str),'Invalid pointer generation/identity',stage='gold_controls')
    require(bool(p.get('writer_token'))==bool(p.get('writer_stage')) and (p.get('writer_stage') is None or p['writer_stage'] in STAGES),'Invalid writer ownership',stage='gold_controls')
    if contract is not None:require(p.get('deployment_contract')==contract,'Reviewed Gold v2 migration/contract upgrade required; see README',stage='gold_controls')
    known={};seen=set();validated={}
    for r in releases:
        rid=r.get('release_id')
        require(isinstance(rid,str) and bool(re.fullmatch('[0-9a-f]{32}',rid)) and rid not in known,'Null/invalid/duplicate release identity',stage='gold_controls')
        require(r.get('publication_state') in PUBLICATION_STATES and r.get('health') in {'HEALTHY','UNHEALTHY','UNKNOWN'},'Invalid publication/health state',stage='gold_controls')
        require(type(r.get('expected_generation')) is int and r['expected_generation']>=0 and isinstance(r.get('expected_predecessor'),str),'Missing predecessor fence',stage='gold_controls')
        for field in ('contract_id','source_config_id','evidence_id'):
            require(isinstance(r.get(field),str) and bool(re.fullmatch('[0-9a-f]{64}',r[field])),'Malformed receipt '+field,stage='gold_controls')
        try:
            versions=json.loads(r['input_versions']);paths=json.loads(r['eligible_paths']);settings=json.loads(r['settings'])
            require(isinstance(versions,dict) and len(versions)==9 and all(type(v) is int and v>=0 for v in versions.values()),'Invalid nine pinned versions',stage='gold_controls')
            require(isinstance(paths,list) and len(paths)==len(set(paths)) and all(isinstance(x,str) for x in paths),'Invalid eligible membership',stage='gold_controls')
            validate_gold_config(settings)
        except (KeyError,TypeError,ValueError) as exc:
            require(False,'Malformed release JSON: '+type(exc).__name__,stage='gold_controls')
        require(r.get('built_at') is not None,'Release build timestamp missing',stage='gold_controls')
        if r['publication_state']=='PUBLISHED':require(r.get('published_at') is not None and r.get('verified_at') is not None,'Published state lacks successful verification timestamps',stage='gold_controls')
        known[rid]=r
    for row in stages:
        key=(row.get('release_id'),row.get('stage_name'))
        require(key not in seen and key[0] in known and key[1] in STAGES and row.get('status') in {'BUILDING','VALIDATED','FAILED'},'Duplicate/orphan/unrecognized stage receipt',stage='gold_controls')
        require(row.get('completed_at') is not None,'Stage receipt lacks timestamp',stage='gold_controls');seen.add(key)
        if row['status']=='VALIDATED':
            try:versions=json.loads(row['output_versions']);counts=json.loads(row['counts_json'])
            except (KeyError,TypeError,ValueError):require(False,'Validated stage lacks output evidence',stage='gold_controls')
            require(isinstance(versions,dict) and set(versions)==STAGE_PRODUCTS[key[1]] and isinstance(counts,dict) and STAGE_PRODUCTS[key[1]]<=set(counts) and all(type(x) is int and x>=0 for x in counts.values()) and all(type(v) is int and v>=0 for v in versions.values()),'Invalid stage output pins',stage='gold_controls')
            validated.setdefault(key[0],set()).add(key[1])
    for rid,r in known.items():
        require((r['expected_predecessor']=='' or r['expected_predecessor'] in known) and r['expected_generation']<=p['generation'],'Invalid predecessor reference/generation',stage='gold_controls')
        if r['publication_state'] in {'READY','PUBLISHED'}:
            require(validated.get(rid,set())==STAGES and r['health']=='HEALTHY','Published/ready release lacks healthy validated stages',stage='gold_controls')
    if p['release_id']:
        require(p['release_id'] in known,'Active release receipt missing',stage='gold_controls')
        active=known[p['release_id']]
        require(validated.get(p['release_id'],set())==STAGES and ((active['publication_state'] in {'READY','PUBLISHED'} and active['health']=='HEALTHY') or (active['publication_state']=='LEGACY_ACTIVE' and active['health']=='UNKNOWN')),'UNHEALTHY active release: operator recovery required',stage='gold_controls')
    return p

def validate_candidate_fence(pointer,receipt):
    if pointer['release_id']==receipt['release_id']:
        require(receipt['publication_state'] in {'READY','PUBLISHED'} and receipt['health']=='HEALTHY','Unhealthy replay',stage='gold_publication')
        return
    require(pointer['release_id']==receipt['expected_predecessor'] and pointer['generation']==receipt['expected_generation'],
            'Stale candidate: predecessor/generation changed (including ABA); restart weather',stage='gold_publication')

def validate_release_rows(db,relation,release_id):
    quote="'"+release_id.replace("'","''")+"'"
    validate_count(db.query(f'SELECT count_if(NOT (_release_id <=> {quote})) n FROM {relation}')[0]['n'],0,'Candidate rows belong to another/null release')

def validate_view_definition(db,cfg,name,query,contract_version):
    """Exact SQL ownership gate; upgrades require reviewed DDL, never auto-replace."""
    quote=lambda x:"'"+x.replace("'","''")+"'"
    rows=db.query(f"SELECT table_type,comment,view_definition,table_owner,(SELECT table_owner FROM {cfg.catalog}.information_schema.tables WHERE table_schema={quote(cfg.schema)} AND table_name='_gold_active') control_owner FROM {cfg.catalog}.information_schema.tables t LEFT JOIN {cfg.catalog}.information_schema.views v USING(table_catalog,table_schema,table_name) WHERE t.table_schema={quote(cfg.schema)} AND t.table_name={quote(name)}")
    if not rows:return False
    r=rows[0]
    normalize=lambda x:tuple(token if token.startswith("'") else token.strip('`').casefold() for token in re.findall(r"'(?:''|[^'])*'|`(?:``|[^`])*`|[A-Za-z_][A-Za-z_0-9]*|[0-9]+(?:\.[0-9]+)?|<=>|<>|<=|>=|[^\s;]",x or ''))
    require(len(rows)==1 and r['table_type']=='VIEW' and r['table_owner'] and r['table_owner']==r['control_owner'] and (r['comment'] or '').startswith('weather-pipeline Gold '+contract_version),'Foreign/unowned view; reviewed migration required: '+name,stage='gold_schema')
    require(bool(r.get('view_definition')) and normalize(r['view_definition'])==normalize(query),'Managed view SQL differs or is unreadable; reviewed migration required: '+name,stage='gold_schema')
    return True


def validate_writer_pointer(rows,token):
    require(len(rows)==1 and rows[0].get('singleton')==1 and type(rows[0].get('generation')) is int and rows[0]['generation']>=0 and bool(token) and rows[0].get('writer_token')==token,'Writer ownership lost; abort without mutations',stage='gold_writer')
    return rows[0]

def validate_legacy_upgrade(pointer,releases,stages,config_id):
    require(len(pointer)==1 and pointer[0].get('singleton')==1 and isinstance(pointer[0].get('release_id'),str) and bool(pointer[0]['release_id']),'Legacy migration requires one existing active release',stage='gold_migration')
    ids=[r.get('release_id') for r in releases]
    require(all(isinstance(x,str) and bool(re.fullmatch('[0-9a-f]{32}',x)) for x in ids) and len(ids)==len(set(ids)) and pointer[0]['release_id'] in ids,'Invalid legacy release identities',stage='gold_migration')
    seen=set();validated={}
    for row in stages:
        key=(row['release_id'],row['stage_name'])
        require(key not in seen and key[0] in ids and key[1] in STAGES and row['status'] in {'BUILDING','VALIDATED','FAILED'},'Invalid legacy stage controls',stage='gold_migration');seen.add(key)
        if row['status']=='VALIDATED':validated.setdefault(key[0],set()).add(key[1])
    require(validated.get(pointer[0]['release_id'])==STAGES,'Legacy active release is not fully validated',stage='gold_migration')
    require(all(r['source_config_id']==config_id for r in releases),'Legacy source configuration mismatch',stage='gold_migration')


def validate_task_handoff(result,saved):
    require(result.get('stage')=='weather' and result.get('ready_for_next_stage') is True and result.get('validate_only') is False and isinstance(result.get('release_id'),str) and bool(re.fullmatch('[0-9a-f]{32}',result['release_id'])),'Weather handoff lacks a validated persisted release',stage='gold_handoff')
    require(saved is not False,'Databricks task-value publication returned False',stage='gold_handoff')


def validate_recovery_confirmation(*,confirmed_terminated,termination_evidence):
    require(confirmed_terminated is True and isinstance(termination_evidence,str) and len(termination_evidence.strip())>=12,
      'Recovery denied: verified owner termination and a durable evidence reference are required; age/timeout is insufficient',stage='gold_recovery')


def validate_migration_plan(plan,cfg):
    require(plan.get('target')==cfg.catalog+'.'+cfg.schema,'Migration destination differs from plan',stage='gold_migration')
    digest=hashlib.sha256(json.dumps({k:plan[k] for k in ('target','contract','baseline','sql')},sort_keys=True).encode()).hexdigest()
    require(plan.get('migration_id')==digest,'Migration plan digest mismatch',stage='gold_migration')
    for statement in plan['sql']:
        match=re.match(r'^(?:ALTER TABLE|UPDATE|CREATE OR REPLACE VIEW)\s+([a-zA-Z0-9_.]+)(?:\s|\()',statement)
        require(match is not None and match[1].startswith(cfg.catalog+'.'+cfg.schema+'.') and match[1].count('.')==2,'Migration statement destination is not allowlisted',stage='gold_migration')
        # All generated operations are single statements; quoted comments contain no semicolons.
        require(';' not in re.sub(r"'(?:''|\\.|[^'])*'",'',statement),'Multiple migration statements are forbidden',stage='gold_migration')

def validate_migration_transition(before,after,statement):
    target=re.match(r'^(?:ALTER TABLE|UPDATE|CREATE OR REPLACE VIEW)\s+([a-zA-Z0-9_.]+)',statement)[1].split('.')[-1]
    require({k:v for k,v in before['objects'].items() if k!=target}=={k:v for k,v in after['objects'].items() if k!=target},'Unexpected other-object metadata change during migration statement',stage='gold_migration')
    for name,version in before['versions'].items():
        expected=version+1 if name==target else version
        require(after['versions'].get(name)==expected,'Unexpected Delta version transition for '+name,stage='gold_migration')

