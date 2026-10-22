"""Committed manifest is the sole downstream eligibility boundary."""
import hashlib
import json
import re
from pathlib import Path
from src.config import SOURCES, load_config
from .common import require, timestamp, numeric, ValidationResult, ValidationError, contextualize
from .ingestion import validate_response

def read_json_file(path):
    try:
        with Path(path).open(encoding='utf-8') as f:return json.load(f)
    except (OSError,ValueError):
        raise ValidationError('Missing/incomplete/invalid JSON file',result=None) from None

def file_digest(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):digest.update(chunk)
    return digest.hexdigest()

def validate_wrapper(data,source=None,run_id=None,entry=None):
    require(isinstance(data,dict) and isinstance(data.get('metadata'),dict) and 'raw_response' in data,'Invalid raw wrapper',stage='raw')
    m=data['metadata'];s=m.get('source')
    require(s in SOURCES and (source is None or source==s),'Unexpected raw source',stage='raw')
    require(isinstance(m.get('run_id'),str) and bool(m['run_id']) and (run_id is None or m['run_id']==run_id),'Unexpected run identity',stage='raw')
    timestamp(m.get('retrieval_timestamp_utc'),offset_required=True)
    if s!='earthquakes':
        require(type(m.get('location_id')) is int and m['location_id']>0,'Stable location_id required; explicitly adopt historical data',stage='raw')
        require(isinstance(m.get('location_name'),str) and bool(m['location_name'].strip()),'Missing location name',stage='raw')
        numeric(m.get('latitude'),-90,90,nullable=False);numeric(m.get('longitude'),-180,180,nullable=False)
        if entry:
            loc=entry['location']
            require(all(m.get(k)==loc.get(v) for k,v in [('location_id','location_id'),('location_name','name'),('latitude','latitude'),('longitude','longitude')]),'File location identity differs from request plan',stage='raw')
    validate_response(s,data['raw_response'])
    return ValidationResult('raw',{'wrapper':True},context={'source':s,'run_id':run_id})

def validate_manifest(manifest,config=None,*,committed=True):
    cfg=(config or load_config()).validate()
    require(isinstance(manifest,dict),'Invalid manifest',stage='manifest')
    run=manifest.get('run_id');require(isinstance(run,str) and re.fullmatch(r'[A-Za-z0-9_-]{1,100}',run) is not None,'Invalid run ID',stage='manifest')
    require(manifest.get('version')==2 and manifest.get('config_id')==cfg.fingerprint,'Manifest/config mismatch; legacy runs require explicit adoption',stage='manifest')
    require(manifest.get('status')==('committed' if committed else 'planned'),'Run is not committed' if committed else 'Invalid request plan',stage='manifest',context={'run_id':run})
    require(type(manifest.get('coverage_slot')) is int and manifest['coverage_slot']>=0,'Missing/invalid coverage slot',stage='manifest')
    sources=manifest.get('sources');requests=manifest.get('requests');counts=manifest.get('expected_by_source')
    require(isinstance(sources,list) and bool(sources) and all(s in SOURCES for s in sources) and len(sources)==len(set(sources)),'Unsupported/duplicate manifest sources',stage='manifest')
    require(isinstance(requests,list) and 0<len(requests)<=cfg.max_manifest_files,'Manifest scope must be nonempty and bounded',stage='manifest')
    require(isinstance(counts,dict) and set(counts)==set(sources) and all(type(n) is int and n>0 for n in counts.values()),'Invalid source coverage counts',stage='manifest')
    require(type(manifest.get('expected_files')) is int and manifest['expected_files']==len(requests)==sum(counts.values()),'Expected file-count mismatch',stage='manifest')
    identities=[];paths=[]
    for entry in requests:
        require(isinstance(entry,dict) and entry.get('source') in sources,'Invalid request source',stage='manifest')
        source=entry['source'];identity=entry.get('location_id')
        if source=='earthquakes':require(identity is None and entry.get('location') is None,'Global feed must not have location identity',stage='manifest')
        else:
            loc=entry.get('location');require(type(identity) is int and identity>0 and isinstance(loc,dict) and loc.get('location_id')==identity,'Invalid request location',stage='manifest')
            require(isinstance(loc.get('name'),str) and bool(loc['name'].strip()),'Missing request name',stage='manifest')
            numeric(loc.get('latitude'),-90,90,nullable=False);numeric(loc.get('longitude'),-180,180,nullable=False)
        expected=cfg.run_dir(source,run)/((str(identity) if identity is not None else 'global')+'.json')
        require(isinstance(entry.get('path'),str) and Path(entry['path']).resolve()==expected.resolve(),'File outside configured run/source root',stage='manifest')
        identities.append((source,identity));paths.append(str(expected.resolve()))
    require(len(set(identities))==len(identities) and len(set(paths))==len(paths),'Duplicate output paths or coverage identities',stage='manifest')
    require(all(sum(e['source']==s for e in requests)==counts[s] for s in sources),'Source coverage mismatch',stage='manifest')
    if 'earthquakes' in sources:require(counts['earthquakes']==1,'Exactly one global feed per run',stage='manifest')
    if committed:
        files=manifest.get('files');require(isinstance(files,list) and len(files)==len(requests),'Incomplete committed files',stage='manifest')
        require(all(isinstance(e,dict) and isinstance(e.get('path'),str) and isinstance(e.get('sha256'),str) and re.fullmatch('[0-9a-f]{64}',e['sha256']) for e in files),'Missing file digests',stage='manifest')
        expected={e['path']:e for e in requests}
        require(len({e.get('path') for e in files})==len(files) and all(e.get('path') in expected and all(e.get(k)==v for k,v in expected[e['path']].items()) for e in files),'Committed membership differs from request plan',stage='manifest')
        require(not manifest.get('errors'),'Failed run cannot commit',stage='manifest')
    return ValidationResult('manifest',{'membership':True,'files':len(requests)},context={'run_id':run})

def validate_raw_files(manifest,config=None,*,committed=True):
    validate_manifest(manifest,config,committed=committed)
    entries=manifest['files'] if committed else manifest['requests']
    for entry in entries:
        data=read_json_file(entry['path'])
        try:validate_wrapper(data,entry['source'],manifest['run_id'],entry)
        except ValidationError as exc:raise contextualize(exc,run_id=manifest['run_id'],source=entry['source']) from None
        if committed:require(file_digest(entry['path'])==entry['sha256'],'Committed file mutated or incompletely published',stage='raw',context={'run_id':manifest['run_id'],'source':entry['source']})
    return ValidationResult('raw',{'membership_and_payloads':True,'files':len(entries)},context={'run_id':manifest['run_id']})

def load_committed(manifest_path,config=None,source=None):
    cfg=config or load_config()
    m=read_json_file(manifest_path)
    validate_manifest(m,cfg)
    require(Path(manifest_path).resolve()==cfg.manifest_path(m['run_id']).resolve(),'Manifest must be the exact configured commit record',stage='manifest')
    require(source is None or source in m['sources'],'Selected source absent from committed run',stage='manifest')
    validate_raw_files(m,cfg)
    return m
