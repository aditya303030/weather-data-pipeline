"""Only inspect files and manifests from an explicitly selected run."""
import json
from pathlib import Path
from .common import require, timestamp, numeric, reconcile, ValidationResult
from .ingestion import validate_response

def validate_wrapper(data, source=None, run_id=None):
    require(isinstance(data,dict) and isinstance(data.get('metadata'),dict) and 'raw_response' in data, 'Invalid raw wrapper')
    m=data['metadata']; s=m.get('source')
    require(s in ('weather','air_quality','earthquakes'), 'Invalid source')
    require(source is None or source==s, 'Unexpected source')
    require(isinstance(m.get('run_id'),str) and bool(m['run_id']), 'Missing run ID')
    require(run_id is None or m['run_id']==run_id, 'Unexpected run ID')
    timestamp(m.get('retrieval_timestamp_utc'),offset_required=True)
    if s!='earthquakes':
        require(isinstance(m.get('location_id'),int) and m['location_id']>0, 'Missing stable location ID; historical records require explicit backfill')
        require(isinstance(m.get('location_name'),str) and bool(m['location_name'].strip()), 'Missing location name')
        numeric(m.get('latitude'),-90,90,nullable=False); numeric(m.get('longitude'),-180,180,nullable=False)
    validate_response(s,data['raw_response'])
    return ValidationResult('raw_wrapper',{'valid':True})

def validate_raw_files(manifest):
    require(isinstance(manifest,dict),'Invalid run manifest')
    require(isinstance(manifest.get('run_id'),str) and bool(manifest['run_id']),'Manifest requires run ID')
    require(type(manifest.get('expected_files')) is int and manifest['expected_files']>=0,'Invalid expected file count')
    require(isinstance(manifest.get('files'),list),'Manifest requires file list')
    for item in manifest['files']:
        require(isinstance(item,dict) and isinstance(item.get('path'),str) and bool(item['path']),'Invalid manifest file entry')
    paths=[str(Path(x['path']).resolve()) for x in manifest['files']]
    require(len(paths)==len(set(paths)), 'Duplicate output paths')
    reconcile(len(paths),manifest['expected_files'],'raw files')
    for item in manifest['files']:
        require(item.get('source') in ('weather','air_quality','earthquakes'),'Missing file source')
        try:
            with Path(item['path']).open() as f: data=json.load(f)
        except (OSError,ValueError) as exc:
            from .common import ValidationError
            raise ValidationError('Missing or invalid raw file: '+item['path']) from exc
        validate_wrapper(data,item['source'],manifest['run_id'])
    require(not manifest.get('errors'), 'Required source ingestion failed')
    return ValidationResult('raw',{'files':len(paths),'run_id':manifest['run_id']}).report()

def read_json_file(path):
    """Validate serialized JSON before publication or independently during replay."""
    from .common import ValidationError
    try:
        with Path(path).open(encoding='utf-8') as f: return json.load(f)
    except (OSError,ValueError) as exc:
        raise ValidationError('Missing or invalid JSON file: '+str(path)) from exc
