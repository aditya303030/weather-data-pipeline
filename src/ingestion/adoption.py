"""Explicit offline historical adoption; never guess IDs or mark processing complete.

An operator supplies one reviewed snapshot cohort (at most one file per location).
Old files remain untouched. New v2 wrappers retain original payloads and retrieval
stamps; a provenance map is recorded in the immutable plan/manifest.
"""
from datetime import datetime, timezone
from src.validation.common import require, timestamp
from src.validation.raw import read_json_file, validate_wrapper, file_digest
from .pipeline import make_plan, save_json, publish_run, safe_endpoint

def adopt_reviewed_files(source,reviewed,config):
    require(source in ('weather','air_quality','earthquakes') and bool(reviewed),'Explicit source/cohort required',stage='adoption')
    require(source!='earthquakes' or len(reviewed)==1,'One earthquake feed per cohort',stage='adoption')
    require(all(isinstance(e,dict) and 'legacy_path' in e and (source=='earthquakes' or isinstance(e.get('location'),dict)) for e in reviewed),'Reviewed path/identity mapping required; names alone are insufficient',stage='adoption')
    locations=[] if source=='earthquakes' else [e['location'] for e in reviewed]
    plan=make_plan(config,source,{source:locations},datetime.now(timezone.utc).date().toordinal())
    plan['adoption']=[{'original_path':e['legacy_path'],'original_sha256':file_digest(e['legacy_path']),'reviewed_location':e.get('location')} for e in reviewed]
    payloads=[]
    for entry,old_entry in zip(plan['requests'],reviewed):
        old=read_json_file(old_entry['legacy_path'])
        require(isinstance(old,dict) and 'raw_response' in old,'Legacy wrapper missing payload; manual review required',stage='adoption')
        old_meta=old.get('metadata',{})
        stamp=old_meta.get('retrieval_timestamp_utc') or old_entry.get('reviewed_retrieval_timestamp')
        timestamp(stamp,offset_required=True)
        require(old_meta.get('source') in (None,source),'Legacy source disagrees with reviewed cohort',stage='adoption')
        m={'source':source,'run_id':plan['run_id'],'retrieval_timestamp_utc':stamp,'api_endpoint':safe_endpoint(None)}
        if source!='earthquakes':
            loc=entry['location']
            require(all(old_meta.get(k) is None or old_meta[k]==loc[v] for k,v in [('latitude','latitude'),('longitude','longitude'),('location_id','location_id')]),'Historical coordinates/ID conflict with reviewed mapping',stage='adoption')
            m.update(location_id=loc['location_id'],location_name=loc['name'],latitude=loc['latitude'],longitude=loc['longitude'])
        payload={'metadata':m,'raw_response':old['raw_response']}
        validate_wrapper(payload,source,plan['run_id'],entry)
        payloads.append(payload)
    save_json(plan,config.attempt_path(plan['run_id']),config.staging_root)
    for entry,payload in zip(plan['requests'],payloads):save_json(payload,entry['path'],config.staging_root)
    return publish_run(plan,config)
