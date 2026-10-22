from dataclasses import replace
from pathlib import Path
from src.config import PipelineConfig
from src.contracts import variables

def config_at(root):
    root=Path(root)
    return replace(PipelineConfig(),catalog='offline_test',registry_source=str(root/'reference/US.txt'),raw_root=str(root/'raw'),staging_root=str(root/'stage'),manifest_root=str(root/'commits'),attempts_root=str(root/'attempts'),schema_root=str(root/'schemas'),checkpoint_root=str(root/'checkpoints')).validate()

def payload(source='weather',value=1):
    if source=='earthquakes':return {'type':'FeatureCollection','metadata':{'generated':1792540800123,'count':0},'features':[]}
    result={'latitude':40.,'longitude':-90.,'utc_offset_seconds':0}
    for section in ('current','hourly','daily') if source=='weather' else ('current','hourly'):
        block={name:(None if section=='current' else [None]) for name in variables(source,section)}
        block['time']='2026-10-21T00:00' if section=='current' else ['2026-10-21' if section=='daily' else '2026-10-21T00:00']
        if section=='current':block['temperature_2m' if source=='weather' else 'pm2_5']=value
        result[section]=block
    return result

def location(identity=1):return {'location_id':identity,'name':'Springfield','latitude':40.,'longitude':-90.}

def wrapper(source='weather',run='run',loc=None,data=None):
    meta={'source':source,'run_id':run,'retrieval_timestamp_utc':'2026-10-21T00:00:00+00:00','api_endpoint':'external-api'}
    if source!='earthquakes':
        loc=loc or location();meta.update(location_id=loc['location_id'],location_name=loc['name'],latitude=loc['latitude'],longitude=loc['longitude'])
    return {'metadata':meta,'raw_response':data if data is not None else payload(source)}
