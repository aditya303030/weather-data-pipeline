"""Pure API contract checks; transport/retries remain in ingestion."""
from src.contracts import variables, EQ_DOUBLES, EQ_LONGS, EQ_STRINGS, CURRENT_TYPES, specs
from .common import require, numeric, timestamp, ValidationResult

def validate_response(source,data):
    require(isinstance(data,dict) and not data.get('error'),'Malformed/error API payload',stage='api',context={'source':source})
    require(source in ('weather','air_quality','earthquakes'),'Unsupported source',stage='api')
    if source=='earthquakes':
        require(data.get('type')=='FeatureCollection','Expected FeatureCollection',stage='api')
        meta=data.get('metadata');features=data.get('features')
        require(isinstance(meta,dict) and isinstance(features,list),'Missing GeoJSON metadata/features',stage='api')
        require(type(meta.get('generated')) is int and meta['generated']>=0,'Generated epoch must be integer milliseconds',stage='api')
        require(type(meta.get('count')) is int and meta['count']==len(features),'GeoJSON count mismatch',stage='api')
        ids=[]
        for f in features:
            require(isinstance(f,dict) and f.get('type')=='Feature' and isinstance(f.get('id'),str) and bool(f['id'].strip()),'Invalid feature identity',stage='api')
            ids.append(f['id']);p=f.get('properties');g=f.get('geometry')
            require(isinstance(p,dict) and isinstance(g,dict) and g.get('type')=='Point','Invalid feature structure',stage='api')
            c=g.get('coordinates');require(isinstance(c,list) and len(c)==3,'Expected longitude, latitude, depth',stage='api')
            for v,lo,hi in zip(c,(-180,-90,-20),(180,90,1000)):numeric(v,lo,hi,nullable=False)
            for name in ('time','updated'):require(type(p.get(name)) is int and 0<=p[name]<=253402300799999,'Missing/invalid event epoch milliseconds',stage='api')
            for name in EQ_DOUBLES:
                if name in p:numeric(p[name])
            for name in EQ_LONGS:
                require(p.get(name) is None or type(p[name]) is int,'Optional event integer field has invalid type',stage='api')
            require(p.get('tsunami') is None or p['tsunami'] in (0,1),'Invalid tsunami flag',stage='api')
            for name in EQ_STRINGS:
                require(p.get(name) is None or isinstance(p[name],str),'Invalid optional event string',stage='api')
            numeric(p.get('mag'),-10,12)
        require(len(ids)==len(set(ids)),'Duplicate event IDs',stage='api')
    else:
        numeric(data.get('latitude'),-90,90,nullable=False);numeric(data.get('longitude'),-180,180,nullable=False)
        require(type(data.get('utc_offset_seconds')) is int and data['utc_offset_seconds']==0,'Payload must use UTC',stage='api')
        numeric(data.get('elevation'))
        for section in ('current','hourly','daily') if source=='weather' else ('current','hourly'):
            block=data.get(section);require(isinstance(block,dict),'Missing '+section+' section',stage='api')
            if section=='current':
                timestamp(block.get('time'));numeric(block.get('interval'),0)
                require(block.get('interval') is None or type(block['interval']) is int,'Optional interval must be integer seconds',stage='api')
                for name in variables(source,section):
                    value=block.get(name);numeric(value)
                    if CURRENT_TYPES[source][name]=='int':require(value is None or float(value).is_integer(),'Integer measurement is fractional',stage='api')
                    if name=='is_day':require(value is None or value in (0,1),'is_day must be 0 or 1',stage='api')
            else:
                times=block.get('time');require(isinstance(times,list) and bool(times),'Missing '+section+'.time',stage='api')
                require(all(isinstance(t,str) for t in times) and len(times)==len(set(times)),'Invalid/duplicate forecast timestamps',stage='api')
                for t in times:timestamp(t)
                for name in variables(source,section):
                    values=block.get(name)
                    if values is None:continue # Optional absent arrays become typed null arrays in transforms.
                    require(isinstance(values,list) and len(values)==len(times),section+'.'+name+': parallel-array length mismatch',stage='api')
                    for value in values:
                        if name in ('sunrise','sunset'):
                            if value is not None:timestamp(value)
                        else:
                            numeric(value)
                            field_type=dict((n,t) for n,_,t in specs(source,section))[name]
                            if field_type=='int':require(value is None or float(value).is_integer(),'Integer measurement is fractional',stage='api')
                            if name=='is_day':require(value is None or value in (0,1),'is_day must be 0 or 1',stage='api')
    return ValidationResult('api',{'contract':True},context={'source':source})
