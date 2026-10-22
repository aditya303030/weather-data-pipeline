"""Validate API payloads without HTTP transport or Spark."""
from .common import require, numeric, timestamp, ValidationResult

WEATHER_CURRENT = ('temperature_2m', 'relative_humidity_2m', 'weather_code')
AQ_CURRENT = ('pm2_5', 'pm10', 'us_aqi')

def validate_response(source, data):
    require(isinstance(data, dict) and not data.get('error'), f'{source}: malformed/error API payload')
    if source == 'earthquakes':
        require(data.get('type') == 'FeatureCollection', 'Expected GeoJSON FeatureCollection')
        require(isinstance(data.get('metadata'), dict), 'Missing GeoJSON metadata')
        require(isinstance(data.get('features'), list), 'Missing GeoJSON features')
        numeric(data['metadata'].get('generated'), 0, nullable=False)
        require(data['metadata'].get('count') == len(data['features']), 'GeoJSON count mismatch')
        ids = []
        for f in data['features']:
            require(isinstance(f, dict) and f.get('type') == 'Feature', 'Malformed GeoJSON feature')
            require(isinstance(f.get('id'), str) and f['id'].strip(), 'Missing event ID')
            ids.append(f['id'])
            p, g = f.get('properties'), f.get('geometry')
            require(isinstance(p,dict) and isinstance(g,dict) and g.get('type') == 'Point', 'Malformed event properties/geometry')
            coords = g.get('coordinates')
            require(isinstance(coords,list) and len(coords)==3, 'Expected longitude, latitude, depth')
            numeric(coords[0], -180,180,nullable=False); numeric(coords[1],-90,90,nullable=False)
            numeric(coords[2],-20,1000,nullable=False)
            for field in ('time','updated'):
                value = p.get(field)
                require(isinstance(value,int) and not isinstance(value,bool), 'Event epochs must be integer milliseconds')
                numeric(value, 0, 253402300799999, nullable=False)
            require('mag' in p, 'Missing magnitude field')
            numeric(p['mag'], -10, 12)
        require(len(ids)==len(set(ids)), 'Duplicate event IDs in feed')
    else:
        require(source in ('weather','air_quality'), 'Unknown API source')
        numeric(data.get('latitude'),-90,90,nullable=False)
        numeric(data.get('longitude'),-180,180,nullable=False)
        require(data.get('utc_offset_seconds') == 0, 'API payload must use UTC')
        sections = ('current','hourly','daily') if source=='weather' else ('current','hourly')
        from importlib import import_module
        module=import_module('src.ingestion.'+source+'_api')
        for section in sections:
            block=data.get(section)
            require(isinstance(block,dict), f'Missing {section} section')
            variable_name=('WEATHER_' if source=='weather' else 'AQ_')+section.upper()+'_VARS'
            for field in getattr(module,variable_name).split(','):
                require(field in block, f'Missing {section}.{field}')
            if section=='current':
                timestamp(block.get('time'))
                for name in getattr(module,variable_name).split(','):
                    numeric(block[name])
            else:
                times=block.get('time')
                require(isinstance(times,list) and bool(times), f'Missing {section}.time')
                require(len(times)==len(set(times)), f'Duplicate {section} timestamps')
                for t in times: timestamp(t)
                required = ('temperature_2m',) if section=='hourly' and source=='weather' else (('pm2_5',) if section=='hourly' else ('temperature_2m_max',))
                for name in required: require(name in block, f'Missing {section}.{name}')
                for name,values in block.items():
                    if name=='time': continue
                    require(isinstance(values,list) and len(values)==len(times), f'{section}.{name}: parallel-array length mismatch')
                    for value in values:
                        if name in ('sunrise','sunset'):
                            if value is not None: timestamp(value)
                        else: numeric(value)
    return ValidationResult('api:'+source, {'valid':True})
