"""Offline regression tests. No test can make an HTTP request or use Databricks."""
import ast
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock

from src.validation.common import ValidationError
from src.validation.ingestion import validate_response
from src.validation.raw import validate_raw_files, validate_wrapper
from src.validation.location_registry import validate_activation_arguments
from src.ingestion.pipeline import select_locations, safe_endpoint, save_json, parameter, run_ingestion
from src.ingestion import weather_api, air_quality_api, usgs_api, location_discovery
from src.transformations.incremental import is_newer

ROOT=Path(__file__).resolve().parents[1]

def weather_payload():
    result={'latitude':40.,'longitude':-90.,'utc_offset_seconds':0}
    for section in ('current','hourly','daily'):
        fields=getattr(weather_api,'WEATHER_'+section.upper()+'_VARS').split(',')
        result[section]={f:None if section=='current' else [None] for f in fields}
        result[section]['time']='2026-10-21T00:00' if section=='current' else ['2026-10-21' if section=='daily' else '2026-10-21T00:00']
    return result

def feed(): return {'type':'FeatureCollection','metadata':{'generated':1792540800123,'count':0},'features':[]}

def wrapper():
    return {'metadata':{'source':'weather','run_id':'run','retrieval_timestamp_utc':'2026-10-21T00:00:00+00:00','location_id':1,'location_name':'Springfield','latitude':40.,'longitude':-90.},'raw_response':weather_payload()}

class NoLiveAccess(unittest.TestCase):
    def setUp(self):
        self.http=patch('requests.sessions.Session.request',side_effect=AssertionError('Live HTTP forbidden'))
        self.http.start(); self.addCleanup(self.http.stop)

class PayloadTests(NoLiveAccess):
    def test_nullable_measurements_and_grid_coordinates(self):
        validate_response('weather',weather_payload())
        validate_response('earthquakes',feed())
    def test_malformed_payloads(self):
        for data in ({},{'error':True},[],None):
            with self.assertRaises(ValidationError): validate_response('weather',data)
    def test_array_mismatch(self):
        data=weather_payload();data['hourly']['temperature_2m']=[1.,2.]
        with self.assertRaisesRegex(ValidationError,'length mismatch'): validate_response('weather',data)
    def test_api_client_calls_validator(self):
        response=Mock();response.json.return_value={'error':True}
        with patch.object(weather_api.requests,'get',return_value=response):
            with self.assertRaises(ValidationError):weather_api.fetch_weather(40,-90)
    def test_malformed_geojson_and_duplicate_events(self):
        data=feed();data['features']=[{}];data['metadata']['count']=1
        with self.assertRaises(ValidationError):validate_response('earthquakes',data)
    def test_required_run_and_identity(self):
        data=wrapper();validate_wrapper(data)
        del data['metadata']['location_id']
        with self.assertRaises(ValidationError):validate_wrapper(data)
    def test_missing_files_and_failure_manifest(self):
        with self.assertRaises(ValidationError):validate_raw_files({'run_id':'x','files':[],'expected_files':1})
        with self.assertRaises(ValidationError):validate_raw_files({'run_id':'x','files':[],'expected_files':0,'errors':['failure']})
    def test_unique_publication_and_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);p=root/'weather'/'one.json';data=wrapper()
            save_json(data,p,root/'_staging')
            validate_raw_files({'run_id':'run','files':[{'source':'weather','path':str(p)}],'expected_files':1})
            self.assertEqual(list((root/'_staging').iterdir()),[])
            with self.assertRaises(ValidationError):save_json(data,p,root/'_staging')
    def test_paths_unique_validation(self):
        with self.assertRaisesRegex(ValidationError,'Duplicate output paths'):
            validate_raw_files({'run_id':'x','files':[{'path':'a'},{'path':'a'}],'expected_files':2})

class ControlTests(NoLiveAccess):
    def test_deterministic_rotation_covers_registry(self):
        locations=[{'location_id':i} for i in [5,1,4,3,2]]
        selected=[x['location_id'] for slot in range(3) for x in select_locations(locations,2,slot)]
        self.assertEqual(selected,[1,2,3,4,5])
        self.assertEqual(select_locations(locations,2,1),select_locations(list(reversed(locations)),2,1))
        self.assertEqual(select_locations(locations,2,3),select_locations(locations,2,0))
    def test_empty_registry_input_rejected(self):
        from src.validation.location_registry import validate_merge_input
        with self.assertRaises(ValidationError):validate_merge_input([])

    def test_activation_argument_validation(self):
        for args in [(None,0,True,True,False),([],None,True,True,False),(['1 OR 1=1'],None,True,True,False),(None,5,False,False,False)]:
            with self.assertRaises(ValidationError):validate_activation_arguments(*args)
        validate_activation_arguments([2,1],None,True,False,False)
    def test_activation_sql_uses_only_ids(self):
        spark=Mock();spark.catalog.tableExists.return_value=True
        spark.sql.return_value.collect.return_value=[{'cnt':2}]
        location_discovery.enable_locations_for_ingestion(spark,location_ids=[2,1,1])
        update=[c.args[0] for c in spark.sql.call_args_list if c.args[0].startswith('UPDATE')][0]
        self.assertIn('location_id IN (1,2)',update)
        self.assertNotIn('ORDER BY',update);self.assertNotIn('LIMIT',update)
    def test_interactive_default(self):
        db=Mock();db.widgets.get.side_effect=Exception('missing')
        self.assertEqual(parameter(db,'source','all'),'all')
    def test_credentials_are_redacted(self):
        self.assertEqual(safe_endpoint('https://u:secret@example.org/api?apikey=secret#secret'),'https://example.org/api')
    def test_older_event_guard(self):
        old={'updated_at':1001,'snapshot_at':2};new={'updated_at':1002,'snapshot_at':1}
        self.assertFalse(is_newer(old,new));self.assertTrue(is_newer(new,old));self.assertTrue(is_newer(new,new))
    def test_all_mode_validation_propagates(self):
        db=Mock();db.widgets.get.side_effect=lambda name:{'source':'all','max_locations':'2','coverage_slot':'0'}.get(name,'')
        with patch.object(location_discovery,'load_active_locations',return_value=[{'location_id':1,'name':'X','latitude':1,'longitude':2}]),patch.object(weather_api,'fetch_weather',side_effect=ValidationError('invalid response')):
            with self.assertRaisesRegex(ValidationError,'invalid response'):run_ingestion(Mock(),db)
    def test_successful_all_mode_publishes_replayable_manifest(self):
        from src.validation.run_validation import run
        aq={'latitude':40.,'longitude':-90.,'utc_offset_seconds':0}
        aq['current']={k:None for k in air_quality_api.AQ_CURRENT_VARS.split(',')}
        aq['current']['time']='2026-10-21T00:00'
        aq['hourly']={k:[None] for k in air_quality_api.AQ_HOURLY_VARS.split(',')}
        aq['hourly']['time']=['2026-10-21T00:00']
        db=Mock();db.widgets.get.side_effect=lambda name:{'source':'all','max_locations':'2','coverage_slot':'0'}[name]
        with tempfile.TemporaryDirectory() as d,patch.dict('os.environ',{'BATCH_DELAY_SECONDS':'0','RAW_VOLUME_ROOT':d}),patch.object(location_discovery,'load_active_locations',return_value=[{'location_id':1,'name':'X','latitude':1,'longitude':2}]),patch.object(weather_api,'fetch_weather',return_value=(weather_payload(),{})),patch.object(air_quality_api,'fetch_air_quality',return_value=(aq,{})),patch.object(usgs_api,'fetch_earthquakes',return_value=(feed(),{})):
            manifest=run_ingestion(Mock(),db)
            self.assertEqual(manifest['expected_files'],3)
            self.assertEqual(len(manifest['files']),3)
            path=next((Path(d)/'_runs').glob('*.json'))
            run('raw','all',manifest=str(path),run_id=manifest['run_id'])
            with self.assertRaises(ValidationError):run('raw','all',manifest=str(path),run_id='wrong')

    def test_all_mode_transport_failure_blocks_success(self):
        db=Mock();db.widgets.get.side_effect=lambda name:{'source':'all','max_locations':'2','coverage_slot':'0'}[name]
        with tempfile.TemporaryDirectory() as d,patch.dict('os.environ',{'MAX_RETRIES':'1','BATCH_DELAY_SECONDS':'0','RAW_VOLUME_ROOT':d}),patch.object(location_discovery,'load_active_locations',return_value=[{'location_id':1,'name':'X','latitude':1,'longitude':2}]),patch.object(weather_api,'fetch_weather',side_effect=OSError('secret')),patch.object(air_quality_api,'fetch_air_quality',side_effect=OSError('secret')),patch.object(usgs_api,'fetch_earthquakes',side_effect=OSError('secret')):
            with self.assertRaises(ValidationError):run_ingestion(Mock(),db)
            self.assertFalse((Path(d)/'_runs').exists())

class NotebookTests(NoLiveAccess):
    def test_notebooks_have_no_validation_implementations(self):
        for p in [*ROOT.glob('notebooks/**/*.ipynb'),*ROOT.glob('src/**/*.ipynb')]:
            for cell in json.loads(p.read_text())['cells']:
                if cell['cell_type']!='code':continue
                tree=ast.parse(''.join(cell['source']))
                self.assertFalse(any(isinstance(n,(ast.Assert,ast.FunctionDef,ast.Raise)) for n in ast.walk(tree)),str(p))
                self.assertFalse(cell.get('outputs'),str(p))
    def test_location_id_final_projections_and_stable_keys(self):
        from src.validation.weather_silver import KEYS as wk
        from src.validation.air_quality_silver import KEYS as ak
        for keys in [*wk.values(),*ak.values()]:self.assertEqual(keys[0],'location_id');self.assertNotIn('location_name',keys)
        for name in ('weather','air_quality'):
            text=(ROOT/'src'/'transformations'/f'{name}.py').read_text()
            self.assertNotIn('"location_name", "latitude", "longitude", "elevation_m",',text)
    def test_no_automatic_silver_drop(self):
        for p in (ROOT/'src'/'transformations').glob('*.py'):self.assertNotIn('DROP TABLE',p.read_text())

if __name__=='__main__':unittest.main()
