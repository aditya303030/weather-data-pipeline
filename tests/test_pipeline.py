"""Offline tests: source contracts, publication and eligibility. HTTP is blocked."""
import ast
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock,patch
from types import SimpleNamespace
from helpers import config_at,payload,location,wrapper
from src.ingestion.pipeline import make_plan,save_json,publish_run,execute_plan,recover_partial_run,select_locations,safe_endpoint,parameter,run_ingestion
from src.ingestion.databricks import publish_task_values
from src.validation.raw import validate_wrapper,validate_raw_files,validate_manifest,load_committed
from src.validation.ingestion import validate_response
from src.validation.common import ValidationError
from src.config import load_config,PipelineConfig
from src.ingestion import weather_api,air_quality_api,usgs_api,location_discovery

# Legacy fixture imports used by optional Spark tests.
weather_payload=payload
feed=lambda:payload('earthquakes')
ROOT=Path(__file__).resolve().parents[1]

class Offline(unittest.TestCase):
    def setUp(self):
        guard=patch('requests.sessions.Session.request',side_effect=AssertionError('Live HTTP forbidden'))
        guard.start();self.addCleanup(guard.stop)

class PayloadTests(Offline):
    def test_optional_null_and_missing_fields(self):
        for source in ('weather','air_quality'):
            data=payload(source)
            # Entire optional arrays, elevation and interval can be missing.
            for section in ('current','hourly','daily') if source=='weather' else ('current','hourly'):
                data[section]={'time':data[section]['time']}
            validate_response(source,data)
        validate_response('earthquakes',feed())
    def test_malformed_and_array_mismatch(self):
        for bad in ({},{'error':True},[],None):
            with self.assertRaises(ValidationError):validate_response('weather',bad)
        data=payload();data['hourly']['temperature_2m']=[1,2]
        with self.assertRaisesRegex(ValidationError,'length mismatch'):validate_response('weather',data)
    def test_invalid_numeric_and_required_time(self):
        data=payload(value='bad')
        with self.assertRaises(ValidationError):validate_response('weather',data)
        data=payload();del data['current']['time']
        with self.assertRaises(ValidationError):validate_response('weather',data)
    def test_required_run_and_identity(self):
        data=wrapper();validate_wrapper(data)
        del data['metadata']['location_id']
        with self.assertRaises(ValidationError):validate_wrapper(data)
    def test_api_client_uses_validator(self):
        r=Mock();r.json.return_value={'error':True}
        with patch.object(weather_api.requests,'get',return_value=r):
            with self.assertRaises(ValidationError):weather_api.fetch_weather(40,-90)
    def test_malformed_geojson_and_duplicate_ids(self):
        data=feed();data['features']=[{}];data['metadata']['count']=1
        with self.assertRaises(ValidationError):validate_response('earthquakes',data)
        f={'type':'Feature','id':'x','properties':{'time':1001,'updated':1002},'geometry':{'type':'Point','coordinates':[-90,40,10]}}
        data=feed();data['features']=[f,f];data['metadata']['count']=2
        with self.assertRaisesRegex(ValidationError,'Duplicate event'):validate_response('earthquakes',data)

class PublicationTests(Offline):
    def plan(self,cfg,run='run',two=False):
        return make_plan(cfg,'weather',{'weather':[location(),location(2)] if two else [location()]},0,run)
    def save(self,plan,index=0):
        e=plan['requests'][index]
        save_json(wrapper(e['source'],plan['run_id'],e['location']),e['path'],self.cfg.staging_root)
    def setUp(self):
        super().setUp();temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.cfg=config_at(temp.name)
    def test_partial_failure_is_ineligible_and_later_run_is_exact(self):
        failed=self.plan(self.cfg,'failed',True);self.save(failed)
        save_json(failed,self.cfg.attempt_path('failed'),self.cfg.staging_root)
        with self.assertRaises(ValidationError):publish_run(failed,self.cfg)
        self.assertFalse(self.cfg.manifest_path('failed').exists())
        good=self.plan(self.cfg,'good');self.save(good);result=publish_run(good,self.cfg)
        manifest=load_committed(result['manifest_path'],self.cfg,'weather')
        self.assertEqual([e['path'] for e in manifest['files']],[good['requests'][0]['path']])
        self.assertNotIn(failed['requests'][0]['path'],str(manifest))
        # A plan/file's presence does not grant downstream eligibility.
        with self.assertRaises(ValidationError):load_committed(self.cfg.attempt_path('failed'),self.cfg)
    def test_interrupted_commit_and_recovery(self):
        plan=self.plan(self.cfg);self.save(plan)
        save_json(plan,self.cfg.attempt_path('run'),self.cfg.staging_root)
        with patch('src.ingestion.pipeline.os.rename',side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):publish_run(plan,self.cfg)
        self.assertFalse(self.cfg.manifest_path('run').exists())
        first=recover_partial_run(self.cfg.attempt_path('run'),self.cfg)
        second=publish_run(plan,self.cfg)
        self.assertEqual(first,second)
    def test_partial_destination_never_eligible(self):
        plan=self.plan(self.cfg);self.save(plan)
        dest=self.cfg.manifest_path('run');dest.parent.mkdir(parents=True);dest.write_text('{')
        with self.assertRaises(ValidationError):load_committed(dest,self.cfg)
        with self.assertRaises(ValidationError):publish_run(plan,self.cfg)
        # Explicit repair retains the interrupted record for audit, then resumes.
        dest.rename(dest.with_suffix('.incomplete'))
        publish_run(plan,self.cfg)
    def test_explicit_resume_fetches_only_missing(self):
        plan=self.plan(self.cfg,two=True);self.save(plan)
        save_json(plan,self.cfg.attempt_path('run'),self.cfg.staging_root)
        original=Path(plan['requests'][0]['path']).read_bytes()
        with patch.object(weather_api,'fetch_weather',return_value=(payload(),{})) as fetch,patch('src.validation.location_registry.validate_recovery_registry') as registry_check:
            recover_partial_run(self.cfg.attempt_path('run'),self.cfg,fetch_missing=True,delay=0)
            self.assertEqual(fetch.call_count,1)
            registry_check.assert_called_once()
        self.assertEqual(Path(plan['requests'][0]['path']).read_bytes(),original)
    def test_identity_root_count_and_digest_mismatch(self):
        import copy
        plan=self.plan(self.cfg);self.save(plan);result=publish_run(plan,self.cfg)
        m=load_committed(result['manifest_path'],self.cfg)
        for change in ('source','path','location','count','run'):
            bad=copy.deepcopy(m)
            if change=='source':bad['sources']=['unknown']
            if change=='path':bad['requests'][0]['path']='/elsewhere/file.json'
            if change=='location':bad['files'][0]['location_id']=2
            if change=='count':bad['expected_files']=2
            if change=='run':bad['run_id']='another'
            with self.assertRaises(ValidationError):validate_manifest(bad,self.cfg)
        Path(m['files'][0]['path']).write_text(json.dumps(wrapper(data=payload(value=2))))
        with self.assertRaisesRegex(ValidationError,'mutated'):load_committed(result['manifest_path'],self.cfg)
    def test_all_mode_transport_and_validation_fail_closed(self):
        for error in (OSError('secret-url'),ValidationError('invalid response')):
            db=SimpleNamespace(widgets=SimpleNamespace(get=lambda name:{'source':'all','max_locations':'1','coverage_slot':'0'}.get(name,'')))
            with patch.object(location_discovery,'select_ingestion_locations',return_value=[location()]),patch.object(weather_api,'fetch_weather',side_effect=error),patch.dict('os.environ',{'MAX_RETRIES':'1','BATCH_DELAY_SECONDS':'0'}):
                with self.assertRaises(ValidationError):run_ingestion(Mock(),db,self.cfg)
        self.assertFalse(Path(self.cfg.manifest_root).exists())

class ControlTests(Offline):
    def test_deterministic_rotation_and_invalid_ids(self):
        locs=[location(i) for i in [5,1,4,3,2]]
        self.assertEqual([x['location_id'] for slot in range(3) for x in select_locations(locs,2,slot)],[1,2,3,4,5])
        for bad in (None,'x',0,True):
            with self.assertRaises(ValidationError):select_locations([dict(location(),location_id=bad)],2,0)
    def test_activation_arguments(self):
        from src.validation.location_registry import validate_activation_arguments
        for args in [(None,0,True,True,False),([],None,True,True,False),(['1 OR 1=1'],None,True,True,False)]:
            with self.assertRaises(ValidationError):validate_activation_arguments(*args)
    def test_default_and_security(self):
        db=Mock();db.widgets.get.side_effect=KeyError('source')
        self.assertEqual(parameter(db,'source','all'),'all')
        self.assertEqual(safe_endpoint('https://user:secret@secret.example/secret?key=secret'),'external-api')
    def test_task_values_only_after_success_and_errors_propagate(self):
        result={'run_id':'run','manifest_path':'/commit/run.json'};db=Mock()
        publish_task_values(db,result,False);db.jobs.taskValues.set.assert_not_called()
        publish_task_values(db,result,True)
        self.assertEqual(db.jobs.taskValues.set.call_count,2)
        db.jobs.taskValues.set.side_effect=RuntimeError('failure')
        with self.assertRaises(RuntimeError):publish_task_values(db,result,True)
    def test_config_is_complete_and_isolated(self):
        from dataclasses import asdict,replace
        with tempfile.TemporaryDirectory() as d:
            cfg=config_at(d);p=Path(d)/'config.json';p.write_text(json.dumps(asdict(cfg)))
            self.assertEqual(load_config(p),cfg)
            p.write_text('{"catalog":"test"}')
            with self.assertRaises(ValidationError):load_config(p)
            with self.assertRaises(ValidationError):replace(PipelineConfig(),catalog='test').validate()
    def test_structured_failure(self):
        from src.validation.common import require
        with self.assertRaises(ValidationError) as caught:require(False,'bad',stage='raw',check='identity',affected_rows=2,context={'run_id':'run'})
        self.assertEqual(caught.exception.result.affected_rows,2)
        self.assertEqual(caught.exception.result.context['run_id'],'run')

class NotebookTests(Offline):
    def test_thin_notebooks(self):
        # Gold notebooks are deliberate self-contained releases: definitions,
        # runtime gates, and orchestration live in their tagged cells by design.
        # Thinness applies to ingest/bronze/silver wrappers only.
        excluded=(ROOT/'notebooks/gold').resolve()
        for p in [*ROOT.glob('notebooks/**/*.ipynb'),*ROOT.glob('src/**/*.ipynb')]:
            if p.resolve().is_relative_to(excluded):continue
            for c in json.loads(p.read_text())['cells']:
                if c['cell_type']!='code':continue
                t=ast.parse(''.join(c['source']))
                self.assertFalse(any(isinstance(n,(ast.Assert,ast.FunctionDef,ast.Raise)) for n in ast.walk(t)),str(p))
    def test_stable_keys_and_no_drop(self):
        from src.validation.weather_silver import KEYS
        for keys in KEYS.values():self.assertEqual(keys[0],'location_id')
        for p in (ROOT/'src/transformations').glob('*.py'):self.assertNotIn('DROP TABLE',p.read_text())

class AdoptionAndHandoffTests(Offline):
    def test_reviewed_adoption_preserves_payload_and_original(self):
        from src.ingestion.adoption import adopt_reviewed_files
        with tempfile.TemporaryDirectory() as d:
            cfg=config_at(d);old=Path(d)/'legacy.json'
            original=wrapper();original['metadata'].pop('run_id');original['metadata'].pop('location_id')
            old.write_text(json.dumps(original));before=old.read_bytes()
            result=adopt_reviewed_files('weather',[{'legacy_path':str(old),'location':location()}],cfg)
            m=load_committed(result['manifest_path'],cfg)
            new=json.loads(Path(m['files'][0]['path']).read_text())
            self.assertEqual(new['raw_response'],original['raw_response'])
            self.assertEqual(old.read_bytes(),before)
            self.assertIn('adoption',m)
    def test_adoption_refuses_missing_identity_and_coordinate_conflict(self):
        from src.ingestion.adoption import adopt_reviewed_files
        with tempfile.TemporaryDirectory() as d:
            cfg=config_at(d);old=Path(d)/'old.json';old.write_text(json.dumps(wrapper()))
            with self.assertRaises(ValidationError):adopt_reviewed_files('weather',[{'legacy_path':str(old)}],cfg)
            with self.assertRaises(ValidationError):adopt_reviewed_files('weather',[{'legacy_path':str(old),'location':dict(location(),latitude=1)}],cfg)
            self.assertFalse(Path(cfg.manifest_root).exists())
    def test_successful_interactive_ingestion_and_standalone_config(self):
        from src.validation.run_validation import run
        with tempfile.TemporaryDirectory() as d:
            cfg=config_at(d)
            db=SimpleNamespace(widgets=SimpleNamespace(get=lambda name:{'source':'earthquakes'}.get(name,'')))
            with patch.object(usgs_api,'fetch_earthquakes',return_value=(feed(),{})),patch.dict('os.environ',{'BATCH_DELAY_SECONDS':'0'}):
                result=run_ingestion(Mock(),db,cfg)
            self.assertEqual(result['status'],'committed')
            run('raw','earthquakes',manifest=result['manifest_path'],config=cfg,run_id=result['run_id'])
            with self.assertRaises(ValidationError):run('raw','weather',manifest=result['manifest_path'],config=cfg)
    def test_recovery_requires_bounded_explicit_scopes(self):
        from src.transformations.recovery import recover_committed
        with tempfile.TemporaryDirectory() as d:
            for paths in ([],(x for x in []),['x']*11):
                with self.assertRaises(ValidationError):recover_committed(Mock(),paths,'weather',config_at(d))
    def test_unknown_widget_error_is_not_suppressed(self):
        db=Mock();db.widgets.get.side_effect=RuntimeError('permission denied')
        with self.assertRaises(RuntimeError):parameter(db,'source','all')
    def test_job_autodetection_and_publication(self):
        db=Mock();db.notebook.entry_point.getDbutils.return_value.notebook.return_value.getContext.return_value.jobId.return_value.isDefined.return_value=True
        publish_task_values(db,{'run_id':'r','manifest_path':'/r.json'})
        self.assertEqual(db.jobs.taskValues.set.call_args_list[0].kwargs,{'key':'run_id','value':'r'})
