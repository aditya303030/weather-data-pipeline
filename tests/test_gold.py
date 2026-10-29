"""Offline Gold gates and actual pointer-publication control, no live services."""
import json
import re
import unittest
from datetime import datetime, timezone
from src.config import load_config
import ast
import sys
import types
from pathlib import Path
from types import SimpleNamespace

def load_notebooks(root=None):
    """Use the actual canonical bootstrap, never widget/publication cells."""
    root=Path(root or Path(__file__).resolve().parents[1])
    module=types.ModuleType('gold_notebook_definitions');sys.modules[module.__name__]=module
    namespace=vars(module)
    nb=json.loads((root/'notebooks/gold/weather_gold.ipynb').read_text())
    boot=[c for c in nb['cells'] if c.get('metadata',{}).get('tags')==['gold_bootstrap']]
    if len(boot)!=1:raise ValueError('Missing canonical bootstrap')
    exec(compile(''.join(boot[0]['source']),'gold_bootstrap','exec'),namespace)
    namespace['load_gold_definitions'](root,['weather','air_quality','earthquakes'],namespace)
    return namespace

N=load_notebooks()
GoldConfig=N['GoldConfig']
def activate_release(db,cfg,new,old,verify):
    return N['activate_release'](db,cfg,new,old,verify,generation=db.generation,writer_token='writer',verify_prior=lambda:None)
literal=N['literal']
sql=SimpleNamespace(measurement=lambda relation,registry,source,kind:
    N['weather_query'](relation,registry,kind) if source=='weather' else N['air_quality_query'](relation,registry,kind))
from src.validation.gold import WEATHER_UNITS,AQ_UNITS

def connection_settings(path):
    """Test/manual SQL runner reads literals without executing the IDE script."""
    tree=ast.parse(Path(path).read_text())
    calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='connect']
    if len(calls)!=1:raise ValueError('Expected one literal SQL connection call')
    return {k.arg:ast.literal_eval(k.value) for k in calls[0].keywords}

from src.validation.common import ValidationError
from src.validation.gold import eligible_manifests, raw_evidence, validate_publication_state, validate_timezones
from tests.helpers import wrapper

class GoldTests(unittest.TestCase):
    def setUp(self):
        self.cfg=load_config(str(Path(__file__).resolve().parents[1]/'config/isolated.example.json'))
    def manifest(self,run='one',sources=('weather','air_quality','earthquakes')):
        requests=[]
        for s in sources:
            loc=None if s=='earthquakes' else {'location_id':1,'name':'Springfield','latitude':40.,'longitude':-90.}
            requests.append({'source':s,'location_id':None if loc is None else 1,'location':loc,
                'path':str(self.cfg.run_dir(s,run)/('global.json' if loc is None else '1.json'))})
        return {'version':2,'status':'committed','config_id':self.cfg.fingerprint,'run_id':run,'coverage_slot':0,
                'sources':list(sources),'requests':requests,'expected_files':len(requests),
                'expected_by_source':dict.fromkeys(sources,1),
                'files':[{**r,'sha256':'a'*64} for r in requests]}
    def ledger(self,m):return [{'source':e['source'],'path':e['path'],'run_id':m['run_id'],'completed_at':datetime.now(timezone.utc)} for e in m['files']]
    def test_whole_run_completion_not_just_one_source(self):
        a=self.manifest();b=self.manifest('two')
        self.assertEqual(eligible_manifests([a,b],self.ledger(a)+self.ledger(b)[:1],self.cfg),[a])
        self.assertEqual(eligible_manifests([a],self.ledger(a)[:-1],self.cfg),[])
    def test_missing_manifest_or_duplicate_ledger_blocks(self):
        m=self.manifest();ledger=self.ledger(m)
        with self.assertRaises(ValidationError):eligible_manifests([],ledger,self.cfg)
        with self.assertRaises(ValidationError):eligible_manifests([m],ledger+ledger[:1],self.cfg)
    def test_manifest_fingerprint_unchanged(self):
        m=self.manifest();m['config_id']='different'
        with self.assertRaises(ValidationError):eligible_manifests([m],[],self.cfg)
    def test_raw_digest_and_explicit_snow_units(self):
        m=self.manifest(sources=('weather',));e=m['files'][0]
        data=wrapper(run='one');data['raw_response']['hourly_units']={'snowfall':'cm','snow_depth':'m','shortwave_radiation':'W/m²'}
        out=raw_evidence(json.dumps(data),'a'*64,e,m)
        self.assertEqual(out['unit_fields_verified'],3)
        data['raw_response']['hourly_units']['snow_depth']='cm'
        with self.assertRaises(ValidationError):raw_evidence(json.dumps(data),'a'*64,e,m)
        with self.assertRaises(ValidationError):raw_evidence(json.dumps(data),'b'*64,e,m)
        data['raw_response']['hourly_units']['snow_depth']='m'
        data['raw_response']['hourly_units']['shortwave_radiation']='MJ/m²'
        with self.assertRaises(ValidationError):raw_evidence(json.dumps(data),'a'*64,e,m)
    def test_zero_event_feed_keeps_retrieval_evidence(self):
        m=self.manifest(sources=('earthquakes',));out=raw_evidence(json.dumps(wrapper('earthquakes',run='one')),'a'*64,m['files'][0],m)
        self.assertEqual(out['counts']['history'],0)
        self.assertIsNotNone(out['feed_generated_ms'])
    def test_no_eligible_history_is_valid(self):self.assertEqual(eligible_manifests([],[],self.cfg),[])
    def test_gold_config_cannot_escape_or_modify_silver(self):
        for cfg in [GoldConfig(catalog='weather_api',schema='gold_verify_0123456789ab'),GoldConfig(catalog=''),GoldConfig(catalog='weather_api_test',schema='silver'),
                    GoldConfig(catalog='weather_api_test',schema='x; DROP'),GoldConfig(catalog='weather_api_test',nearby_radius_km=float('nan')),
                    GoldConfig(catalog='weather_api_test',grid_degrees=7),GoldConfig(catalog='weather_api_test',max_runs=0)]:
            with self.assertRaises(ValidationError):cfg.validate()
        self.assertEqual(GoldConfig(catalog='weather_api').validate().schema,'gold')
        self.assertEqual(GoldConfig(catalog='weather_api_test').validate().nearby_radius_km,250)
    def test_timezones_required_not_invented(self):
        validate_timezones([{'location_id':1,'timezone':'America/New_York'}])
        with self.assertRaises(ValidationError):validate_timezones([{'location_id':1,'timezone':'Unknown'}])
    def test_publication_cannot_regress_versions_or_membership(self):
        old={'contract_id':'v1','input_versions':'{"table":2}','eligible_paths':'["old"]'}
        validate_publication_state(old,{'table':2},['old','new'],'v1')
        for versions,paths,contract in [({'table':1},['old'],'v1'),({'table':2},[],'v1'),({'table':2},['old'],'v2')]:
            with self.assertRaises(ValidationError):validate_publication_state(old,versions,paths,contract)
    def test_failed_publication_restores_prior_pointer(self):
        db=PointerDB('old')
        with self.assertRaisesRegex(RuntimeError,'postwrite'):
            activate_release(db,GoldConfig(catalog='weather_api_test'),'new','old',lambda:(_ for _ in ()).throw(RuntimeError('postwrite')))
        self.assertEqual(db.active,'old')
    def test_idempotent_pointer_and_success(self):
        db=PointerDB('old');verified=[]
        activate_release(db,GoldConfig(catalog='weather_api_test'),'new','old',lambda:verified.append(True))
        activate_release(db,GoldConfig(catalog='weather_api_test'),'new','new',lambda:verified.append(True))
        self.assertEqual((db.active,len(verified)),('new',2))
    def test_competing_publication_fails_without_overwriting_other_release(self):
        db=PointerDB('other')
        with self.assertRaises(ValidationError):activate_release(db,GoldConfig(catalog='weather_api_test'),'new','old',lambda:None)
        self.assertEqual(db.active,'other')
    def test_source_and_output_contracts_keep_advanced_fields(self):
        q=sql.measurement('s','r','weather','hourly')
        for col in ['location_id','soil_moisture_27_to_81cm','forecast_for','snapshot_at','valid_local_display']:
            self.assertIn(col,q)
        self.assertNotIn(' WHERE forecast_for>',q)
        self.assertEqual(WEATHER_UNITS['shortwave_radiation_sum'],'MJ/m²')
        self.assertEqual(WEATHER_UNITS['snowfall'],'cm')
        self.assertEqual(WEATHER_UNITS['snow_depth'],'m')
    def test_sql_quote(self):self.assertEqual(literal("a'b"),"'a''b'")

    def test_stages_cannot_publish_without_successful_predecessors(self):
        from src.validation.gold import validate_stage_dependency,validate_publication_completion
        validate_stage_dependency(set(),'weather')
        for completed,stage in [(set(),'air_quality'),({'weather'},'earthquakes')]:
            with self.assertRaises(ValidationError):validate_stage_dependency(completed,stage)
        with self.assertRaises(ValidationError):validate_publication_completion({'weather','air_quality'})
        validate_publication_completion({'weather','air_quality','earthquakes'})

    def test_notebooks_define_without_connecting_or_running_widgets(self):
        import unittest.mock as mock
        with mock.patch('socket.socket',side_effect=RuntimeError('Unexpected network')):
            loaded=load_notebooks()
        for name in ('run_weather','run_air_quality','run_earthquakes'):
            self.assertTrue(callable(loaded[name]))
        self.assertFalse(Path('src/gold').exists())

    def test_actual_weather_orchestrator_marks_write_failure_without_publication(self):
        from unittest.mock import Mock,patch
        class Context(dict):
            def close(self):self['closed']=True
        ctx=Context(release_id='candidate',sources={('weather',k):'source' for k in ('current','hourly','daily')},
            stages={name:'stage' for name in N['WEATHER_KEYS']},report={},db=Mock(metrics=Mock(return_value={})),cfg=GoldConfig(catalog='weather_api_test'),registry='registry')
        status=Mock();activate=Mock()
        replacements={'prepare_context':Mock(return_value=ctx),'stage_products':Mock(return_value=dict.fromkeys(N['WEATHER_KEYS'],1)),
            'ensure_state':Mock(),'_active_id':Mock(return_value='prior'),'stage_status':status,
            'preflight_and_write':Mock(side_effect=RuntimeError('write failed')),'activate_release':activate,'record_failure':Mock()}
        with patch.dict(N,replacements),patch('src.validation.gold.validate_latest'):
            with self.assertRaisesRegex(RuntimeError,'write failed'):
                N['run_weather'](object(),config_path='unused',validate_only=False)
        self.assertEqual([c.args[2] for c in status.call_args_list],['BUILDING','FAILED'])
        activate.assert_not_called();self.assertTrue(ctx['closed'])

    def test_actual_weather_active_retry_validates_without_resetting_stage_status(self):
        from unittest.mock import Mock,patch
        class Context(dict):
            def close(self):self['closed']=True
        ctx=Context(release_id='same',sources={('weather',k):'source' for k in ('current','hourly','daily')},
            stages={name:'stage' for name in N['WEATHER_KEYS']},report={},db=Mock(metrics=Mock(return_value={})),cfg=GoldConfig(catalog='weather_api_test'),registry='registry')
        status=Mock();persist=Mock()
        with patch.dict(N,{'prepare_context':Mock(return_value=ctx),'stage_products':Mock(return_value=dict.fromkeys(N['WEATHER_KEYS'],1)),
            'ensure_state':Mock(),'_active_id':Mock(return_value='same'),'stage_status':status,'preflight_and_write':persist,'record_failure':Mock()}),patch('src.validation.gold.validate_latest'):
            result=N['run_weather'](object(),config_path='unused',validate_only=False)
        status.assert_not_called();persist.assert_called_once();self.assertEqual(result['release_id'],'same')

    def controls_fixture(self):
        from src.validation.gold import STAGE_PRODUCTS
        rid='a'*32
        pointer=[{'singleton':1,'release_id':rid,'generation':3,'writer_token':None,'writer_stage':None,'deployment_contract':'contract'}]
        receipt={'release_id':rid,'contract_id':'c'*64,'source_config_id':'d'*64,'evidence_id':'e'*64,
            'input_versions':json.dumps({str(i):0 for i in range(9)}),'eligible_paths':'[]','settings':GoldConfig(catalog='weather_api_test').json(),
            'expected_predecessor':'','expected_generation':0,'publication_state':'PUBLISHED','health':'HEALTHY','built_at':datetime.now(timezone.utc),'published_at':datetime.now(timezone.utc),'verified_at':datetime.now(timezone.utc)}
        stages=[{'release_id':rid,'stage_name':stage,'status':'VALIDATED','completed_at':datetime.now(timezone.utc),
                 'output_versions':json.dumps(dict.fromkeys(products,0)),'counts_json':json.dumps(dict.fromkeys(products,0))} for stage,products in STAGE_PRODUCTS.items()]
        return pointer,[receipt],stages

    def test_complete_control_invariants_and_empty_first_run(self):
        from src.validation.gold import validate_control_rows
        import copy
        args=self.controls_fixture();validate_control_rows(*args,contract='contract')
        empty=copy.deepcopy(args[0]);empty[0]['release_id']='';validate_control_rows(empty,[],[],contract='contract')
        mutations=[lambda p,r,s:p.append(dict(p[0],singleton=2)),lambda p,r,s:p[0].update(singleton=None),
            lambda p,r,s:r.append(dict(r[0])),lambda p,r,s:r[0].update(release_id=None),
            lambda p,r,s:s.append(dict(s[0])),lambda p,r,s:s[0].update(stage_name='surprise'),
            lambda p,r,s:s[0].update(status='OK'),lambda p,r,s:s.pop(),
            lambda p,r,s:r[0].update(health='UNHEALTHY'),lambda p,r,s:r[0].update(input_versions='{}'),
            lambda p,r,s:s[0].update(output_versions='{}')]
        for mutate in mutations:
            values=copy.deepcopy(args);mutate(*values)
            with self.subTest(mutation=mutate),self.assertRaises(ValidationError):validate_control_rows(*values,contract='contract')

    def test_generation_fence_rejects_stale_same_input_and_aba(self):
        from src.validation.gold import validate_candidate_fence
        p,r,_=self.controls_fixture();r=r[0];r.update(publication_state='CANDIDATE',expected_predecessor='old',expected_generation=4)
        validate_candidate_fence({'release_id':'old','generation':4},r)
        for pointer in [{'release_id':'new','generation':5},{'release_id':'old','generation':6}]:
            with self.assertRaises(ValidationError):validate_candidate_fence(pointer,r)

    def test_writer_acquisition_is_conditional_and_busy_owner_is_not_cleared(self):
        from unittest.mock import Mock,patch
        db=Mock();db.query.side_effect=[[],[{'writer_token':'other'}]]
        ctx={'db':db,'cfg':GoldConfig(catalog='weather_api_test')}
        with patch.dict(N,{'controls':Mock()}),self.assertRaises(ValidationError):N['acquire_writer'](ctx,'weather')
        self.assertIn('AND writer_token IS NULL',db.query.call_args_list[0].args[0])
        self.assertNotIn('writer_token',ctx)

    def test_activation_requires_verified_prior_and_generation(self):
        from unittest.mock import Mock
        db=PointerDB('old');prior=Mock(side_effect=ValidationError('prior corrupt'))
        with self.assertRaises(ValidationError):N['activate_release'](db,GoldConfig(catalog='weather_api_test'),'new','old',lambda:None,generation=0,writer_token='writer',verify_prior=prior)
        self.assertEqual((db.active,db.generation),('old',0))
        activate_release(db,GoldConfig(catalog='weather_api_test'),'new','old',lambda:None)
        self.assertEqual(db.generation,1)
        activate_release(db,GoldConfig(catalog='weather_api_test'),'new','new',lambda:None)
        self.assertEqual(db.generation,1)

    def test_weather_task_value_success_validate_only_and_failure(self):
        from unittest.mock import Mock
        utilities=Mock();result={'stage':'weather','release_id':'a'*32,'validate_only':False,'ready_for_next_stage':True}
        self.assertTrue(N['publish_weather_task_value'](utilities,result,job_mode=True))
        utilities.jobs.taskValues.set.assert_called_once_with(key='release_id',value='a'*32)
        utilities.reset_mock()
        for change in [{'validate_only':True},{'ready_for_next_stage':False}]:
            self.assertFalse(N['publish_weather_task_value'](utilities,result|change,job_mode=True))
        utilities.jobs.taskValues.set.assert_not_called()
        utilities.jobs.taskValues.set.side_effect=RuntimeError('job handoff failed')
        with self.assertRaises(RuntimeError):N['publish_weather_task_value'](utilities,result,job_mode=True)
        self.assertFalse(N['publish_weather_task_value'](object(),result,job_mode=False))
        utilities.jobs.taskValues.set.side_effect=None;utilities.jobs.taskValues.set.return_value=False
        with self.assertRaises(ValidationError):N['publish_weather_task_value'](utilities,result,job_mode=True)

    def test_root_and_config_from_multiple_working_directories(self):
        import os
        root=Path(__file__).resolve().parents[1];prior=Path.cwd()
        try:
            for directory in [root,root/'notebooks/gold',root/'tests']:
                os.chdir(directory)
                self.assertEqual(N['resolve_project_root'](),root)
                self.assertEqual(N['resolve_config_path'](root,'config/isolated.example.json'),str(root/'config/isolated.example.json'))
                self.assertEqual(N['resolve_config_path'](root,str(root/'config/isolated.example.json')),str(root/'config/isolated.example.json'))
            with self.assertRaises(ValueError):N['resolve_project_root']('relative')
            with self.assertRaises(FileNotFoundError):N['resolve_config_path'](root,'missing.json')
        finally:os.chdir(prior)

    def test_bootstrap_rejects_missing_malformed_and_duplicate_definitions(self):
        import tempfile,shutil
        root=Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as d:
            out=Path(d);(out/'notebooks/gold').mkdir(parents=True)
            for name in ('weather','air_quality','earthquakes'):shutil.copy(root/f'notebooks/gold/{name}_gold.ipynb',out/'notebooks/gold')
            target=out/'notebooks/gold/air_quality_gold.ipynb';original=target.read_text();book=json.loads(original)
            for cell in book['cells']:
                if cell.get('metadata',{}).get('tags')==['gold_definitions']:cell['metadata']['tags']='gold_definitions';break
            target.write_text(json.dumps(book))
            with self.assertRaises(ValueError):N['definition_cells'](out,'air_quality')
            target.write_text(original);target.unlink()
            with self.assertRaises(ValueError):N['definition_cells'](out,'air_quality')
        with self.assertRaises(ValueError):N['load_gold_definitions'](root,['weather','weather'],{})
        with self.assertRaises(ValueError):N['load_gold_definitions'](root,['weather'],N)

    def test_spark_and_connector_adapter_metrics_without_runtime_claim(self):
        from unittest.mock import Mock
        spark=Mock();spark.sql.return_value.collect.return_value=[Mock(asDict=Mock(return_value={'n':1}))]
        executor=N['SparkExecutor'](spark);self.assertEqual(executor.query('SELECT 1 n'),[{'n':1}]);self.assertEqual(executor.metrics()['query_count'],1)
        class Connection:
            def cursor(self):return self
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def execute(self,q):self.description=[('n',)]
            def fetchall(self):return [(2,)]
        self.assertEqual(N['SQLExecutor'](Connection()).query('SELECT 2 n'),[{'n':2}])

    def test_immutable_writer_has_no_delete_or_active_insert_and_history_is_filtered(self):
        import inspect
        # Inspect actual notebook definitions: no alternate test implementation.
        root=Path(__file__).resolve().parents[1]
        cells=N['definition_cells'](root,'weather');tree=ast.parse('\n'.join(cells))
        node=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='preflight_and_write')
        body=ast.get_source_segment('\n'.join(cells),node)
        self.assertNotIn('DELETE FROM',body);self.assertIn('if active!=rid:',body);self.assertIn('require_writer(ctx)',body)
        history=N['copy_view_queries'](GoldConfig(catalog='weather_api_test'),'weather_current_latest')['weather_current_latest_history']
        self.assertIn("publication_state='PUBLISHED'",history);self.assertIn("health='HEALTHY'",history)

    def test_active_failure_records_unhealthy_without_rewriting_data(self):
        from unittest.mock import Mock
        db=Mock();db.query.side_effect=[[{'writer_token':'owner','release_id':'a'*32}],[]]
        ctx={'db':db,'cfg':GoldConfig(catalog='weather_api_test'),'release_id':'a'*32,'writer_token':'owner','write_stage':'weather','failure_mutations_allowed':True}
        N['record_failure'](ctx,ValidationError('copy mismatch'))
        statement=db.query.call_args.args[0]
        self.assertIn("health='UNHEALTHY'",statement);self.assertNotIn('DELETE',statement);self.assertNotIn('SET release_id',statement)

    def test_view_preflight_preserves_literals_and_checks_owner(self):
        from unittest.mock import Mock
        from src.validation.gold import validate_view_definition
        db=Mock();db.query.return_value=[{'table_type':'VIEW','comment':'weather-pipeline Gold v2','table_owner':'owner','control_owner':'owner','view_definition':"SELECT 'HEALTHY' status"}]
        validate_view_definition(db,GoldConfig(catalog='weather_api_test'),'example',"select 'HEALTHY' status",'v2')
        for expected in ["select 'healthy' status","select 'OTHER' status"]:
            with self.assertRaises(ValidationError):validate_view_definition(db,GoldConfig(catalog='weather_api_test'),'example',expected,'v2')
        db.query.return_value[0]['table_owner']='foreign'
        with self.assertRaises(ValidationError):validate_view_definition(db,GoldConfig(catalog='weather_api_test'),'example',"SELECT 'HEALTHY' status",'v2')

    def test_empty_provision_plan_is_pure_and_has_fenced_schema(self):
        from unittest.mock import patch
        root=Path(__file__).resolve().parents[1]
        with patch('socket.socket',side_effect=RuntimeError('network forbidden')):
            plan=N['migration_plan'](project_root=root,gold=GoldConfig(catalog='weather_api_test'))
        self.assertEqual(plan['mode'],'FIRST_RUN_EMPTY_STATE')
        self.assertEqual(len(plan['sql']),5)
        text=' '.join(plan['sql'])
        for field in ('generation BIGINT','writer_token STRING','expected_generation BIGINT','publication_state STRING','output_versions STRING'):
            self.assertIn(field,text)
        self.assertNotIn('DROP ',text)

    def test_persisted_dependency_corruption_blocks_rollback_authorization(self):
        from unittest.mock import Mock,patch
        db=Mock();db.query.return_value=[{'stage_name':s,'status':'VALIDATED','output_versions':json.dumps({s:4})} for s in ('weather','air_quality','earthquakes')]
        ctx={'db':db,'cfg':GoldConfig(catalog='weather_api_test')}
        with patch('src.validation.gold.reconcile',side_effect=ValidationError('corrupted backing')):
            with self.assertRaises(ValidationError):N['verify_release_copies'](ctx,'a'*32)
        self.assertEqual(ctx['suspect_release'],'a'*32)

    def test_output_persisted_then_receipt_failure_is_truthful(self):
        from unittest.mock import Mock,patch
        class Context(dict):
            def close(self):self['closed']=True
        ctx=Context(release_id='candidate',sources={('weather',k):'source' for k in ('current','hourly','daily')},
            stages={name:'stage' for name in N['WEATHER_KEYS']},report={},db=Mock(),cfg=GoldConfig(catalog='weather_api_test'),registry='registry')
        def status(*args,**kw):
            if args[2]=='VALIDATED':raise RuntimeError('receipt failed after write')
        status_mock=Mock(side_effect=status);persist=Mock();failure=Mock()
        with patch.dict(N,{'prepare_context':Mock(return_value=ctx),'stage_products':Mock(return_value=dict.fromkeys(N['WEATHER_KEYS'],1)),
            'ensure_state':Mock(),'_active_id':Mock(return_value='old'),'stage_status':status_mock,'preflight_and_write':persist,'record_failure':failure}),patch('src.validation.gold.validate_latest'):
            with self.assertRaisesRegex(RuntimeError,'receipt failed'):N['run_weather'](object(),config_path='unused',validate_only=False)
        persist.assert_called_once();failure.assert_called_once();self.assertTrue(ctx['closed'])
        self.assertEqual([c.args[2] for c in status_mock.call_args_list],['BUILDING','VALIDATED','FAILED'])

    def test_actual_parameter_cell_from_different_working_directories(self):
        import os
        from unittest.mock import Mock
        root=Path(__file__).resolve().parents[1];old=Path.cwd();old_path=list(sys.path)
        book=json.loads((root/'notebooks/gold/weather_gold.ipynb').read_text())
        source=next(''.join(c['source']) for c in book['cells'] if 'Minimal source locator' in ''.join(c.get('source',[])))
        class Widgets:
            def __init__(self):self.values={}
            def text(self,n,d):self.values.setdefault(n,d)
            def dropdown(self,n,d,choices):self.values.setdefault(n,d)
            def get(self,n):return self.values[n]
        try:
            for directory in (root,root/'tests',root/'notebooks/gold'):
                os.chdir(directory);scope={'dbutils':SimpleNamespace(widgets=Widgets())}
                exec(compile(source,'actual_weather_parameters','exec'),scope)
                self.assertEqual(scope['project_root'],root)
                self.assertEqual(scope['config_path'],str(root/'config/isolated.example.json'))
        finally:os.chdir(old);sys.path[:]=old_path

    def test_rejected_stale_or_invalid_invocation_does_not_withdraw_history(self):
        from unittest.mock import Mock
        db=Mock()
        N['record_failure']({'db':db,'cfg':GoldConfig(catalog='weather_api_test'),'release_id':'a'*32,'writer_token':'owner'},ValidationError('stale parameter'))
        db.query.assert_not_called()

    def test_disposable_destination_is_tightly_scoped(self):
        cfg=GoldConfig(catalog='weather_api_test', schema='gold_verify_0123456789ab').validate()
        root=Path(__file__).resolve().parents[1]
        plan=N['migration_plan'](project_root=root,gold=cfg)
        self.assertTrue(all('weather_api_test.gold.' not in q for q in plan['sql']))
        mapping={name:cfg.table(name) for name in N['WEATHER_KEYS']|N['AQ_KEYS']|N['SEISMIC_KEYS']}
        mapping['_gold_settings']=cfg.table('_gold_releases')
        for q in (N['air_quality_views'](mapping)|N['earthquake_views'](mapping)|N['copy_view_queries'](cfg,'dim_location')).values():
            self.assertNotIn('weather_api_test.gold.',q)
        for schema in ('silver','reference','pipeline_state','gold_verify_x','gold_verify_0123456789AB','gold_verify_0123456789ab.extra'):
            with self.subTest(schema=schema),self.assertRaises(ValidationError):GoldConfig(catalog='weather_api_test', schema=schema).validate()

    def test_recovery_requires_evidence_before_any_database_action(self):
        from unittest.mock import Mock
        ctx={'db':Mock(),'cfg':GoldConfig(catalog='weather_api_test', schema='gold_verify_0123456789ab')}
        for kwargs in ({},{'confirmed_terminated':True},{'confirmed_terminated':False,'termination_evidence':'age exceeded a timeout'}):
            with self.assertRaises(ValidationError):N['recover_writer'](ctx,expected_token='old',**kwargs)
        ctx['db'].query.assert_not_called()

    def test_legacy_active_is_readable_but_not_claimed_healthy_history(self):
        import copy
        from src.validation.gold import validate_control_rows
        p,r,s=self.controls_fixture();r[0].update(publication_state='LEGACY_ACTIVE',health='UNKNOWN',verified_at=None)
        validate_control_rows(p,r,s,contract='contract')
        q=N['copy_view_queries'](GoldConfig(catalog='weather_api_test'),'dim_location')
        self.assertIn("publication_state='LEGACY_ACTIVE'",q['dim_location'])
        self.assertNotIn("publication_state='LEGACY_ACTIVE'",q['dim_location_history'])

    def test_migration_plan_scope_digest_and_transition_checks(self):
        import hashlib,copy
        from src.validation.gold import validate_migration_plan,validate_migration_transition
        cfg=GoldConfig(catalog='weather_api_test', schema='gold_verify_0123456789ab')
        plan={'target':cfg.catalog+'.'+cfg.schema,'contract':'test','baseline':{},'sql':['UPDATE '+cfg.table('_gold_active')+' SET generation=1']}
        plan['migration_id']=hashlib.sha256(json.dumps(plan,sort_keys=True).encode()).hexdigest()
        validate_migration_plan(plan,cfg)
        bad=copy.deepcopy(plan);bad['sql'][0]+=' WHERE false'
        with self.assertRaises(ValidationError):validate_migration_plan(bad,cfg)
        for target in ('weather_api_test.gold._gold_active','weather_api_test.silver.weather_current'):
            bad=copy.deepcopy(plan);bad['sql']=['UPDATE '+target+' SET generation=1']
            bad['migration_id']=hashlib.sha256(json.dumps({k:bad[k] for k in ('target','contract','baseline','sql')},sort_keys=True).encode()).hexdigest()
            with self.assertRaises(ValidationError):validate_migration_plan(bad,cfg)
        before={'versions':{'_gold_active':0,'other':2},'objects':{'_gold_active':'old','other':'same'}}
        after={'versions':{'_gold_active':1,'other':2},'objects':{'_gold_active':'new','other':'same'}}
        validate_migration_transition(before,after,plan['sql'][0])
        after['versions']['other']=3
        with self.assertRaises(ValidationError):validate_migration_transition(before,after,plan['sql'][0])
        after['versions']['other']=2;after['objects']['other']='foreign change'
        with self.assertRaises(ValidationError):validate_migration_transition(before,after,plan['sql'][0])


    def test_migration_journal_uses_byte_safe_json_and_qualified_restore(self):
        payload={'sql':["SELECT '\\n', 'quoted'"],'text':'line1\nline2'}
        expr=N['migration_json_sql'](payload)
        encoded=re.search(r"unhex\('([0-9a-f]+)'\)",expr)[1]
        self.assertEqual(json.loads(bytes.fromhex(encoded).decode()),payload)
        self.assertTrue(expr.startswith('decode(unhex('))
        book=json.loads((Path(__file__).resolve().parents[1]/'notebooks/gold/weather_gold.ipynb').read_text())
        definitions=''.join(''.join(c.get('source',[])) for c in book['cells'] if c.get('metadata',{}).get('tags')==['gold_definitions'])
        self.assertIn('INSERT INTO {journal} SELECT 1,',definitions)
        self.assertNotIn('INSERT INTO {journal} VALUES (1,',definitions)

class PointerDB:
    """Atomic conditional-update semantics, not a mock Spark engine."""
    def __init__(self,active):self.active=active;self.generation=0
    def query(self,q):
        if q.startswith('SELECT'):return [{'release_id':self.active,'generation':self.generation,'writer_token':'writer'}]
        target,condition=re.findall("release_id='([^']*)'",q)
        generation=int(re.search(r'AND generation=(\d+)',q).group(1))
        if self.active==condition and self.generation==generation:self.active=target;self.generation+=1
        return []


class ApprovedCatalogAndSelectionTests(unittest.TestCase):
    """Approved production/test Gold catalogs, notebook catalog propagation, and strict selection completeness."""

    def setUp(self):
        self.root=Path(__file__).resolve().parents[1]

    def test_approved_catalogs_only(self):
        from src.validation.gold import APPROVED_GOLD_CATALOGS
        self.assertEqual(set(APPROVED_GOLD_CATALOGS),{'weather_api','weather_api_test'})
        self.assertEqual(GoldConfig(catalog='weather_api').validate().schema,'gold')
        self.assertEqual(GoldConfig(catalog='weather_api_test').validate().schema,'gold')

    def test_production_disposable_schema_rejected_and_blank_fails(self):
        with self.assertRaises(ValidationError):GoldConfig(catalog='weather_api',schema='gold_verify_0123456789ab').validate()
        with self.assertRaises(ValidationError):GoldConfig(catalog='').validate()
        for catalog in ('weather_api','weather_api_test'):
            with self.assertRaises(ValidationError):GoldConfig(catalog=catalog,schema='silver').validate()

    def test_all_three_notebooks_propagate_catalog(self):
        for name in ('weather_gold','air_quality_gold','earthquakes_gold'):
            nb=json.loads((self.root/f'notebooks/gold/{name}.ipynb').read_text())
            sources=''.join(''.join(c.get('source',[])) if isinstance(c.get('source'),list) else c.get('source','') for c in nb['cells'] if c.get('cell_type')=='code')
            self.assertTrue(("dbutils.widgets.get('catalog')" in sources) or ('dbutils.widgets.get("catalog")' in sources),msg=name)
            self.assertTrue(("dbutils.widgets.text('catalog','')" in sources) or ('dbutils.widgets.text("catalog", "")' in sources),msg=name)

    def test_selection_completeness_rejects_subsets_and_extras(self):
        from src.validation.location_registry import validate_selection_completeness
        validate_selection_completeness([1,2,3],[1,2,3])
        for selected,enabled in ([[1,2],[1,2,3]],[[1,2,3,4],[1,2,3]],[[1,1,2],[1,2]],[[],[1]],[[1,2],[]]):
            with self.subTest(selected=selected,enabled=enabled):
                with self.assertRaises(ValidationError):validate_selection_completeness(selected,enabled)

    def test_country_cells_reference_and_attribution_rules(self):
        # Reference resolution: exact table name, fail-closed on missing/invalid columns.
        from src.validation.gold import country_cells_relation
        ok=[{'column_name':c,'data_type':t} for c,t in (('cell','BIGINT'),('country_code','STRING'),('country_name','STRING'))]
        ctx={'db':SimpleNamespace(query=lambda sql:ok),'silver':SimpleNamespace(catalog='weather_api',reference_schema='reference')}
        self.assertEqual(country_cells_relation(ctx),'weather_api.reference.country_h3_cells')
        for broken in ([{'column_name':'cell','data_type':'INT'}],[],
                       [{'column_name':c,'data_type':t} for c,t in (('cell','BIGINT'),('country_code','BIGINT'),('country_name','STRING'))]):
            with self.subTest(broken=broken):
                with self.assertRaises(ValidationError):
                    country_cells_relation({'db':SimpleNamespace(query=lambda sql:broken),'silver':SimpleNamespace(catalog='weather_api',reference_schema='reference')})
        # Seismic SQL: country columns are always emitted, and an optional country
        # relation is joined on H3 cells when provided.
        seismic_sql=N['seismic']('rel')
        self.assertIn("'UNASSIGNED' country_code",seismic_sql)
        joined=N['seismic']('rel','weather_api.reference.country_h3_cells')
        self.assertIn('LEFT JOIN weather_api.reference.country_h3_cells c',joined)
        self.assertIn('h3_longlatash3(s.longitude,s.latitude,6)',joined)
        self.assertIn("coalesce(c.country_code,'UNASSIGNED') country_code",joined)
        # The earthquakes notebook wires the reference table and attribution check.
        nb=json.loads((self.root/'notebooks/gold/earthquakes_gold.ipynb').read_text())
        sources=''.join(''.join(c.get('source',[])) if isinstance(c.get('source'),list) else c.get('source','') for c in nb['cells'] if c.get('cell_type')=='code')
        self.assertIn('checks.country_cells_relation(ctx)',sources)
        self.assertIn("checks.validate_country_attribution(ctx['db'],ctx['stages']['seismic_events_latest'])",sources)
