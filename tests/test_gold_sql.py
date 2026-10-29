"""Explicit SQL-engine regressions; persistent isolated tests require a separate opt-in.

Run manually with --connection-settings, or run_suite(SQLExecutor(existing_spark)).
Persistent opt-in cases exercise Delta helpers; none proves remote PySpark/widgets/jobs.
"""
import argparse
import json
import unittest
import uuid
from datetime import datetime
from tests.test_gold import N,GoldConfig,connection_settings
SQLExecutor=N['SQLExecutor']
aqi_case=N['aqi_case']
distance_sql=N['distance_sql']
literal=N['literal']
EARTH_RADIUS_KM=N['EARTH_RADIUS_KM']
latest=N['latest']
seismic=N['seismic']
audit=N['audit']

def measurement(relation,registry,source,kind):
    return N['weather_query'](relation,registry,kind) if source=='weather' else N['air_quality_query'](relation,registry,kind)

def derived_views(config):
    names=('dim_location','weather_current_latest','weather_hourly_latest','weather_daily_latest',
      'air_quality_current_latest','air_quality_hourly_latest','weather_aq_coverage',
      'seismic_events_latest','seismic_coverage')
    mapping={name:config.table(name) for name in names}
    mapping['_gold_settings']=config.table('_gold_releases')
    return N['air_quality_views'](mapping)|N['earthquake_views'](mapping)

from src.contracts import output_types
from src.validation.gold import check_relation, reconcile
from src.validation.common import ValidationError

class GoldSQLTests(unittest.TestCase):
    db=None
    @classmethod
    def setUpClass(cls):
        if cls.db is None:raise unittest.SkipTest('Explicit SQL session required; no automatic live connection')
    def setUp(self):
        self.db.query("SET TIME ZONE 'UTC'")
        self.prefix='_gold_test_'+uuid.uuid4().hex[:10]+'_';self.created=[]
    def tearDown(self):
        for v in reversed(self.created):self.db.query('DROP VIEW IF EXISTS '+v)
    def view(self,name,q):
        v=self.prefix+name;self.db.query('CREATE OR REPLACE TEMP VIEW '+v+' AS '+q);self.created.append(v);return v
    def frame(self,name,source,kind,rows):
        fields=output_types(source,kind)
        selects=[]
        for i,row in enumerate(rows):
            defaults={'source_file_path':f'file{i}','source':source,'ingested_at':'2026-01-01T00:00:00Z','snapshot_at':'2026-01-01T00:00:00Z',
                'latitude':0.,'longitude':0.,'location_name':'test','event_type':'earthquake','magnitude_type':'ml',
                'event_time':'2026-01-01T00:00:00Z','updated_at':'2026-01-01T00:00:00Z','updated_at_ms':1767225600000,'event_time_ms':1767225600000}
            defaults.update(row)
            selects.append('SELECT '+','.join(f'cast({"NULL" if defaults.get(n) is None else literal(defaults[n])} AS {typ}) AS `{n}`' for n,typ in fields.items()))
        return self.view(name,' UNION ALL '.join(selects))
    def relation(self,name,q):return self.view(name,"SELECT 'release' _release_id,cast('2026-01-01T00:00:00Z' AS TIMESTAMP) _built_at,s.* FROM ("+q+') s')
    def mapping(self,stages):
        pointer=self.view('pointer',"SELECT 'release' release_id")
        releases=self.view('releases',"SELECT 'release' release_id,"+literal(GoldConfig(catalog='weather_api_test').json())+" settings,current_timestamp() published_at")
        class Config:
            def table(_,name):return stages.get(name,{'_gold_active':pointer,'_gold_releases':releases}.get(name,name))
        return Config()
    def test_latest_full_timestamp_alignment_missing_sources_and_history(self):
        w=self.frame('w','weather','hourly',[
            {'location_id':1,'forecast_for':'2026-10-31T00:00:00Z','snapshot_at':'2026-10-30T00:00:00Z','temperature_2m':1},
            {'location_id':1,'forecast_for':'2026-10-31T00:00:00Z','snapshot_at':'2026-10-31T00:01:00Z','temperature_2m':2},
            {'location_id':1,'forecast_for':'2026-11-01T00:00:00Z','temperature_2m':3},
            {'location_id':2,'forecast_for':'2026-11-01T00:00:00Z','temperature_2m':4}])
        a=self.frame('a','air_quality','hourly',[
            {'location_id':1,'forecast_for':'2026-10-31T00:00:00Z','snapshot_at':'2026-10-31T00:01:05Z','pm2_5':10},
            {'location_id':3,'forecast_for':'2026-11-01T00:00:00Z','pm2_5':20}])
        registry=self.view('registry',"SELECT * FROM VALUES (1,'America/New_York'),(2,'UTC'),(3,'UTC') r(location_id,timezone)")
        stages={'weather_hourly_latest':self.relation('wh',measurement(w,registry,'weather','hourly')),
                'air_quality_hourly_latest':self.relation('ah',measurement(a,registry,'air_quality','hourly'))}
        env=self.view('env',derived_views(self.mapping(stages))['environment_hourly'])
        self.assertEqual(check_relation(self.db,env,['location_id','forecast_for']),4)
        rows=self.db.query('SELECT * FROM '+env+' WHERE location_id=1 ORDER BY forecast_for')
        self.assertEqual([r['weather_temperature_2m'] for r in rows],[2.,3.])
        self.assertEqual(rows[0]['aq_minus_weather_retrieval_seconds'],5.)
        self.assertEqual(rows[0]['alignment_quality'],'within_threshold')
        self.assertFalse(rows[1]['air_quality_available'])
        self.assertEqual(self.db.query('SELECT count(*) n FROM '+env+" WHERE alignment_quality='single_source'")[0]['n'],3)
    def test_latest_current_out_of_order_replay_and_conflicting_keys(self):
        rel=self.view('versions',"SELECT * FROM VALUES (1,timestamp'2026-01-02 00:00:00','b',20),(1,timestamp'2026-01-01 00:00:00','a',10),(2,timestamp'2026-01-01 00:00:00','c',30) s(location_id,snapshot_at,source_file_path,value)")
        q=latest(rel,['location_id']);rows=self.db.query(q+' ORDER BY location_id')
        self.assertEqual([r['value'] for r in rows],[20,30])
        dup=self.view('dup','SELECT * FROM '+rel+' UNION ALL SELECT * FROM '+rel+' WHERE location_id=2')
        with self.assertRaises(ValidationError):check_relation(self.db,dup,['location_id','snapshot_at'])
        conflict=self.view('conflict',"SELECT * FROM VALUES (1,timestamp'2026-01-01 00:00:00',1),(1,timestamp'2026-01-01 00:00:00',2) s(location_id,snapshot_at,value)")
        with self.assertRaises(ValidationError):check_relation(self.db,conflict,['location_id','snapshot_at'])
    def test_aqi_boundaries_nulls_and_invalid(self):
        values=[None,-1,0,20,21,40,41,50,51,60,61,80,81,100,101,150,151,200,201,300,301,500,501]
        r=self.view('aqivalues',' UNION ALL '.join('SELECT cast('+('NULL' if v is None else str(v))+f' AS DOUBLE) v,{i} seq' for i,v in enumerate(values)))
        rows=self.db.query(f'SELECT v,{aqi_case("v","us")} us,{aqi_case("v","eu")} eu FROM {r} ORDER BY seq')
        self.assertEqual(rows[0]['us'],'Unknown');self.assertEqual(rows[1]['eu'],'Invalid')
        expected_us={50:'Good',51:'Moderate',100:'Moderate',101:'Unhealthy for sensitive groups',150:'Unhealthy for sensitive groups',151:'Unhealthy',200:'Unhealthy',201:'Very unhealthy',300:'Very unhealthy',301:'Hazardous',501:'Hazardous'}
        expected_eu={20:'Good',21:'Fair',40:'Fair',41:'Moderate',60:'Moderate',61:'Poor',80:'Poor',81:'Very poor',100:'Very poor',101:'Extremely poor'}
        for row in rows:
            if row['v'] in expected_us:self.assertEqual(row['us'],expected_us[row['v']])
            if row['v'] in expected_eu:self.assertEqual(row['eu'],expected_eu[row['v']])
    def test_utc_local_date_and_both_dst_transitions(self):
        q="""SELECT instant,from_utc_timestamp(instant,'America/New_York') local FROM VALUES
          (timestamp'2026-11-01 05:30:00'),(timestamp'2026-11-01 06:30:00'),
          (timestamp'2026-03-08 06:30:00'),(timestamp'2026-03-08 07:30:00'),
          (timestamp'2026-10-23 00:00:00') s(instant) ORDER BY instant"""
        rows=self.db.query(q)
        self.assertEqual([r['local'].hour for r in rows[:2]],[1,3])
        self.assertEqual(rows[2]['local'].date().isoformat(),'2026-10-22')
        self.assertEqual([r['local'].hour for r in rows[-2:]],[1,1])
        self.assertNotEqual(rows[-2]['instant'],rows[-1]['instant'])
    def test_earthquake_unchanged_source_update_and_substantive_revisions(self):
        hist=self.frame('history','earthquakes','history',[
            {'event_id':'e','snapshot_at':'2026-01-01T00:00:00Z','magnitude':1},
            {'event_id':'e','snapshot_at':'2026-01-02T00:00:00Z','magnitude':1},
            {'event_id':'e','snapshot_at':'2026-01-03T00:00:00Z','magnitude':1,'updated_at_ms':1767312000000,'updated_at':'2026-01-02T00:00:00Z'},
            {'event_id':'e','snapshot_at':'2026-01-04T00:00:00Z','magnitude':3,'latitude':5,'longitude':5,'event_type':'explosion','event_time':'2026-01-02T00:00:00Z','event_time_ms':1767312000000,'updated_at_ms':1767398400000,'updated_at':'2026-01-03T00:00:00Z'},
            {'event_id':'blast','event_type':'quarry blast','magnitude':-1,'depth_km':-1}])
        audited=self.view('audited',audit(hist,list(output_types('earthquakes','history'))))
        rows=self.db.query("SELECT * FROM "+audited+" WHERE event_id='e' ORDER BY snapshot_at")
        self.assertIsNone(rows[0]['unchanged_repeat']);self.assertTrue(rows[1]['unchanged_repeat'])
        self.assertTrue(rows[2]['source_version_changed']);self.assertFalse(rows[2]['substantive_attributes_changed'])
        self.assertTrue(rows[3]['substantive_attributes_changed']);self.assertTrue(rows[3]['measurement_or_geometry_changed'])
        events=self.relation('events',seismic(hist));d=derived_views(self.mapping({'seismic_events_latest':events}))
        summary=self.view('summary',d['seismic_daily_summary'])
        rows=self.db.query('SELECT * FROM '+summary+" WHERE event_type='explosion'")
        self.assertEqual(len(rows),1);self.assertEqual(str(rows[0]['event_date_utc']),'2026-01-02')
        self.assertEqual(rows[0]['max_magnitude'],3.)
        self.assertEqual(self.db.query('SELECT count(*) n FROM '+summary+" WHERE event_type='earthquake'")[0]['n'],0)
        grid=self.view('moved_grid',d['seismic_grid_daily'])
        moved=self.db.query('SELECT * FROM '+grid+" WHERE event_type='explosion'")[0]
        self.assertEqual((moved['latitude_min'],moved['longitude_min']),(4.,4.))
        self.assertEqual(self.db.query('SELECT count(*) n FROM '+events+" WHERE event_type='quarry blast' AND magnitude<0 AND depth_km<0")[0]['n'],1)
    def test_nearby_candidate_filter_equals_all_pairs_including_dateline_and_poles(self):
        d=self.view('locations',"SELECT 'release' _release_id,* FROM VALUES (1,0D,179.5D),(2,89D,0D),(3,90D,0D),(4,-89D,45D) s(location_id,latitude,longitude)")
        hist=self.frame('seismic','earthquakes','history',[
            {'event_id':'date','latitude':0.,'longitude':-179.5},
            {'event_id':'north','latitude':89.,'longitude':180.},
            {'event_id':'south','latitude':-89.,'longitude':-135.},
            {'event_id':'far','latitude':0.,'longitude':0.}])
        events=self.relation('events',seismic(hist));q=derived_views(self.mapping({'dim_location':d,'seismic_events_latest':events}))['location_seismic_nearby']
        bridge=self.view('bridge',q);self.assertGreater(check_relation(self.db,bridge,['location_id','event_id']),0)
        dist=distance_sql('d.latitude','d.longitude','e.latitude','e.longitude')
        expected=self.view('allpairs',f'SELECT location_id,event_id FROM {d} d CROSS JOIN {events} e WHERE {dist}<=250')
        actual=self.view('bridgekeys','SELECT location_id,event_id FROM '+bridge)
        reconcile(self.db,expected,actual)
        n=self.db.query(f'SELECT {distance_sql("0D","179.5D","0D","-179.5D")} d')[0]['d']
        self.assertAlmostEqual(n,111.1950802335,places=6)
        polar=self.db.query(f'SELECT {distance_sql("89D","0D","89D","180D")} d')[0]['d']
        self.assertAlmostEqual(polar,222.3901604671,places=6)
    def test_grid_dateline_poles_and_empty_events(self):
        hist=self.frame('edges','earthquakes','history',[
            {'event_id':'east','latitude':90.,'longitude':180.},
            {'event_id':'west','latitude':-90.,'longitude':-180.}])
        events=self.relation('events',seismic(hist));q=derived_views(self.mapping({'seismic_events_latest':events}))['seismic_grid_daily']
        rows=self.db.query(q)
        self.assertEqual(sum(r['event_count'] for r in rows),2)
        self.assertTrue(all(r['longitude_min']==-180 for r in rows))
        self.assertTrue(all(-90<=r['latitude_min']<r['latitude_max']<=90 for r in rows))
        empty=self.view('empty','SELECT * FROM '+events+' WHERE false')
        summary=derived_views(self.mapping({'seismic_events_latest':empty}))['seismic_daily_summary']
        self.assertEqual(self.db.query(summary),[])

    def test_radiation_uses_known_interval_and_preserves_units(self):
        expression=N['radiation_energy_mj']('1000D','3600D')
        self.assertEqual(self.db.query('SELECT '+expression+' energy')[0]['energy'],3.6)
        self.assertIsNone(self.db.query('SELECT '+N['radiation_energy_mj']('1000D','cast(NULL AS DOUBLE)')+' energy')[0]['energy'])
        raw=self.frame('solar','weather','hourly',[
            {'location_id':1,'forecast_for':'2026-01-01T00:00:00Z','shortwave_radiation':1000},
            {'location_id':1,'forecast_for':'2026-01-01T01:00:00Z','shortwave_radiation':1000},
            {'location_id':1,'forecast_for':'2026-01-01T03:00:00Z','shortwave_radiation':1000}])
        registry=self.view('r',"SELECT 1 location_id,'UTC' timezone")
        rows=self.db.query(N['weather_query'](raw,registry,'hourly')+' ORDER BY forecast_for')
        self.assertEqual([r['shortwave_energy_known_interval_mj_m2'] for r in rows],[None,3.6,None])
        self.assertEqual([r['shortwave_radiation'] for r in rows],[1000.,1000.,1000.])

    def test_location_current_preserves_disabled_missing_sources_and_nulls(self):
        registry=self.relation('dimension',"SELECT * FROM VALUES (1,'UTC',true),(2,'UTC',false),(3,'UTC',true) d(location_id,timezone,is_active)")
        weather=self.frame('wc','weather','current',[{'location_id':1,'observed_at':'2026-01-01T00:00:00Z','temperature_2m':None}])
        aq=self.frame('ac','air_quality','current',[{'location_id':3,'observed_at':'2026-01-01T00:00:00Z','us_aqi':None}])
        stages={'dim_location':registry,
            'weather_current_latest':self.relation('wcurrent',measurement(weather,registry,'weather','current')),
            'air_quality_current_latest':self.relation('acurrent',measurement(aq,registry,'air_quality','current'))}
        q=derived_views(self.mapping(stages))['location_current']
        rows=self.db.query(q+' ORDER BY location_id')
        self.assertEqual(len(rows),3)
        self.assertFalse(rows[1]['is_active']);self.assertFalse(rows[1]['weather_available']);self.assertFalse(rows[1]['air_quality_available'])
        self.assertTrue(rows[0]['weather_available']);self.assertIsNone(rows[0]['weather_temperature_2m'])
        self.assertEqual(rows[2]['us_aqi_category'],'Unknown')

    def test_join_multiplication_is_blocking(self):
        from src.validation.gold import validate_derived
        one=self.view('keys',"SELECT 1 location_id,timestamp'2026-01-01 00:00:00' forecast_for")
        multiplied=self.view('multiplied','SELECT * FROM '+one+' UNION ALL SELECT * FROM '+one)
        with self.assertRaises(ValidationError):
            validate_derived(self.db,{'weather_hourly_latest':one,'air_quality_hourly_latest':one},{'environment_hourly':multiplied})

    def test_eligible_history_retains_older_version_when_latest_is_incomplete(self):
        hist=self.frame('versions','earthquakes','history',[
            {'event_id':'e','snapshot_at':'2026-01-01T00:00:00Z','source_file_path':'eligible','magnitude':1},
            {'event_id':'e','snapshot_at':'2026-01-02T00:00:00Z','source_file_path':'incomplete','magnitude':9,'updated_at':'2026-01-02T00:00:00Z'}])
        eligible=self.view('eligible',"SELECT * FROM "+hist+" WHERE source_file_path='eligible'")
        rows=self.db.query(seismic(eligible))
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['magnitude'],1.)

    def test_alignment_threshold_is_configurable_and_upper_inclusive(self):
        w=self.frame('w','weather','hourly',[
            {'location_id':1,'forecast_for':'2026-01-01T00:00:00Z'},
            {'location_id':2,'forecast_for':'2026-01-01T00:00:00Z'}])
        a=self.frame('a','air_quality','hourly',[
            {'location_id':1,'forecast_for':'2026-01-01T00:00:00Z','snapshot_at':'2026-01-01T00:15:00Z'},
            {'location_id':2,'forecast_for':'2026-01-01T00:00:00Z','snapshot_at':'2026-01-01T00:15:01Z'}])
        registry=self.view('r',"SELECT * FROM VALUES (1,'UTC'),(2,'UTC') r(location_id,timezone)")
        stages={'weather_hourly_latest':self.relation('wh',measurement(w,registry,'weather','hourly')),
                'air_quality_hourly_latest':self.relation('ah',measurement(a,registry,'air_quality','hourly'))}
        q=derived_views(self.mapping(stages))['environment_hourly']
        rows=self.db.query(q+' ORDER BY location_id')
        self.assertEqual([r['alignment_quality'] for r in rows],['within_threshold','retrievals_apart'])

    def test_decimal_mean_avoids_floating_reduction_drift(self):
        history=self.frame('depthmeans','earthquakes','history',[
            {'event_id':'a','depth_km':0.1},{'event_id':'b','depth_km':0.2},{'event_id':'c','depth_km':0.3}])
        events=self.relation('events',seismic(history))
        q=derived_views(self.mapping({'seismic_events_latest':events}))['seismic_daily_summary']
        self.assertEqual(self.db.query(q)[0]['mean_depth_km'],0.2)

    def test_seismic_source_version_precedes_retrieval_and_ties_use_latest_snapshot(self):
        hist=self.frame('order_versions','earthquakes','history',[
            {'event_id':'e','magnitude':2,'snapshot_at':'2026-01-02T00:00:00Z','updated_at':'2026-01-02T00:00:00Z','updated_at_ms':1767312000000},
            {'event_id':'e','magnitude':1,'snapshot_at':'2026-01-03T00:00:00Z','updated_at':'2026-01-01T00:00:00Z','updated_at_ms':1767225600000}])
        self.assertEqual(self.db.query(seismic(hist))[0]['magnitude'],2.)
        later=self.frame('same_source_version','earthquakes','history',[
            {'event_id':'e','magnitude':3,'snapshot_at':'2026-01-04T00:00:00Z','updated_at':'2026-01-02T00:00:00Z','updated_at_ms':1767312000000}])
        combined=self.view('combined_versions','SELECT * FROM '+hist+' UNION ALL SELECT * FROM '+later)
        self.assertEqual(self.db.query(seismic(combined))[0]['magnitude'],3.)

    def test_null_safe_category_validation_rejects_corrupt_labels(self):
        from src.validation.gold import validate_aqi_categories
        # Independent expected boundary labels, not generated by aqi_case.
        pairs=[(None,'Unknown',None,'Unknown'),(0,'Good',0,'Good'),(50,'Good',20,'Good'),
          (51,'Moderate',21,'Fair'),(100,'Moderate',40,'Fair'),(101,'Unhealthy for sensitive groups',41,'Moderate'),
          (150,'Unhealthy for sensitive groups',60,'Moderate'),(151,'Unhealthy',61,'Poor'),(200,'Unhealthy',80,'Poor'),
          (201,'Very unhealthy',81,'Very poor'),(300,'Very unhealthy',100,'Very poor'),(301,'Hazardous',101,'Extremely poor')]
        rows=['SELECT '+','.join(('cast(NULL AS INT)' if value is None else str(value)) if i%2==0 else literal(value) for i,value in enumerate(row)) for row in pairs]
        relation=self.view('valid_categories','SELECT * FROM ('+' UNION ALL '.join(rows)+') v(us_aqi,us_aqi_category,european_aqi,european_aqi_category)')
        validate_aqi_categories(self.db,relation)
        for i,(u,uc,e,ec) in enumerate([(None,'Good',None,'Unknown'),(None,None,None,'Unknown'),(50,None,20,'Good'),(50,'Bad',20,'Good'),(50,'Good',None,'Good'),(50,'Good',20,None),(50,'Good',20,'Unexpected'),(-1,'Good',20,'Good')]):
            expressions=[('cast(NULL AS INT)' if value is None else str(value)) if j%2==0 else ('cast(NULL AS STRING)' if value is None else literal(value)) for j,value in enumerate((u,uc,e,ec))]
            bad=self.view('bad_category'+str(i),'SELECT * FROM (SELECT '+','.join(expressions)+') v(us_aqi,us_aqi_category,european_aqi,european_aqi_category)')
            with self.subTest(case=i),self.assertRaises(ValidationError):validate_aqi_categories(self.db,bad)

    def test_retained_public_reads_exclude_failed_incomplete_and_unhealthy(self):
        releases=self.view('history_receipts',"SELECT * FROM VALUES ('a','PUBLISHED','HEALTHY'),('b','PUBLISHED','HEALTHY'),('f','FAILED','HEALTHY'),('i','CANDIDATE','UNKNOWN'),('u','PUBLISHED','UNHEALTHY'),('x','WITHDRAWN','HEALTHY') r(release_id,publication_state,health)")
        stages=self.view('history_stages',"SELECT r.release_id,s.stage_name,'VALIDATED' status FROM "+releases+" r CROSS JOIN VALUES ('weather'),('air_quality'),('earthquakes') s(stage_name) WHERE r.release_id<>'i'")
        pointer=self.view('history_active',"SELECT 1 singleton,'b' release_id")
        data=self.view('history_data',"SELECT * FROM VALUES ('a',1,20),('b',1,30),('f',1,99),('i',1,99),('u',1,99),('x',1,99) d(_release_id,location_id,temperature_2m)")
        names={'_gold_releases':releases,'_gold_stages':stages,'_gold_active':pointer,'_data_weather_current_latest':data}
        class Config:
            def table(_,name):return names[name]
        queries=N['copy_view_queries'](Config(),'weather_current_latest')
        history=self.view('public_history',queries['weather_current_latest_history'])
        self.assertEqual(self.db.query('SELECT _release_id,temperature_2m FROM '+history+' ORDER BY _release_id'),[{'_release_id':'a','temperature_2m':20},{'_release_id':'b','temperature_2m':30}])
        self.assertEqual(self.db.query('SELECT temperature_2m FROM ('+queries['weather_current_latest']+')')[0]['temperature_2m'],30)
        self.assertEqual(self.db.query('SELECT temperature_2m FROM '+history+" WHERE _release_id='a'")[0]['temperature_2m'],20)

    def test_historical_environment_join_is_release_qualified(self):
        raww=self.frame('history_w','weather','hourly',[{'location_id':1,'forecast_for':'2026-01-01T00:00:00Z','temperature_2m':20}])
        rawa=self.frame('history_a','air_quality','hourly',[{'location_id':1,'forecast_for':'2026-01-01T00:00:00Z','us_aqi':50}])
        registry=self.view('history_registry',"SELECT 1 location_id,'UTC' timezone")
        def two(name,query):return self.view(name,"SELECT 'a' _release_id,s.* FROM ("+query+") s UNION ALL SELECT 'b' _release_id,s.* FROM ("+query+') s')
        w=two('history_weather',measurement(raww,registry,'weather','hourly'));a=two('history_aq',measurement(rawa,registry,'air_quality','hourly'))
        settings=self.view('history_settings',"SELECT release_id,"+literal(GoldConfig(catalog='weather_api_test').json())+" settings FROM VALUES ('a'),('b') r(release_id)")
        mapping=dict.fromkeys(('dim_location','weather_current_latest','air_quality_current_latest','weather_aq_coverage'),'unused')|{'weather_hourly_latest':w,'air_quality_hourly_latest':a,'_gold_settings':settings}
        rows=self.db.query(N['air_quality_views'](mapping)['environment_hourly']+' ORDER BY _release_id')
        self.assertEqual(len(rows),2);self.assertEqual([r['_release_id'] for r in rows],['a','b'])
        self.assertTrue(all(r['weather_temperature_2m']==20 and r['air_quality_us_aqi']==50 for r in rows))

    def test_unordered_source_arrays_do_not_create_false_revisions(self):
        raw=self.frame('set_history','earthquakes','history',[{'event_id':'e'}])
        base='SELECT * EXCEPT(all_event_ids,all_sources,available_product_types,snapshot_at),'
        hist=self.view('set_versions',base+"array('b','a','a') all_event_ids,array('x','y') all_sources,array('origin','phase-data') available_product_types,timestamp'2026-01-01 00:00:00' snapshot_at FROM "+raw+' UNION ALL '+base+"array('a','b') all_event_ids,array('y','x') all_sources,array('phase-data','origin') available_product_types,timestamp'2026-01-02 00:00:00' snapshot_at FROM "+raw)
        rows=self.db.query(audit(hist,list(output_types('earthquakes','history')))+' ORDER BY snapshot_at')
        self.assertTrue(rows[1]['unchanged_repeat']);self.assertFalse(rows[1]['substantive_attributes_changed'])
        self.assertEqual(list(rows[0]['all_event_ids']),['b','a','a']) # Output remains faithful; normalization is hash-only.

    def test_known_raw_boundary_values_survive_gold(self):
        # Independent raw/Silver boundary fixture, not a proof of upstream PySpark execution.
        raw=json.loads('{"location_id":42,"weather":{"temperature_2m":12.75},"aq":{"pm2_5":8.25},"earthquake":{"magnitude":-0.5,"latitude":12.25,"longitude":179.75,"updated_ms":1767312000123}}')
        registry=self.view('known_registry',"SELECT 42 location_id,'UTC' timezone")
        w=self.frame('known_weather','weather','current',[{'location_id':raw['location_id'],'temperature_2m':raw['weather']['temperature_2m'],'observed_at':'2026-01-01T00:00:00Z'}])
        a=self.frame('known_aq','air_quality','current',[{'location_id':42,'pm2_5':raw['aq']['pm2_5'],'observed_at':'2026-01-01T00:00:00Z'}])
        wr=self.db.query(measurement(w,registry,'weather','current'))[0];ar=self.db.query(measurement(a,registry,'air_quality','current'))[0]
        # Session TZ is UTC; SQL-driver sessions return tz-aware datetimes while
        # Spark Connect returns naive ones, so compare the UTC-normalized form.
        observed=(wr['observed_at'].isoformat() or '').replace('+00:00','')
        self.assertEqual((wr['location_id'],wr['temperature_2m'],observed),(42,12.75,'2026-01-01T00:00:00'))
        self.assertEqual(ar['pm2_5'],8.25)
        e=self.frame('known_event','earthquakes','history',[{'event_id':'known','magnitude':raw['earthquake']['magnitude'],'latitude':12.25,'longitude':179.75,'updated_at':'2026-01-02T00:00:00.123Z','updated_at_ms':1767312000123}])
        event=self.db.query(seismic(e))[0]
        self.assertEqual((event['magnitude'],event['latitude'],event['longitude'],event['updated_at_ms']),(-.5,12.25,179.75,1767312000123))
        self.assertEqual(event['updated_at'].microsecond,123000)

    def test_country_attribution_land_and_unassigned(self):
        # Attribution joins precomputed Natural Earth-derived H3 land cells; offshore or
        # unmatched events carry explicit UNASSIGNED values, never NULL.
        cells=self.view('country_cells',f"SELECT {604222352263217151} cell,'USA' country_code,'United States' country_name")
        land=self.frame('attr_land','earthquakes','history',[{'event_id':'attr_land','latitude':40.7128,'longitude':-74.0060}])
        sea=self.frame('attr_sea','earthquakes','history',[{'event_id':'attr_sea','latitude':0.0,'longitude':-160.0}])
        both=self.view('attr_both',f'SELECT * FROM {land} UNION ALL SELECT * FROM {sea}')
        rows={r['event_id']:r for r in self.db.query(seismic(both,cells))}
        self.assertEqual((rows['attr_land']['country_code'],rows['attr_land']['country_name']),('USA','United States'))
        self.assertEqual((rows['attr_sea']['country_code'],rows['attr_sea']['country_name']),('UNASSIGNED','Unassigned (offshore or outside land boundaries)'))
        # Without a country relation the attribution remains explicit, never NULL.
        nodb={r['event_id']:r for r in self.db.query(seismic(both))}
        self.assertEqual(nodb['attr_land']['country_code'],'UNASSIGNED')
        self.assertEqual(nodb['attr_sea']['country_name'],'Unassigned (offshore or outside land boundaries)')
        from src.validation.gold import validate_country_attribution
        attributed=self.view('attr_rows',f'SELECT event_id,country_code,country_name FROM ({seismic(both,cells)})')
        self.assertEqual(validate_country_attribution(self.db,attributed),{'total':2,'assigned':1,'unassigned':1})

    def test_publication_status_exposes_unhealthy_and_invalid_controls(self):
        releases=self.view('status_releases',"SELECT 'a' release_id,current_timestamp() built_at,cast(NULL AS TIMESTAMP) published_at,'{}' input_versions,'[]' eligible_paths,'PUBLISHED' publication_state,'UNHEALTHY' health")
        stages=self.view('status_stages',"SELECT 'a' release_id,* FROM VALUES ('weather','VALIDATED'),('air_quality','VALIDATED'),('earthquakes','FAILED') s(stage_name,status)")
        pointer=self.view('status_pointer',"SELECT 1 singleton,'a' release_id")
        names={'_gold_releases':releases,'_gold_stages':stages,'_gold_active':pointer}
        class Config:
            def table(_,n):return names[n]
        query=N['publication_status_query'](Config())
        self.assertEqual(self.db.query(query)[0]['publication_status'],'UNHEALTHY_ACTIVE')
        bad_pointer=self.view('status_bad_pointer',"SELECT 1 singleton,'a' release_id UNION ALL SELECT 2,'a'")
        names['_gold_active']=bad_pointer
        rows=self.db.query(N['publication_status_query'](Config()))
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['publication_status'],'INVALID_CONTROL_STATE');self.assertFalse(rows[0]['is_active_release'])
        duplicate=self.view('status_duplicate','SELECT * FROM '+releases+' UNION ALL SELECT * FROM '+releases)
        names['_gold_releases']=duplicate;names['_gold_active']=pointer
        rows=self.db.query(N['publication_status_query'](Config()))
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['publication_status'],'INVALID_CONTROL_STATE')

    def isolated_context(self,cfg,rid):
        from src.config import load_config
        from pathlib import Path
        from datetime import timezone
        silver=load_config(str(Path(__file__).resolve().parents[1]/'config/isolated.example.json'))
        keys=[silver.registry,silver.ledger,*[silver.silver(*key) for key in N['SOURCE_TABLES']],silver.silver('earthquakes','latest')]
        return N['GoldContext'](db=self.db,cfg=cfg,silver=silver,contract=N['notebook_contract'](Path(__file__).resolve().parents[1]),
          release_id=rid,evidence_id=rid*2,expected_predecessor='',expected_generation=0,versions=dict.fromkeys(keys,0),eligible_paths=[],
          built_at=datetime.now(timezone.utc).isoformat(),report={'fixture_only':True},prefix=self.prefix+rid[:3]+'_',temps=[],stages={},output_versions={})

    def fixture_sources(self,ctx,version):
        """Synthetic Silver boundary only: never fabricates committed raw evidence."""
        from datetime import timezone,timedelta
        registry=self.view('fixture_registry'+str(version),"""SELECT cast(id AS BIGINT) location_id,cast(id AS BIGINT) geonames_id,
          name location_name,lat latitude,lon longitude,cast(NULL AS STRING) state,cast(NULL AS STRING) state_code,
          'United States' country,'US' country_code,'UTC' timezone,cast(NULL AS DOUBLE) elevation,cast(NULL AS BIGINT) population,
          'PPL' feature_code,'fixture' admin2,'synthetic_test' location_source,'2026-01-01' modification_date,
          active is_active,active weather_enabled,active air_quality_enabled,timestamp'2026-01-01 00:00:00' created_at,timestamp'2026-01-01 00:00:00' updated_at
          FROM VALUES (1,'Fixture One',42D,-72.6D,true),(2,'Fixture Two',41.8D,-87.7D,true),(3,'No data',40D,-100D,false) r(id,name,lat,lon,active)""")
        sources={}
        for source,kinds in [('weather',('current','hourly','daily')),('air_quality',('current','hourly'))]:
            for kind in kinds:
                rows=[]
                for vintage in range(version+1):
                    snap='2026-01-0'+str(vintage+1)+'T00:'+('05' if source=='air_quality' and vintage==0 else '30' if source=='air_quality' else '00')+':00Z'
                    for identity in (1,2):
                        base={'location_id':identity,'location_name':'Fixture '+str(identity),'latitude':42. if identity==1 else 41.8,'longitude':-72.6 if identity==1 else -87.7,
                          'snapshot_at':snap,'source_file_path':f'fixture_{source}_{vintage}_{identity}','temperature_2m':10+identity+10*vintage,'pm2_5':5+identity+vintage,'pm10':8+identity,'us_aqi':50+vintage,'european_aqi':20+vintage}
                        if kind=='current':rows.append(base|{'observed_at':'2026-01-01T00:00:00Z','interval_seconds':3600})
                        elif kind=='daily':rows.append(base|{'forecast_date':'2026-01-01','temperature_2m_min':1.,'temperature_2m_max':20.+vintage})
                        else:
                            hours=[0,1] if source=='weather' else [0] if identity==1 else [2]
                            for hour in hours:rows.append(base|{'forecast_for':f'2026-01-01T{hour:02d}:00:00Z','shortwave_radiation':100.})
                            if vintage:rows.append(base|{'forecast_for':'2026-01-02T00:00:00Z','shortwave_radiation':200.})
                sources[source,kind]=self.frame(source+kind+str(version),source,kind,rows)
        events=[]
        for vintage in range(version+1):
            snap=f'2026-01-0{vintage+1}T01:00:00Z';updated=1767225600000+vintage*86400000
            common={'snapshot_at':snap,'source_file_path':'fixture_events_'+str(vintage),'feed_generated_ms':updated,'feed_generated_at':f'2026-01-0{vintage+1}T00:00:00Z'}
            events += [common|{'event_id':'near','latitude':43.,'longitude':-72.6,'magnitude':2.+vintage*3,'magnitude_type':'ml','depth_km':10.,'event_type':'earthquake' if not vintage else 'explosion','event_time':f'2026-01-0{vintage+1}T00:00:00Z','event_time_ms':updated,'updated_at':f'2026-01-0{vintage+1}T00:00:00Z','updated_at_ms':updated},
              common|{'event_id':'far','latitude':0.,'longitude':0.,'magnitude':-.5,'depth_km':-1.,'event_type':'quarry blast'}]
            if vintage:events.append(common|{'event_id':'chicago','latitude':41.8,'longitude':-87.7,'magnitude':3.5,'depth_km':50.,'event_time_ms':updated,'event_time':'2026-01-02T00:00:00Z','updated_at_ms':updated,'updated_at':'2026-01-02T00:00:00Z'})
        sources['earthquakes','history']=self.frame('event_history'+str(version),'earthquakes','history',events)
        ctx.update(registry=registry,sources=sources,evidence=[{'entry':{'source':'earthquakes'},'snapshot_at':f'2026-01-0{version+1}T01:00:00Z','feed_generated_ms':1767225600000+version*86400000}])

    def publish_fixture(self,cfg,rid,version):
        from src.validation.gold import validate_latest,reconcile_derived
        ctx=self.isolated_context(cfg,rid);self.fixture_sources(ctx,version)
        pointer=N['acquire_writer'](ctx,'weather');ctx.update(expected_predecessor=pointer['release_id'],expected_generation=pointer['generation'])
        try:
            N['ensure_state'](ctx)
            queries=N['weather_transformations'](ctx)|{'air_quality_'+k+'_latest':N['air_quality_query'](ctx['sources']['air_quality',k],ctx['registry'],k) for k in ('current','hourly')}
            queries.update(seismic_events_latest=N['seismic'](ctx['sources']['earthquakes','history']),earthquake_snapshot_audit=N['audit'](ctx['sources']['earthquakes','history'],list(output_types('earthquakes','history'))))
            allkeys=N['WEATHER_KEYS']|N['AQ_KEYS']|N['SEISMIC_KEYS']
            N['stage_products'](ctx,queries,allkeys)
            N['stage_products'](ctx,{'weather_aq_coverage':N['weather_aq_coverage'](ctx),'seismic_coverage':N['seismic_coverage'](ctx)},allkeys)
            candidate=N['candidate_mapping'](ctx);derived_queries=N['air_quality_views'](candidate)|N['earthquake_views'](candidate)
            derived,counts=N['build_derived'](ctx,derived_queries,N['AQ_VIEW_KEYS']|N['SEISMIC_VIEW_KEYS'])
            for stage,keys in [('weather',N['WEATHER_KEYS']),('air_quality',N['AQ_KEYS']),('earthquakes',N['SEISMIC_KEYS'])]:
                if stage!='weather':N['acquire_writer'](ctx,stage)
                N['stage_status'](ctx,stage,'BUILDING')
                owncounts={name:self.db.query('SELECT count(*) n FROM '+ctx['stages'][name])[0]['n'] for name in keys}
                ctx['output_versions']={};N['preflight_and_write'](ctx,keys,allkeys)
                N['stage_status'](ctx,stage,'VALIDATED',owncounts)
                if stage!='earthquakes':N['release_writer'](ctx)
            active=N['public_mapping'](ctx);history=N['history_mapping'](ctx)
            N['install_views'](ctx,N['air_quality_views'](active)|N['earthquake_views'](active),N['AQ_VIEW_KEYS']|N['SEISMIC_VIEW_KEYS'])
            N['install_history_views'](ctx,N['air_quality_views'](history)|N['earthquake_views'](history),N['AQ_VIEW_KEYS']|N['SEISMIC_VIEW_KEYS'])
            N['install_views'](ctx,{'published_releases':N['published_releases_query'](cfg),'publication_status':N['publication_status_query'](cfg)}, {})
            self.db.query(f"UPDATE {cfg.table('_gold_releases')} SET publication_state='READY',health='HEALTHY' WHERE release_id={literal(rid)}")
            def verify():
                for name,rel in ctx['stages'].items():reconcile(self.db,rel,cfg.table(name))
                for name,rel in derived.items():reconcile_derived(self.db,rel,cfg.table(name),freshness='freshness' in name)
            N['activate_release'](self.db,cfg,rid,ctx['expected_predecessor'],verify,generation=ctx['expected_generation'],writer_token=ctx['writer_token'],verify_prior=lambda:N['verify_release_copies'](ctx,ctx['expected_predecessor']))
            self.db.query(f"UPDATE {cfg.table('_gold_releases')} SET publication_state='PUBLISHED',published_at=current_timestamp(),verified_at=current_timestamp() WHERE release_id={literal(rid)}")
        finally:ctx.close()
        return list(allkeys)+list(N['AQ_VIEW_KEYS'])+list(N['SEISMIC_VIEW_KEYS'])

    def capture_history(self,cfg,rid,products):
        result={}
        excluded='retrieval_age_seconds,gold_build_age_seconds,gold_published_at,retrieval_freshness'
        for name in products:
            relation=cfg.table(name+'_history');cols='* EXCEPT('+excluded+')' if 'freshness' in name else '*'
            result[name]=self.view('expected_'+rid[:1]+name,'SELECT '+cols+' FROM '+relation+' WHERE _release_id='+literal(rid))
            # Freeze content now in a TEMP VIEW of literal JSON-independent VALUES, not a lazy source reference.
            rows=self.db.query('SELECT * FROM '+result[name]);schema=self.db.query('DESCRIBE '+result[name])
            result[name]={'rows':rows,'schema':[(r['col_name'],r['data_type']) for r in schema if r['col_name'] and not r['col_name'].startswith('#')]}
        return result

    def assert_history(self,cfg,rid,expected):
        excluded='retrieval_age_seconds,gold_build_age_seconds,gold_published_at,retrieval_freshness'
        def canonical(rows):
            def encode(x):
                if hasattr(x,'tolist'):return x.tolist()
                return x.isoformat() if hasattr(x,'isoformat') else str(x)
            return sorted(json.dumps(r,sort_keys=True,default=encode) for r in rows)
        for name,data in expected.items():
            cols='* EXCEPT('+excluded+')' if 'freshness' in name else '*';relation=cfg.table(name+'_history')
            actual=self.db.query('SELECT '+cols+' FROM '+relation+' WHERE _release_id='+literal(rid))
            self.assertEqual(canonical(actual),canonical(data['rows']),name)
            schema=self.db.query('DESCRIBE '+relation);schema=[(r['col_name'],r['data_type']) for r in schema if r['col_name'] and not r['col_name'].startswith('#') and (not 'freshness' in name or r['col_name'] not in excluded.split(','))]
            self.assertEqual(schema,data['schema'],name)

    def create_v1_fixture(self,source,cfg,rid):
        self.db.query('CREATE SCHEMA '+cfg.catalog+'.'+cfg.schema)
        pins={name:N['_version'](self.db,source.table(name)) for name in ['_gold_active','_gold_releases','_gold_stages']+['_data_'+n for n in N['WEATHER_KEYS']|N['AQ_KEYS']|N['SEISMIC_KEYS']]}
        for name in N['WEATHER_KEYS']|N['AQ_KEYS']|N['SEISMIC_KEYS']:
            target='_data_'+name
            self.db.query(f"CREATE TABLE {cfg.table(target)} USING DELTA TBLPROPERTIES ('weather_pipeline.gold.contract'='gold-notebooks-v1') AS SELECT * FROM {source.table(target)} VERSION AS OF {pins[target]} WHERE _release_id={literal(rid)}")
        old={'_gold_active':'singleton,release_id','_gold_releases':'release_id,contract_id,source_config_id,input_versions,eligible_paths,settings,built_at,report,published_at','_gold_stages':'release_id,stage_name,status,counts_json,completed_at,error_type'}
        for name,columns in old.items():
            projection=columns.replace('settings',literal(GoldConfig(catalog='weather_api_test', schema=cfg.schema).json())+' settings')
            where=' WHERE release_id='+literal(rid)
            self.db.query(f"CREATE TABLE {cfg.table(name)} USING DELTA TBLPROPERTIES ('weather_pipeline.gold.contract'='gold-notebooks-v1') AS SELECT {projection} FROM {source.table(name)} VERSION AS OF {pins[name]}{where}")
        for candidate,status in [('f'*32,'FAILED'),('e'*32,'BUILDING')]:
            self.db.query(f"INSERT INTO {cfg.table('_gold_releases')} SELECT {literal(candidate)},contract_id,source_config_id,input_versions,eligible_paths,settings,built_at,report,NULL FROM {cfg.table('_gold_releases')} WHERE release_id={literal(rid)}")
            self.db.query(f"INSERT INTO {cfg.table('_gold_stages')} SELECT {literal(candidate)},stage_name,{literal(status)},counts_json,completed_at,'isolated_fixture' FROM {cfg.table('_gold_stages')} WHERE release_id={literal(rid)} AND stage_name='weather'")
        mapping={name:cfg.table(name) for name in N['WEATHER_KEYS']|N['AQ_KEYS']|N['SEISMIC_KEYS']}
        mapping['_gold_settings']=f"(SELECT r.* FROM {cfg.table('_gold_releases')} r JOIN {cfg.table('_gold_active')} p ON r.release_id=p.release_id)"
        queries={name:f"SELECT d.* FROM {cfg.table('_data_'+name)} d JOIN {cfg.table('_gold_active')} p ON d._release_id=p.release_id" for name in N['WEATHER_KEYS']|N['AQ_KEYS']|N['SEISMIC_KEYS']}
        queries.update(N['air_quality_views'](mapping)|N['earthquake_views'](mapping))
        queries['publication_status']=f"SELECT r.release_id,r.built_at,r.published_at,r.input_versions,r.eligible_paths,cast(3 AS BIGINT) validated_stages,cast(0 AS BIGINT) failed_stages,true is_active_release,'ACTIVE_VALIDATED_RELEASE' publication_status FROM {cfg.table('_gold_releases')} r"
        for name,q in queries.items():self.db.query(f"CREATE VIEW {cfg.table(name)} COMMENT 'weather-pipeline Gold gold-notebooks-v1 fixture' AS {q}")
        self.assertEqual(pins,{name:N['_version'](self.db,source.table(name)) for name in pins})

    def test_isolated_delta_migration_writer_and_history(self):
        if not getattr(self,'new_connection',None) or not getattr(self,'allow_isolated_persistent',False):self.skipTest('Explicit separate-session factory and isolated-persistent authorization required')
        from pathlib import Path
        from concurrent.futures import ThreadPoolExecutor
        import threading,time,sys
        root=Path(__file__).resolve().parents[1]
        cfg=GoldConfig(catalog='weather_api_test', schema='gold_verify_'+uuid.uuid4().hex[:12])
        legacy=GoldConfig(catalog='weather_api_test', schema='gold_verify_'+uuid.uuid4().hex[:12])
        def progress(label,**fields):print(json.dumps({'at':datetime.now().isoformat(),'phase':label,'target':cfg.schema,'legacy':legacy.schema,**self.db.metrics(),**fields}),file=sys.stderr,flush=True)
        progress('fresh-init-start')
        for statement in N['migration_plan'](project_root=root,gold=cfg)['sql']:self.db.query(statement)
        N['controls'](self.isolated_context(cfg,'a'*32));progress('fresh-init-passed')
        connections=[self.new_connection(),self.new_connection()];executors=[SQLExecutor(c,verbose=False) for c in connections]
        contexts=[self.isolated_context(cfg,'a'*32) for _ in range(2)]
        for c,db in zip(contexts,executors):c['db']=db
        barrier=threading.Barrier(2)
        def acquire(index):
            barrier.wait()
            try:N['acquire_writer'](contexts[index],'weather');return index
            except Exception as exc:return {'error':type(exc).__name__,'detail':str(exc)[:1200]}
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(acquire,range(2)))
        winners=[r for r in results if type(r) is int];self.assertEqual(len(winners),1,results);winner=winners[0];loser=1-winner
        token=contexts[winner]['writer_token'];N['release_writer'](contexts[loser]);self.assertEqual(self.db.query('SELECT writer_token FROM '+cfg.table('_gold_active'))[0]['writer_token'],token)
        recovery=self.isolated_context(cfg,'a'*32)
        with self.assertRaises(ValidationError):N['recover_writer'](recovery,expected_token=token)
        connections[winner].close() # Abandoned ownership; no statements in flight. Not a hard process-kill test.
        N['recover_writer'](recovery,expected_token=token,confirmed_terminated=True,termination_evidence='test: synchronous owner session closed; all submitted statements terminal')
        contexts[winner]['db']=self.db
        with self.assertRaises(ValidationError):N['require_writer'](contexts[winner])
        N['acquire_writer'](contexts[loser],'air_quality');N['release_writer'](contexts[loser]);connections[loser].close()
        progress('two-session-writer-recovery-passed',acquisition_results=results)
        products=self.publish_fixture(cfg,'a'*32,0);expected_a=self.capture_history(cfg,'a'*32,products);progress('release-A-published',products=len(products))
        self.db.query(f"INSERT INTO {cfg.table('_gold_active')} SELECT 2,release_id,generation,writer_token,writer_stage,writer_acquired_at,deployment_contract FROM {cfg.table('_gold_active')} WHERE singleton=1")
        try:
            with self.assertRaises(ValidationError):N['controls'](self.isolated_context(cfg,'a'*32))
        finally:self.db.query('DELETE FROM '+cfg.table('_gold_active')+' WHERE singleton=2')
        columns=[part.split()[0] for part in N['STATE_SCHEMAS']['_gold_releases'].split(',')]
        projection=','.join(literal('f'*32) if name=='release_id' else name for name in columns)
        for _ in range(2):
            self.db.query(f"INSERT INTO {cfg.table('_gold_releases')} SELECT {projection} FROM {cfg.table('_gold_releases')} WHERE release_id={literal('a'*32)}")
        try:
            with self.assertRaises(ValidationError):N['controls'](self.isolated_context(cfg,'a'*32))
        finally:self.db.query('DELETE FROM '+cfg.table('_gold_releases')+' WHERE release_id='+literal('f'*32))
        N['controls'](self.isolated_context(cfg,'a'*32));progress('invalid-and-duplicate-controls-rejected')
        self.create_v1_fixture(cfg,legacy,'a'*32);progress('v1-fixture-created')
        plan=N['migration_plan'](self.db,project_root=root,gold=legacy);self.assertEqual(len(plan['sql']),59)
        progress('v1-plan-verified',statement_count=len(plan['sql']))
        first=N['apply_migration_plan'](self.db,plan,gold=legacy,stop_after=2);self.assertEqual(first['checkpoint'],2)
        # Unexpected view baseline is detected. Restore exact view DDL, never reset a Delta version.
        saved=next(q for q in plan['legacy_view_rollback'] if legacy.table('weather_current_latest') in q.split(' AS ',1)[0])
        self.db.query(f"CREATE OR REPLACE VIEW {legacy.table('weather_current_latest')} COMMENT 'unexpected isolated change' AS SELECT * FROM {legacy.table('_data_weather_current_latest')} WHERE false")
        with self.assertRaises(ValidationError):N['apply_migration_plan'](self.db,plan,gold=legacy)
        self.db.query(saved)
        resumed=N['migration_plan'](self.db,project_root=root,gold=legacy);self.assertEqual(resumed['migration_id'],plan['migration_id'])
        result=N['apply_migration_plan'](self.db,resumed,gold=legacy);self.assertEqual(result['status'],'COMPLETE');progress('migration-resume-passed')
        self.assertEqual(self.db.query('SELECT release_id FROM '+legacy.table('_gold_active'))[0]['release_id'],'a'*32)
        self.assertEqual(self.db.query('SELECT count(*) n FROM '+legacy.table('published_releases'))[0]['n'],0)
        for name in N['WEATHER_KEYS']|N['AQ_KEYS']|N['SEISMIC_KEYS']:
            self.assertEqual(self.db.query('SELECT count(*) n FROM '+legacy.table(name))[0]['n'],len(expected_a[name]['rows']))
        candidates=self.db.query('SELECT release_id,publication_state FROM '+legacy.table('_gold_releases'))
        self.assertEqual({r['release_id']:r['publication_state'] for r in candidates},{'a'*32:'LEGACY_ACTIVE','f'*32:'FAILED','e'*32:'CANDIDATE'})
        self.assertEqual(self.db.query('SELECT count(*) n FROM '+legacy.table('_gold_stages'))[0]['n'],5)
        progress('legacy-readable-not-falsely-admitted')
        self.publish_fixture(legacy,'d'*32,1);progress('new-v2-after-migration-published')
        cfg_b=GoldConfig(catalog='weather_api_test', schema=cfg.schema,nearby_radius_km=75,grid_degrees=1,alignment_seconds=60,stale_seconds=60)
        self.publish_fixture(cfg_b,'b'*32,1);expected_b=self.capture_history(cfg_b,'b'*32,products)
        self.assert_history(cfg,'a'*32,expected_a);self.assert_history(cfg_b,'b'*32,expected_b)
        for name in products:
            rows=self.db.query('SELECT DISTINCT _release_id FROM '+cfg.table(name))
            self.assertTrue(all(r['_release_id']=='b'*32 for r in rows),name)
        self.assertEqual(self.db.query('SELECT DISTINCT grid_degrees FROM '+cfg.table('seismic_grid_daily_history')+" WHERE _release_id='"+'a'*32+"'")[0]['grid_degrees'],2.)
        self.assertEqual(self.db.query('SELECT DISTINCT grid_degrees FROM '+cfg.table('seismic_grid_daily_history')+" WHERE _release_id='"+'b'*32+"'")[0]['grid_degrees'],1.)
        # Historical health/policy exclusions are isolated and restored after each check.
        for state,health in [('CANDIDATE','UNKNOWN'),('FAILED','UNHEALTHY'),('PUBLISHED','UNHEALTHY'),('WITHDRAWN','UNHEALTHY')]:
            self.db.query(f"UPDATE {cfg.table('_gold_releases')} SET publication_state={literal(state)},health={literal(health)} WHERE release_id={literal('a'*32)}")
            self.assertEqual(self.db.query('SELECT count(*) n FROM '+cfg.table('dim_location_history')+' WHERE _release_id='+literal('a'*32))[0]['n'],0)
        self.db.query(f"UPDATE {cfg.table('_gold_releases')} SET publication_state='PUBLISHED',health='HEALTHY' WHERE release_id={literal('a'*32)}")
        self.assert_history(cfg,'a'*32,expected_a)
        progress('all-history-products-and-settings-passed',historical_products=len(products),retained_schemas=[cfg.schema,legacy.schema])

    def test_retained_isolated_independent_expectations(self):
        schema=getattr(self,'retained_schema',None)
        if not schema:self.skipTest('Explicit retained isolated schema required')
        from datetime import timezone
        cfg=GoldConfig(catalog='weather_api_test', schema=schema).validate()
        self.assertNotEqual(cfg.schema,'gold')
        for rid,temperature,aqi,grid,radius,threshold,label,staleness,events,hours in [
          ('a'*32,11.,50,2.,250.,900.,'within_threshold',10800.,2,5),
          ('b'*32,21.,51,1.,75.,60.,'retrievals_apart',60.,3,7)]:
            predicate=' WHERE _release_id='+literal(rid)
            w=self.db.query('SELECT temperature_2m FROM '+cfg.table('weather_current_latest_history')+predicate+' AND location_id=1')[0]
            self.assertEqual(w['temperature_2m'],temperature)
            a=self.db.query('SELECT us_aqi FROM '+cfg.table('air_quality_current_latest_history')+predicate+' AND location_id=1')[0]
            self.assertEqual(a['us_aqi'],aqi)
            env=self.db.query('SELECT alignment_quality FROM '+cfg.table('environment_hourly_history')+predicate+" AND location_id=1 AND forecast_for=timestamp'2026-01-01 00:00:00'")[0]
            self.assertEqual(env['alignment_quality'],label)
            self.assertEqual(self.db.query('SELECT count(*) n FROM '+cfg.table('environment_hourly_history')+predicate)[0]['n'],hours)
            self.assertTrue(all(r['grid_degrees']==grid for r in self.db.query('SELECT DISTINCT grid_degrees FROM '+cfg.table('seismic_grid_daily_history')+predicate)))
            self.assertTrue(all(r['radius_km']==radius for r in self.db.query('SELECT radius_km FROM '+cfg.table('location_seismic_nearby_history')+predicate)))
            self.assertEqual(self.db.query('SELECT count(*) n FROM '+cfg.table('seismic_events_latest_history')+predicate)[0]['n'],events)
            before=datetime.now(timezone.utc)
            rows=self.db.query('SELECT latest_retrieval_at,retrieval_age_seconds,stale_after_seconds,gold_published_at FROM '+cfg.table('freshness_coverage_history')+predicate)
            after=datetime.now(timezone.utc)
            published=self.db.query('SELECT published_at FROM '+cfg.table('published_releases')+' WHERE release_id='+literal(rid))[0]['published_at']
            for row in rows:
                self.assertEqual(row['stale_after_seconds'],staleness);self.assertEqual(row['gold_published_at'],published)
                if row['latest_retrieval_at'] is None:self.assertIsNone(row['retrieval_age_seconds']);continue
                retrieved=row['latest_retrieval_at']
                if retrieved.tzinfo is None:retrieved=retrieved.replace(tzinfo=timezone.utc) # SQL session is explicitly UTC.
                self.assertGreaterEqual(row['retrieval_age_seconds'],int((before-retrieved).total_seconds())-1)
                self.assertLessEqual(row['retrieval_age_seconds'],int((after-retrieved).total_seconds())+1)
        joined=self.db.query('SELECT count(*) n FROM '+cfg.table('weather_current_latest_history')+' w JOIN '+cfg.table('air_quality_current_latest_history')+' a ON w._release_id=a._release_id AND w.location_id=a.location_id')[0]['n']
        self.assertEqual(joined,4)

    def test_retained_isolated_retry_handoff_and_pointer_failure(self):
        if not getattr(self,'allow_isolated_persistent',False):self.skipTest('Explicit isolated-persistent authorization required')
        schema=getattr(self,'retained_schema',None)
        if not schema:self.skipTest('Explicit retained isolated schema required')
        from unittest.mock import Mock
        cfg=GoldConfig(catalog='weather_api_test', schema=schema,nearby_radius_km=75,grid_degrees=1,alignment_seconds=60,stale_seconds=60).validate()
        self.assertNotEqual(cfg.schema,'gold')
        ctx=self.isolated_context(cfg,'b'*32)
        before={name:N['_version'](self.db,cfg.table('_data_'+name)) for name in N['WEATHER_KEYS']}
        try:
            N['acquire_writer'](ctx,'weather');ctx['output_versions']={}
            ctx['stages']={name:ctx.temp('retry_'+name,'SELECT * FROM '+cfg.table('_data_'+name)+' WHERE _release_id='+literal('b'*32)) for name in N['WEATHER_KEYS']}
            N['preflight_and_write'](ctx,N['WEATHER_KEYS'],N['WEATHER_KEYS']|N['AQ_KEYS']|N['SEISMIC_KEYS'])
            self.assertEqual(before,{name:N['_version'](self.db,cfg.table('_data_'+name)) for name in N['WEATHER_KEYS']})
            utility=Mock();utility.jobs.taskValues.set.return_value=False
            with self.assertRaises(ValidationError):N['publish_weather_task_value'](utility,{'stage':'weather','release_id':'b'*32,'validate_only':False,'ready_for_next_stage':True},job_mode=True)
        finally:ctx.close()
        # Exercise actual Delta pointer commit/conditional rollback on injected verification error.
        # This is a control-plane fault test, not proof of a complete failed run_* adapter.
        failed=self.isolated_context(cfg,'c'*32)
        try:
            pointer=N['acquire_writer'](failed,'earthquakes')
            generation=pointer['generation'];failed.update(expected_predecessor='b'*32,expected_generation=generation,failure_mutations_allowed=True,write_stage='earthquakes')
            N['ensure_state'](failed)
            def fail():raise ValidationError('isolated injected post-pointer verification failure')
            with self.assertRaises(ValidationError):N['activate_release'](self.db,cfg,'c'*32,'b'*32,fail,generation=generation,writer_token=failed['writer_token'],verify_prior=lambda:N['verify_release_copies'](failed,'b'*32))
            actual=self.db.query('SELECT release_id,generation FROM '+cfg.table('_gold_active'))[0]
            self.assertEqual(actual,{'release_id':'b'*32,'generation':generation+2})
            N['record_failure'](failed,ValidationError('isolated injected publication error'))
            receipt=self.db.query('SELECT publication_state,health FROM '+cfg.table('_gold_releases')+' WHERE release_id='+literal('c'*32))[0]
            self.assertEqual(receipt,{'publication_state':'FAILED','health':'UNHEALTHY'})
            self.assertEqual(self.db.query('SELECT count(*) n FROM '+cfg.table('published_releases')+' WHERE release_id='+literal('c'*32))[0]['n'],0)
        finally:failed.close()
        self.assertIsNone(self.db.query('SELECT writer_token FROM '+cfg.table('_gold_active'))[0]['writer_token'])


    def test_retained_isolated_hard_kill_recovery(self):
        if not getattr(self,'allow_isolated_persistent',False):self.skipTest('Explicit isolated-persistent authorization required')
        schema=getattr(self,'retained_schema',None)
        if not schema:self.skipTest('Explicit retained isolated schema required')
        import os,signal,subprocess,sys,tempfile,time
        from pathlib import Path
        cfg=GoldConfig(catalog='weather_api_test', schema=schema).validate();self.assertNotEqual(cfg.schema,'gold')
        root=Path(__file__).resolve().parents[1]
        settings_path=str(Path(getattr(self,'connection_settings_path',str(root/'check_databricks_connection.py'))).resolve())
        child_code="""
import json,os,sys,time
from pathlib import Path
from databricks import sql
from tests.test_gold_sql import N,GoldSQLTests,SQLExecutor,GoldConfig,connection_settings
settings=connection_settings(sys.argv[1]);settings['_socket_timeout']=60
with sql.connect(**settings) as conn:
 test=GoldSQLTests('test_retained_isolated_hard_kill_recovery');test.db=SQLExecutor(conn,verbose=False);test.setUp()
 ctx=test.isolated_context(GoldConfig(catalog='weather_api_test', schema=sys.argv[2]),'b'*32)
 N['acquire_writer'](ctx,'weather')
 # All SQL is synchronous and terminal before handshake; submit nothing afterward.
 ready=Path(sys.argv[3]);pending=ready.with_suffix('.tmp')
 pending.write_text(json.dumps({'pid':os.getpid(),'token':ctx['writer_token'],'all_sql_terminal':True}));pending.replace(ready)
 time.sleep(300)
"""
        with tempfile.TemporaryDirectory(prefix='gold-kill-') as directory:
            ready=Path(directory)/'ready.json'
            process=subprocess.Popen([sys.executable,'-c',child_code,settings_path,schema,str(ready)],cwd=root,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                deadline=time.monotonic()+120
                while not ready.exists() and time.monotonic()<deadline and process.poll() is None:time.sleep(.1)
                self.assertTrue(ready.exists(),'Owner did not acknowledge terminal SQL before timeout; inspect guard/query history')
                evidence=json.loads(ready.read_text());self.assertEqual(evidence['pid'],process.pid);self.assertTrue(evidence['all_sql_terminal'])
                token=evidence['token'];blocked=self.isolated_context(cfg,'b'*32)
                with self.assertRaises(ValidationError):N['acquire_writer'](blocked,'air_quality')
                N['release_writer'](blocked)
                recovery=self.isolated_context(cfg,'b'*32)
                with self.assertRaises(ValidationError):N['recover_writer'](recovery,expected_token=token)
                process.kill();self.assertEqual(process.wait(timeout=30),-signal.SIGKILL)
                self.assertEqual(self.db.query('SELECT writer_token FROM '+cfg.table('_gold_active'))[0]['writer_token'],token)
                confirmation=f'isolated SIGKILL pid={process.pid}; waitpid=-SIGKILL; synchronous SQL terminal before ready handshake; no later submissions'
                N['recover_writer'](recovery,expected_token=token,confirmed_terminated=True,termination_evidence=confirmation)
                stale=self.isolated_context(cfg,'b'*32);stale['writer_token']=token
                with self.assertRaises(ValidationError):N['require_writer'](stale)
                winner=self.isolated_context(cfg,'b'*32)
                try:N['acquire_writer'](winner,'air_quality')
                finally:winner.close()
                self.assertIsNone(self.db.query('SELECT writer_token FROM '+cfg.table('_gold_active'))[0]['writer_token'])
                print(json.dumps({'phase':'actual-sigkill-recovery-passed','schema':schema,'pid':process.pid,'sql_in_flight_at_kill':False}),flush=True)
            finally:
                if process.poll() is None:process.kill();process.wait(timeout=30)
                process.communicate(timeout=5)


    def test_retained_legacy_provenance_and_copies(self):
        schema=getattr(self,'retained_schema',None)
        if not schema:self.skipTest('Explicit retained migrated fixture required')
        cfg=GoldConfig(catalog='weather_api_test', schema=schema).validate();self.assertNotEqual(cfg.schema,'gold')
        receipts={r['release_id']:r for r in self.db.query('SELECT * FROM '+cfg.table('_gold_releases'))}
        self.assertEqual(set(receipts),{'a'*32,'d'*32,'e'*32,'f'*32})
        original=receipts['a'*32]
        fields='contract_id,source_config_id,input_versions,eligible_paths,settings,built_at,report'.split(',')
        for rid,state in [('f'*32,'FAILED'),('e'*32,'CANDIDATE')]:
            row=receipts[rid]
            for field in fields:self.assertEqual(row[field],original[field],(rid,field))
            self.assertIsNone(row['published_at']);self.assertIsNone(row['verified_at'])
            self.assertEqual(row['publication_state'],state);self.assertEqual(row['health'],'UNKNOWN')
        stages=self.db.query('SELECT * FROM '+cfg.table('_gold_stages'))
        initial=next(r for r in stages if r['release_id']=='a'*32 and r['stage_name']=='weather')
        for rid,status in [('f'*32,'FAILED'),('e'*32,'BUILDING')]:
            row=next(r for r in stages if r['release_id']==rid)
            self.assertEqual(row['status'],status);self.assertEqual(row['stage_name'],'weather')
            for field in ('counts_json','completed_at'):self.assertEqual(row[field],initial[field])
            self.assertEqual(row['error_type'],'isolated_fixture');self.assertEqual(json.loads(row['output_versions']),{})
        self.assertEqual(len(stages),8)
        N['verify_release_copies'](self.isolated_context(cfg,'d'*32),'a'*32)
        self.assertEqual(self.db.query('SELECT release_id FROM '+cfg.table('_gold_active'))[0]['release_id'],'d'*32)
        self.assertEqual({r['release_id'] for r in self.db.query('SELECT release_id FROM '+cfg.table('published_releases'))},{'d'*32})


def run_suite(db,cases=None):
    GoldSQLTests.db=db
    suite=unittest.TestSuite(GoldSQLTests(name) for name in cases) if cases else unittest.defaultTestLoader.loadTestsFromTestCase(GoldSQLTests)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():raise RuntimeError('Gold SQL regressions failed')
    return {'sql_tests_run':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'metrics':db.metrics()}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--connection-settings',required=True);p.add_argument('--case',action='append');p.add_argument('--timeout-seconds',type=int,default=300);p.add_argument('--isolated-persistent',action='store_true');p.add_argument('--retained-schema');args=p.parse_args()
    import signal
    if args.timeout_seconds<=0:raise ValueError('Positive batch timeout required')
    signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(TimeoutError('Gold SQL batch timeout; inspect current query, do not duplicate batch')))
    signal.alarm(args.timeout_seconds)
    from databricks import sql
    settings=connection_settings(args.connection_settings);settings['_socket_timeout']=60
    GoldSQLTests.new_connection=staticmethod(lambda:sql.connect(**settings))
    GoldSQLTests.allow_isolated_persistent=args.isolated_persistent
    GoldSQLTests.retained_schema=args.retained_schema
    GoldSQLTests.connection_settings_path=args.connection_settings
    with sql.connect(**settings) as conn:
        print(json.dumps(run_suite(SQLExecutor(conn),args.case)))
