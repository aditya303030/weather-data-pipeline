"""Optional real local Spark contracts and Delta production-MERGE tests. No services."""
import importlib.util
import json
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch
from helpers import payload,wrapper,config_at
HAS_SPARK=importlib.util.find_spec('pyspark') is not None
HAS_DELTA=importlib.util.find_spec('delta') is not None

@unittest.skipUnless(HAS_SPARK,'Local PySpark unavailable')
class ContractSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark=SparkSession.builder.master('local[1]').appName('offline-contracts').config('spark.ui.enabled','false').getOrCreate()
        if not cls.spark.sparkContext.master.startswith('local'):
            raise unittest.SkipTest('Offline tests require a local Spark context')
        cls.spark.conf.set('spark.sql.session.timeZone','UTC')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def frame(self,data,source):
        from src.transformations.schemas import envelope_schema
        from pyspark.sql import functions as F
        return self.spark.read.schema(envelope_schema(source)).json(self.spark.sparkContext.parallelize([json.dumps(wrapper(source=source,data=data))])).withColumn('_source_file_path',F.lit('/x')).withColumn('_ingest_timestamp',F.to_timestamp(F.lit('2026-10-21T00:00:00Z')))
    def test_accepted_missing_optional_payload_transforms_and_schema_is_stable(self):
        from importlib import import_module
        from src.validation.ingestion import validate_response
        for source in ('weather','air_quality'):
            schemas=[]
            for val in (1,1.5,None):
                data=payload(source,value=val)
                for section in ('hourly','daily') if source=='weather' else ('hourly',):data[section]={'time':data[section]['time']}
                validate_response(source,data)
                outputs=import_module('src.transformations.'+source).transform(self.spark,self.frame(data,source))
                self.assertEqual(outputs['hourly'].count(),1)
                self.assertIsNone(outputs['current'].first().elevation_m)
                schemas.append({k:v.schema.simpleString() for k,v in outputs.items()})
            self.assertEqual(schemas[0],schemas[1]);self.assertEqual(schemas[1],schemas[2])
    def test_bad_cast_blocks(self):
        from src.validation.common import normalize_output,ValidationError
        df=self.spark.createDataFrame([('nonnumeric',)],'measurement string')
        with self.assertRaises(ValidationError):normalize_output(df,{'measurement':'double'})
    def test_real_preflight_later_schema_mismatch_prevents_writes(self):
        from src.transformations.incremental import preflight_outputs
        from src.validation.common import ValidationError
        good=self.spark.createDataFrame([(1,)],'id long');bad=self.spark.createDataFrame([('wrong',)],'id string')
        spark=__import__('unittest.mock',fromlist=['Mock']).Mock()
        spark.catalog.tableExists.side_effect=lambda n:n=='later'
        spark.table.return_value=bad
        with self.assertRaises(ValidationError):preflight_outputs(spark,{'first':good,'later':good},{'first':'first','later':'later'},{'first':['id'],'later':['id']})
        spark.sql.assert_not_called()

@unittest.skipUnless(HAS_SPARK and HAS_DELTA,'Local PySpark/Delta runtime unavailable')
class ProductionDeltaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # No configure_spark_with_delta_pip: that may download Maven artifacts.
        # Runner must supply local Delta jars, extensions and catalog in advance.
        from pyspark.sql import SparkSession
        cls.spark=SparkSession.builder.master('local[1]').appName('offline-delta').getOrCreate()
        if not cls.spark.sparkContext.master.startswith('local'):
            raise unittest.SkipTest('Offline Delta tests require a local Spark context')
        if cls.spark.conf.get('spark.sql.extensions','').find('DeltaSparkSessionExtension')<0:
            cls.spark.stop();raise unittest.SkipTest('Preinstalled local Delta JVM jars/extensions required; no downloads allowed')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def test_production_merge_rejects_older_and_accepts_newer(self):
        from src.transformations.incremental import merge_output
        from uuid import uuid4
        table='offline_merge_'+uuid4().hex
        def rows(updated,snapshot,value):return self.spark.createDataFrame([('e',datetime(2026,1,1,0,0,updated),datetime(2026,1,1,0,0,snapshot),value)],'event_id string, updated_at timestamp, snapshot_at timestamp, magnitude double')
        try:
            merge_output(self.spark,rows(2,2,2.),table,['event_id'],latest=True)
            merge_output(self.spark,rows(1,3,1.),table,['event_id'],latest=True)
            self.assertEqual(self.spark.table(table).first().magnitude,2.)
            merge_output(self.spark,rows(3,1,3.),table,['event_id'],latest=True)
            self.assertEqual(self.spark.table(table).first().magnitude,3.)
            merge_output(self.spark,rows(3,1,3.),table,['event_id'],latest=True)
            self.assertEqual(self.spark.table(table).count(),1)
        finally:self.spark.sql('DROP TABLE IF EXISTS '+table)
    def test_real_multi_table_partial_write_replay_is_idempotent(self):
        from src.transformations.incremental import merge_output,preflight_outputs
        from uuid import uuid4
        prefix='offline_retry_'+uuid4().hex
        targets={k:prefix+'_'+k for k in ('first','second')}
        source=self.spark.createDataFrame([(1,1.5)],'id long, value double')
        outputs={k:source for k in targets};keys={k:['id'] for k in targets}
        try:
            preflight_outputs(self.spark,outputs,targets,keys)
            merge_output(self.spark,source,targets['first'],['id'])
            # An unexpected failure leaves only the first table persisted.
            self.assertFalse(self.spark.catalog.tableExists(targets['second']))
            preflight_outputs(self.spark,outputs,targets,keys)
            for k in targets:merge_output(self.spark,source,targets[k],['id'])
            for k in targets:
                self.assertEqual(self.spark.table(targets[k]).count(),1)
                self.assertEqual(self.spark.table(targets[k]).first().value,1.5)
        finally:
            for table in targets.values():self.spark.sql('DROP TABLE IF EXISTS '+table)
