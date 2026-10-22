"""Optional offline Spark integration tests; no Databricks connection or HTTP.

These run automatically when PySpark is installed and a local JVM is available.
They intentionally do not test UC Volume semantics or Delta persistence.
"""
import importlib.util
import unittest
from datetime import datetime
from unittest.mock import patch
from src.validation.common import ValidationError

HAS_SPARK=importlib.util.find_spec('pyspark') is not None

@unittest.skipUnless(HAS_SPARK,'PySpark is not installed locally')
class SparkBoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark=SparkSession.builder.master('local[1]').appName('offline-pipeline-tests').config('spark.ui.enabled','false').getOrCreate()
        if not cls.spark.sparkContext.master.startswith('local'):
            raise unittest.SkipTest('Offline tests require a local Spark context')
        cls.spark.conf.set('spark.sql.session.timeZone','UTC')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def registry(self):
        return self.spark.createDataFrame([(1,1,'X',40.,-90.,'US',True,True,False,datetime(2020,1,1),datetime(2026,1,1))], 'location_id long, geonames_id long, location_name string, latitude double, longitude double, country_code string, is_active boolean, weather_enabled boolean, air_quality_enabled boolean, created_at timestamp, updated_at timestamp')
    def test_empty_registry_rejected(self):
        from src.validation.location_registry import validate_registry
        with self.assertRaises(ValidationError):validate_registry(self.registry().limit(0))
    def test_refresh_preserves_activation_and_creation_time(self):
        from src.ingestion.location_discovery import preserve_registry_settings
        from src.validation.location_registry import validate_registry
        from pyspark.sql import functions as F
        old=self.registry()
        fresh=old.withColumn('weather_enabled',F.lit(False)).withColumn('created_at',F.current_timestamp())
        preserved=preserve_registry_settings(fresh,old)
        validate_registry(preserved,old)
        self.assertEqual(preserved.first().created_at,datetime(2020,1,1))
        self.assertTrue(preserved.first().weather_enabled)
        with self.assertRaises(ValidationError):validate_registry(fresh,old)
    def test_deterministic_activation_selection(self):
        from src.ingestion.location_discovery import enable_locations_for_ingestion
        from unittest.mock import Mock
        candidates=self.spark.createDataFrame([(3,100,True),(1,100,True),(2,200,True)],'location_id long, population long, is_active boolean')
        spark=Mock();spark.catalog.tableExists.return_value=True;spark.table.return_value=candidates
        spark.sql.return_value.collect.return_value=[{'cnt':2}]
        enable_locations_for_ingestion(spark,limit=2)
        self.assertIn('location_id IN (1,2)',spark.sql.call_args_list[0].args[0])
    def test_weather_location_id_survives_final_projections(self):
        import json
        from test_pipeline import wrapper
        from src.transformations.weather import transform
        data=wrapper();data.update(_source_file_path='/x',_rescued_data=None,_ingest_timestamp='2026-10-21T00:00:00Z')
        data['raw_response']['elevation']=10.;data['raw_response']['current']['interval']=900
        from pyspark.sql import functions as F
        from src.transformations.schemas import envelope_schema
        bronze=self.spark.read.schema(envelope_schema('weather')).json(self.spark.sparkContext.parallelize([json.dumps(data)])).withColumn('_ingest_timestamp',F.to_timestamp(F.lit('2026-10-21T00:00:00Z'))).withColumn('_source_file_path',F.lit('/x'))
        outputs=transform(self.spark,bronze)
        for df in outputs.values():self.assertEqual(df.select('location_id').first()[0],1)
    def test_empty_earthquake_feed_and_millisecond_precision(self):
        import json
        from src.transformations.schemas import earthquake_envelope
        from src.transformations.earthquakes import transform
        from src.validation.earthquakes_silver import validate_earthquakes
        from test_pipeline import feed
        from pyspark.sql import functions as F
        data={'metadata':{'source':'earthquakes','run_id':'run','retrieval_timestamp_utc':'2026-10-21T00:00:00Z','api_endpoint':'https://example.org'},'raw_response':feed()}
        def frame(data):
            return self.spark.read.schema(earthquake_envelope()).json(self.spark.sparkContext.parallelize([json.dumps(data)])).withColumn('_source_file_path',F.lit('/x')).withColumn('_ingest_timestamp',F.current_timestamp())
        outputs=transform(self.spark,frame(data))
        validate_earthquakes(outputs['latest'],'latest');self.assertEqual(outputs['latest'].count(),0)
        data['raw_response']['features']=[{'id':'event','type':'Feature','properties':{'time':1792540800123,'updated':1792540800456,'mag':1.0},'geometry':{'type':'Point','coordinates':[-90.,40.,10.]}}]
        data['raw_response']['metadata']['count']=1
        row=transform(self.spark,frame(data))['latest'].first()
        self.assertEqual(row.event_time.microsecond,123000);self.assertEqual(row.updated_at.microsecond,456000)
