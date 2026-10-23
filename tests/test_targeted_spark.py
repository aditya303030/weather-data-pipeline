"""Same Spark Connect-compatible cases used by the separate Databricks runner."""
import importlib.util
import unittest
from src.integration.cases import earthquake_content_cases,metric_cases,domain_cases,spark_timestamp_cases

@unittest.skipUnless(importlib.util.find_spec('pyspark'),'Local PySpark unavailable')
class TargetedSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark=SparkSession.builder.master('local[1]').appName('targeted-regressions').getOrCreate()
        if not cls.spark.sparkContext.master.startswith('local'):raise unittest.SkipTest('Use separate Databricks runner')
        cls.spark.conf.set('spark.sql.session.timeZone','UTC')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def test_earthquake_version_content(self):earthquake_content_cases(self.spark)
    def test_distinct_affected_rows(self):metric_cases(self.spark)
    def test_aqi_daily_and_completeness(self):domain_cases(self.spark)
    def test_actual_spark_timestamp_parsing(self):spark_timestamp_cases(self.spark)
