"""Pure contract regressions; all inputs synthetic, no services."""
import unittest
from datetime import datetime,timezone
from src.contracts import AQI_FIELDS,DAILY_RELATIONSHIPS
from src.validation.common import timestamp,ValidationError
from src.validation.ingestion import validate_response
from helpers import payload

class TimestampTests(unittest.TestCase):
    def test_supported_formats_and_normalization(self):
        expected=timestamp('2026-10-21T00:00Z')
        for value in ('2026-10-21T00:00','2026-10-21T00:00:00Z','2026-10-21T00:00:00+00:00','2026-10-21T05:30:00+05:30'):
            with self.subTest(value=value):self.assertEqual(timestamp(value),expected)
        self.assertEqual(timestamp('2026-10-21',kind='date').isoformat(),'2026-10-21')
        self.assertEqual(timestamp('2026-10-21T00:00:00.123456Z').microsecond,123456)
        timestamp(datetime.now(timezone.utc).isoformat(),offset_required=True)
    def test_invalid_formats_and_values(self):
        for value in (None,'','20261021T000000','2026-02-30T00:00','2026-10-21T25:00','2026-10-21T00:00+19:00','2026-10-21T00:00+00:99','2026-10-21T00:00:00.1234567Z'):
            with self.subTest(value=value),self.assertRaises(ValidationError):timestamp(value)
        with self.assertRaises(ValidationError):timestamp('2026-10-21T00:00',offset_required=True)
        with self.assertRaises(ValidationError):timestamp('20261021',kind='date')
    def test_equivalent_instants_block_before_commit(self):
        for other in ('2026-10-21T00:00:00+00:00','2026-10-21T05:30:00+05:30','2026-10-21T00:00'):
            data=payload();data['hourly']={'time':['2026-10-21T00:00:00Z',other]}
            with self.subTest(other=other),self.assertRaisesRegex(ValidationError,'Duplicate normalized'):validate_response('weather',data)
        data=payload();data['daily']={'time':['2026-10-21','2026-10-21']}
        with self.assertRaises(ValidationError):validate_response('weather',data)
    def test_optional_sunrise_and_daily_date_policy(self):
        data=payload();data['daily']['sunrise']=['2026-10-21T06:00:00.123Z'];data['daily']['sunset']=[None]
        validate_response('weather',data)
        data['daily']['time']=['2026-10-21T00:00Z']
        with self.assertRaises(ValidationError):validate_response('weather',data)

class DomainTests(unittest.TestCase):
    def test_all_aqi_components_current_and_hourly(self):
        components=[n for n in AQI_FIELDS if n.startswith('us_aqi_')]
        self.assertEqual(len(components),6)
        for field in components:
            for section in ('current','hourly'):
                for value in (-1,0,25,None):
                    data=payload('air_quality');data[section][field]=value if section=='current' else [value]
                    with self.subTest(field=field,section=section,value=value):
                        if value==-1:
                            with self.assertRaises(ValidationError):validate_response('air_quality',data)
                        else:validate_response('air_quality',data)
    def test_daily_relationships(self):
        for low,high in DAILY_RELATIONSHIPS:
            for a,b in ((10,30),(10,10),(30,10),(None,10),(10,None),(None,None)):
                data=payload();data['daily'][low]=[a];data['daily'][high]=[b]
                with self.subTest(low=low,a=a,b=b):
                    if a is not None and b is not None and a>b:
                        with self.assertRaisesRegex(ValidationError,low+'_le_'+high):validate_response('weather',data)
                    else:validate_response('weather',data)
        data=payload();data['daily']={'time':['2026-10-21','2026-10-22'],'temperature_2m_min':[5,30],'temperature_2m_max':[10,10]}
        with self.assertRaises(ValidationError):validate_response('weather',data)

class IntegrationSafetyTests(unittest.TestCase):
    def test_runner_requires_explicit_config_and_refuses_production(self):
        from src.integration.runner import run_regressions
        from src.config import PipelineConfig
        from dataclasses import asdict
        from unittest.mock import Mock
        import tempfile,json
        from pathlib import Path
        spark=Mock()
        with self.assertRaises(ValidationError):run_regressions(spark,'')
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'production.json';path.write_text(json.dumps(asdict(PipelineConfig())))
            with self.assertRaises(ValidationError):run_regressions(spark,str(path))
        spark.sql.assert_not_called();spark.catalog.tableExists.assert_not_called()
    def test_unknown_repair_modes_rejected_without_processing(self):
        from src.transformations.silver import run_silver
        from src.transformations.recovery import recover_committed
        from unittest.mock import Mock
        spark=Mock()
        with self.assertRaises(ValidationError):run_silver(spark,'weather','missing',repair='false')
        with self.assertRaises(ValidationError):recover_committed(spark,['missing'],'weather',Mock(),repair=True,validate_only=True)
        spark.sql.assert_not_called();spark.table.assert_not_called()
