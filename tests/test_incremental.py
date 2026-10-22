"""Fault-injection tests of the actual Silver orchestration, using fake frames."""
import unittest
from unittest.mock import Mock,patch
from src.transformations.silver import run_silver
from src.validation.common import ValidationError

class IncrementalTests(unittest.TestCase):
    def scenario(self):
        bronze=Mock();frames={k:Mock(name=k) for k in ('current','hourly','daily')}
        for f in frames.values():f.cache.return_value=f
        transform=Mock();transform.transform.return_value=frames
        validators=Mock();validators.KEYS={k:['location_id','snapshot_at'] for k in frames}
        validators.source_context.return_value={}
        def module(name):return validators if 'validation' in name else transform
        return bronze,frames,module
    def test_partial_write_does_not_commit_and_retry_is_same_scope(self):
        bronze,frames,module=self.scenario();written=[];completed=[]
        # Simulate durable, idempotent keyed tables and a completion ledger.
        targets={}
        def write(spark,df,table,keys,**kw):
            written.append(table)
            if len(written)==2:raise RuntimeError('injected partial failure')
            targets[table]=df
        def commit(spark,source,paths):completed.extend(paths)
        with patch('src.transformations.silver.pending_files',side_effect=lambda *args:[] if completed else ['late-file','file']),patch('src.transformations.silver.read_scope',return_value=bronze),patch('src.transformations.silver.validate_bronze'),patch('src.transformations.silver.import_module',side_effect=module),patch('src.transformations.silver.merge_output',side_effect=write),patch('src.transformations.silver.complete_files',side_effect=commit):
            spark=Mock()
            with self.assertRaises(RuntimeError):run_silver(spark,'weather')
            self.assertEqual(completed,[])
            run_silver(spark,'weather')
            self.assertEqual(completed,['late-file','file'])
            self.assertEqual(len(targets),3)
            before=len(written);run_silver(spark,'weather');self.assertEqual(len(written),before)
            bronze.unpersist.assert_called()
    def test_validation_failure_prevents_any_write(self):
        bronze,frames,module=self.scenario()
        validators=module('validation');validators.validate_weather.side_effect=ValidationError('bad keys')
        with patch('src.transformations.silver.pending_files',return_value=['f']),patch('src.transformations.silver.read_scope',return_value=bronze),patch('src.transformations.silver.validate_bronze'),patch('src.transformations.silver.import_module',side_effect=module),patch('src.transformations.silver.merge_output') as write,patch('src.transformations.silver.complete_files') as commit:
            with self.assertRaises(ValidationError):run_silver(Mock(),'weather')
            write.assert_not_called();commit.assert_not_called()
    def test_postwrite_failure_prevents_completion(self):
        bronze,frames,module=self.scenario()
        module('validation').validate_weather_target.side_effect=ValidationError('missing target')
        with patch('src.transformations.silver.pending_files',return_value=['f']),patch('src.transformations.silver.read_scope',return_value=bronze),patch('src.transformations.silver.validate_bronze'),patch('src.transformations.silver.import_module',side_effect=module),patch('src.transformations.silver.merge_output'),patch('src.transformations.silver.complete_files') as commit:
            with self.assertRaises(ValidationError):run_silver(Mock(),'weather')
            commit.assert_not_called()
