"""Fault injection verifies orchestration, not Delta/Auto Loader semantics."""
import tempfile
import unittest
from unittest.mock import Mock,patch
from helpers import config_at
from src.transformations.silver import run_silver
from src.validation.common import ValidationError

class IncrementalTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);self.cfg=config_at(t.name)
    def scenario(self):
        bronze=Mock();frames={k:Mock(name=k) for k in ('current','hourly','daily')}
        for f in frames.values():f.cache.return_value=f
        transform=Mock();transform.transform.return_value=frames
        validators=Mock();validators.KEYS={k:['location_id','snapshot_at'] for k in frames}
        validators.source_context.return_value={}
        return bronze,frames,lambda name:validators if 'validation' in name else transform
    def patches(self,bronze,module,pending,write,complete,preflight=None):
        from contextlib import ExitStack
        stack=ExitStack()
        for name,options in {
            'load_committed':{'return_value':{'run_id':'run','sources':['weather']}},
            'read_scope':{'return_value':bronze},'validate_bronze':{},
            'import_module':{'side_effect':module},'pending_files':{'side_effect':pending},
            'merge_output':{'side_effect':write},'complete_files':{'side_effect':complete},
            'preflight_outputs':{'side_effect':preflight},
        }.items():stack.enter_context(patch('src.transformations.silver.'+name,**options))
        return stack
    def test_partial_write_retries_immutable_scope_and_commits_last(self):
        bronze,frames,module=self.scenario();writes=[];completed=[];targets={}
        def pending(*args):
            df=Mock();df.limit.return_value.count.return_value=0 if completed else 1;return df
        def write(spark,df,table,keys,**kwargs):
            writes.append(table)
            if len(writes)==2:raise RuntimeError('partial failure')
            targets[table]=df
        def complete(*args):completed.append(args[2]['run_id'])
        with self.patches(bronze,module,pending,write,complete):
            spark=Mock()
            with self.assertRaises(RuntimeError):run_silver(spark,'weather','/manifest',self.cfg)
            self.assertEqual(completed,[])
            run_silver(spark,'weather','/manifest',self.cfg)
            self.assertEqual(len(targets),3);self.assertEqual(completed,['run'])
            before=len(writes);run_silver(spark,'weather','/manifest',self.cfg)
            self.assertEqual(len(writes),before)
            bronze.unpersist.assert_called()
    def test_later_target_mismatch_prevents_all_writes(self):
        bronze,frames,module=self.scenario();write=Mock();complete=Mock()
        def preflight(spark,outputs,targets,keys):
            self.assertEqual(len(outputs),3)
            raise ValidationError('daily target schema mismatch')
        with self.patches(bronze,module,Mock(),write,complete,preflight):
            with self.assertRaises(ValidationError):run_silver(Mock(),'weather','/manifest',self.cfg)
        write.assert_not_called();complete.assert_not_called()
    def test_source_validation_and_postwrite_failure_block_completion(self):
        for point in ('validate_weather','validate_weather_target'):
            bronze,frames,module=self.scenario();getattr(module('validation'),point).side_effect=ValidationError('bad')
            complete=Mock();write=Mock()
            with self.patches(bronze,module,Mock(),write,complete):
                with self.assertRaises(ValidationError):run_silver(Mock(),'weather','/manifest',self.cfg)
            complete.assert_not_called()
            if point=='validate_weather':write.assert_not_called()
    def test_validate_only_has_no_persistent_writes(self):
        bronze,frames,module=self.scenario();write=Mock();complete=Mock();spark=Mock();spark.catalog.tableExists.return_value=False
        pending=Mock();pending.return_value.limit.return_value.count.return_value=1
        with self.patches(bronze,module,pending,write,complete):run_silver(spark,'weather','/manifest',self.cfg,validate_only=True)
        spark.sql.assert_not_called();write.assert_not_called();complete.assert_not_called()
    def test_uncommitted_run_stops_before_read_or_write(self):
        spark=Mock()
        with patch('src.transformations.silver.load_committed',side_effect=ValidationError('uncommitted')):
            with self.assertRaises(ValidationError):run_silver(spark,'weather','/failed',self.cfg)
        spark.sql.assert_not_called();spark.table.assert_not_called()

    def test_standalone_persistence_mode_rejects_missing_targets(self):
        bronze,frames,module=self.scenario();spark=Mock();spark.catalog.tableExists.return_value=False
        write=Mock();complete=Mock()
        with self.patches(bronze,module,Mock(),write,complete):
            with self.assertRaisesRegex(ValidationError,'Missing required persisted'):
                run_silver(spark,'weather','/manifest',self.cfg,validate_only=True,require_targets=True)
        spark.sql.assert_not_called();write.assert_not_called();complete.assert_not_called()
