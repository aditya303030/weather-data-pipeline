"""Manifest-bounded idempotent writes. One active writer initially; no lock guarantee."""
from uuid import uuid4
from src.validation.common import compatible_target, require, check_frame, require_columns
from src.config import load_config
from .scope import membership, scoped_frame

LATEST_UPDATE_CONDITION='s.updated_at > t.updated_at OR (s.updated_at = t.updated_at AND s.snapshot_at >= t.snapshot_at)'

def ensure_state(spark,config=None):
    cfg=(config or load_config()).validate()
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.state_schema}')
    spark.sql(f'CREATE TABLE IF NOT EXISTS {cfg.ledger} (source STRING, path STRING, run_id STRING, batch_id STRING, completed_at TIMESTAMP) USING DELTA')

def read_scope(spark,table,manifest,source):
    return scoped_frame(spark.table(table),manifest,source).cache()

def pending_files(spark,manifest,source,config=None):
    """No historical discovery or giant Python path backlog. Only this manifest."""
    cfg=config or load_config();wanted=membership(spark,manifest,source).select('_source_file_path')
    if not spark.catalog.tableExists(cfg.ledger):return wanted
    from pyspark.sql import functions as F
    require_columns(spark.table(cfg.ledger),{'source':('string',),'path':('string',),'run_id':('string',),'batch_id':('string',),'completed_at':('timestamp',)})
    from delta.tables import DeltaTable
    DeltaTable.forName(spark,cfg.ledger)
    done=spark.table(cfg.ledger).filter(F.col('source')==source).select(F.col('path').alias('_source_file_path'),'run_id')
    check_frame(done.join(wanted,'_source_file_path'),stage='state',keys=['_source_file_path'])
    require(not done.join(wanted,'_source_file_path').filter(~F.col('run_id').eqNullSafe(F.lit(manifest['run_id']))).limit(1).count(),'Conflicting processing state for manifest paths',stage='state')
    return wanted.join(done.select('_source_file_path'),'_source_file_path','left_anti')

def preflight_outputs(spark,outputs,targets,keys):
    """Check EVERY existing target before the first write, including keyed uniqueness."""
    for kind,df in outputs.items():
        target=targets[kind]
        if spark.catalog.tableExists(target):
            existing=spark.table(target)
            compatible_target(df,existing)
            relevant=existing.join(df.select(*keys[kind]).distinct(),keys[kind],'inner')
            check_frame(relevant,stage='target_preflight',keys=keys[kind])
            # Views/non-Delta tables must fail preflight rather than during a later write.
            from delta.tables import DeltaTable
            DeltaTable.forName(spark,target)

def merge_output(spark,source,table,keys,*,latest=False):
    from delta.tables import DeltaTable
    if not spark.catalog.tableExists(table):
        source.write.format('delta').saveAsTable(table)
        return
    condition=' AND '.join(f't.{k} = s.{k}' for k in keys)
    merge=DeltaTable.forName(spark,table).alias('t').merge(source.alias('s'),condition)
    merge=merge.whenMatchedUpdateAll(condition=LATEST_UPDATE_CONDITION) if latest else merge.whenMatchedUpdateAll()
    merge.whenNotMatchedInsertAll().execute()

def complete_files(spark,source,manifest,config=None):
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable
    cfg=config or load_config();ensure_state(spark,cfg)
    records=membership(spark,manifest,source).select(F.lit(source).alias('source'),F.col('_source_file_path').alias('path'),F.lit(manifest['run_id']).alias('run_id'),F.lit(str(uuid4())).alias('batch_id'),F.current_timestamp().alias('completed_at'))
    DeltaTable.forName(spark,cfg.ledger).alias('t').merge(records.alias('s'),'t.source = s.source AND t.path = s.path').whenNotMatchedInsertAll().execute()
