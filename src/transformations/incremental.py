"""File-based durable completion ledger; commit only after every output validates.

One writer per source is required. A crash before commit replays the same files;
MERGE keys make replay idempotent. Files discovered late remain pending regardless
of their ingestion/event timestamp. No timestamp high-water mark is used.
"""
from uuid import uuid4
from src.validation.common import compatible_target

STATE_TABLE='weather_api.pipeline_state.silver_completed_files'

def ensure_state(spark):
    spark.conf.set('spark.sql.session.timeZone','UTC')
    spark.sql('CREATE SCHEMA IF NOT EXISTS weather_api.pipeline_state')
    spark.sql(f'CREATE TABLE IF NOT EXISTS {STATE_TABLE} (source STRING, path STRING, batch_id STRING, completed_at TIMESTAMP) USING DELTA')

def pending_files(spark,bronze_table,source):
    from pyspark.sql import functions as F
    ensure_state(spark)
    # Materialize an immutable file list, not a mutable timestamp boundary.
    done=spark.table(STATE_TABLE).filter(F.col('source')==source).select(F.col('path').alias('_source_file_path'))
    paths=[r[0] for r in spark.table(bronze_table).select('_source_file_path').distinct().join(done,'_source_file_path','left_anti').collect()]
    return paths

def read_scope(spark,table,paths):
    from pyspark.sql import functions as F
    return spark.table(table).filter(F.col('_source_file_path').isin(paths)).cache()

def merge_output(spark,source,table,keys,*,latest=False):
    from delta.tables import DeltaTable
    if not spark.catalog.tableExists(table):
        source.write.format('delta').saveAsTable(table)
        return
    compatible_target(source,spark.table(table))
    condition=' AND '.join(f't.{k} = s.{k}' for k in keys)
    merge=DeltaTable.forName(spark,table).alias('t').merge(source.alias('s'),condition)
    if latest:
        merge=merge.whenMatchedUpdateAll(condition='s.updated_at > t.updated_at OR (s.updated_at = t.updated_at AND s.snapshot_at >= t.snapshot_at)')
    else: merge=merge.whenMatchedUpdateAll()
    merge.whenNotMatchedInsertAll().execute()

def complete_files(spark,source,paths):
    if not paths: return
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable
    batch_id=str(uuid4())
    records=spark.createDataFrame([(source,p,batch_id) for p in paths],['source','path','batch_id']).withColumn('completed_at',F.current_timestamp())
    DeltaTable.forName(spark,STATE_TABLE).alias('t').merge(records.alias('s'),'t.source = s.source AND t.path = s.path').whenNotMatchedInsertAll().execute()

def is_newer(incoming,existing):
    """Pure equivalent of the conditional event MERGE, used in offline tests."""
    return (incoming['updated_at'],incoming['snapshot_at']) >= (existing['updated_at'],existing['snapshot_at'])
