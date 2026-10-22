"""Auto Loader reads only one committed run directory and exact file membership."""
from pathlib import Path
from src.config import load_config
from src.ingestion.pipeline import parameter
from src.validation.raw import load_committed
from src.validation.bronze import validate_bronze
from .schemas import envelope_schema
from .scope import scoped_frame

def run_bronze(spark,dbutils,source,config=None,manifest_path=None):
    from pyspark.sql import functions as F
    cfg=(config or load_config(parameter(dbutils,'config_path','') or None)).validate()
    path=manifest_path or parameter(dbutils,'manifest_path','')
    manifest=load_committed(path,cfg,source)
    spark.conf.set('spark.sql.session.timeZone','UTC')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.bronze_schema}')
    run=manifest['run_id']
    stream=(spark.readStream.schema(envelope_schema(source)).format('cloudFiles')
        .option('cloudFiles.format','json').option('multiLine','true').option('pathGlobFilter','*.json')
        .option('cloudFiles.schemaLocation',str(Path(cfg.schema_root)/source/run))
        .option('cloudFiles.schemaEvolutionMode','rescue').option('rescuedDataColumn','_rescued_data')
        .load(str(cfg.run_dir(source,run)))
        .withColumn('_ingest_timestamp',F.current_timestamp())
        .withColumn('_source_file_path',F.regexp_replace(F.col('_metadata.file_path'),'^(dbfs:|file:)/+','/')))
    # Failed/unlisted runs are never input; even stray files inside this run are
    # excluded by immutable membership. Each run has isolated checkpoint/schema state.
    eligible=scoped_frame(stream,manifest,source)
    (eligible.writeStream.format('delta').option('checkpointLocation',str(Path(cfg.checkpoint_root)/source/run))
        .trigger(availableNow=True).toTable(cfg.bronze(source)).awaitTermination())
    return validate_bronze(spark.table(cfg.bronze(source)),source,manifest,cfg,rescued_policy='warn').report()
