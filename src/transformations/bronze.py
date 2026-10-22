"""Auto Loader transport, with current-run file reconciliation after termination."""
import json
from pathlib import Path
from src.ingestion.pipeline import parameter, DEFAULT_ROOT
from src.validation.raw import validate_raw_files, read_json_file
from src.validation.bronze import validate_bronze
from src.validation.common import require

def run_bronze(spark,dbutils,source):
    from pyspark.sql import functions as F
    manifest_path=parameter(dbutils,'manifest_path','')
    require(bool(manifest_path),'Set manifest_path to a successfully completed ingestion run')
    manifest=read_json_file(manifest_path)
    validate_raw_files(manifest)
    require(source in manifest['sources'],'Source absent from run manifest')
    paths=[x['path'] for x in manifest['files'] if x['source']==source]
    spark.conf.set('spark.sql.session.timeZone','UTC')
    spark.sql('CREATE SCHEMA IF NOT EXISTS weather_api.bronze')
    # Auto Loader discovers all completed JSON files in the source root.
    # Operationally gate this task after ingestion; reconcile the explicit manifest scope.
    root=str(Path(manifest_path).parent.parent/source)
    reader=spark.readStream
    if source=='earthquakes':
        from .schemas import earthquake_envelope
        reader=reader.schema(earthquake_envelope())
    stream=(reader.format('cloudFiles').option('cloudFiles.format','json')
        .option('multiLine','true').option('pathGlobFilter','*.json')
        .option('cloudFiles.schemaLocation',f'/Volumes/weather_api/pipeline_state/autoloader_state/_schemas/{source}')
        .option('cloudFiles.schemaEvolutionMode','rescue')
        .option('rescuedDataColumn','_rescued_data').option('cloudFiles.inferColumnTypes','true')
        .load(root).withColumn('_ingest_timestamp',F.current_timestamp())
        .withColumn('_source_file_path',F.regexp_replace(F.col('_metadata.file_path'),'^(dbfs:|file:)/+','/')))
    target=f'weather_api.bronze.{source}_raw'
    (stream.writeStream.format('delta').option('checkpointLocation',f'/Volumes/weather_api/pipeline_state/autoloader_state/_checkpoints/{source}')
     .option('mergeSchema','true').trigger(availableNow=True).toTable(target).awaitTermination())
    return validate_bronze(spark.table(target),source,paths,rescued_policy='warn')
