"""Shared Weather/AQ flattening; schemas are defined in src.contracts."""
from src.contracts import CURRENT_TYPES, specs, output_types
from src.validation.common import normalize_output

def transform_measurements(spark,bronze,source):
    from pyspark.sql import functions as F
    base=[F.col('metadata.'+n).alias(n) for n in ('location_name','location_id','latitude','longitude','source','api_endpoint')]
    base += [F.col('raw_response.elevation').alias('elevation_m'), F.to_timestamp('metadata.retrieval_timestamp_utc').alias('snapshot_at'),F.col('_source_file_path').alias('source_file_path'),F.col('_ingest_timestamp').alias('ingested_at'),F.col('_rescued_data')]
    current=bronze.select(*base,F.to_timestamp('raw_response.current.time').alias('observed_at'),F.col('raw_response.current.interval').alias('interval_seconds'),*[F.col('raw_response.current.'+n).alias(n) for n in CURRENT_TYPES[source]])
    outputs={'current':normalize_output(current,output_types(source,'current'))}
    for kind in ('hourly','daily') if source=='weather' else ('hourly',):
        fields=specs(source,kind)
        exploded=bronze.select(*base,F.explode(F.arrays_zip(*[(F.col('raw_response.'+kind+'.'+n) if n=='time' else F.coalesce(F.col('raw_response.'+kind+'.'+n),F.array_repeat(F.lit(None).cast('string' if n in ('sunrise','sunset') else 'double'),F.size('raw_response.'+kind+'.time')))).alias(n) for n,_,_ in fields])).alias('values'))
        frame=exploded.select(*[F.col(c) for c in current.columns if c not in CURRENT_TYPES[source] and c not in ('observed_at','interval_seconds')],*[F.col('values.'+n).alias(out) for n,out,_ in fields])
        outputs[kind]=normalize_output(frame,output_types(source,kind))
    return outputs
