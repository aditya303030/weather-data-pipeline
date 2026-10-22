"""Coordinate scoped transforms, validation, idempotent writes and completion."""
from importlib import import_module
from .incremental import pending_files, read_scope, merge_output, complete_files, ensure_state
from src.validation.bronze import validate_bronze
from src.validation.common import require, ValidationResult

TABLES={'weather':{'current':'weather_current','hourly':'weather_hourly','daily':'weather_daily'},'air_quality':{'current':'air_quality_current','hourly':'air_quality_hourly'},'earthquakes':{'latest':'earthquakes','history':'earthquakes_event_history'}}

def run_silver(spark,source,paths=None,*,commit=True):
    require(source in TABLES,'Unknown Silver source')
    spark.conf.set('spark.sql.session.timeZone','UTC')
    spark.sql('CREATE SCHEMA IF NOT EXISTS weather_api.silver')
    table=f'weather_api.bronze.{source}_raw'
    if paths is not None and commit: ensure_state(spark)
    scope=pending_files(spark,table,source) if paths is None else paths
    if not scope:
        return ValidationResult('silver:'+source,{'pending_files':0}).report()
    bronze=read_scope(spark,table,scope)
    outputs={}
    try:
        validate_bronze(bronze,source,scope)
        outputs=import_module('src.transformations.'+source).transform(spark,bronze)
        outputs={k:v.cache() for k,v in outputs.items()}
        validators=import_module('src.validation.'+source+'_silver')
        if source=='earthquakes':
            context=validators.source_context(bronze,outputs['history'])
            for kind,df in outputs.items(): validators.validate_earthquakes(df,kind,context)
            validators.validate_latest_selection(outputs['history'],outputs['latest'])
        else:
            context=validators.source_context(bronze)
            for kind,df in outputs.items():
                getattr(validators,'validate_'+source)(df,kind,context=context)
        # Every source output passes before the first table is written.
        for kind,df in outputs.items():
            keys=(['event_id'] if kind=='latest' else ['event_id','snapshot_at']) if source=='earthquakes' else validators.KEYS[kind]
            target='weather_api.silver.'+TABLES[source][kind]
            merge_output(spark,df,target,keys,latest=(source=='earthquakes' and kind=='latest'))
            if source=='earthquakes': validators.validate_earthquake_target(df,spark.table(target),kind)
            else: getattr(validators,'validate_'+('weather' if source=='weather' else 'air_quality')+'_target')(df,spark.table(target),kind)
        if commit: complete_files(spark,source,scope)
        return ValidationResult('silver:'+source,{'processed_files':len(scope),'committed':commit}).report()
    finally:
        for df in outputs.values(): df.unpersist()
        bronze.unpersist()
