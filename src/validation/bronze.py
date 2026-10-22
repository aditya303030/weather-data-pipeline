"""Validate Bronze within explicit file scope, including checkpointed reruns."""
from .common import check_frame, require_columns, require, ValidationResult

def validate_bronze(df, source, paths, *, rescued_policy='fail'):
    from pyspark.sql import functions as F
    require(paths is not None,'Explicit raw file scope required')
    require(rescued_policy in ('fail','warn'),'Unknown rescued-data policy')
    require(len(paths)==len(set(paths)),'Duplicate expected paths')
    required={'metadata.source':('string',),'metadata.run_id':('string',),'metadata.retrieval_timestamp_utc':('string',),'raw_response':None,'_source_file_path':('string',),'_ingest_timestamp':('timestamp',),'_rescued_data':('string',)}
    if source!='earthquakes': required.update({'metadata.location_id':('bigint','int'),'metadata.location_name':('string',),'raw_response.current.time':('string',),'raw_response.hourly.time':None})
    else:
        required.update({'raw_response.features':None,'raw_response.metadata.generated':('bigint',),'raw_response.metadata.count':('bigint',),'raw_response.type':('string',)})
    require_columns(df,required)
    scope=df.filter(F.col('_source_file_path').isin(paths))
    predicates={'source':~F.col('metadata.source').eqNullSafe(F.lit(source)), 'run_id': F.col('metadata.run_id').isNull(), 'retrieval_timestamp':F.to_timestamp('metadata.retrieval_timestamp_utc').isNull()}
    if source!='earthquakes': predicates['identity']=F.col('metadata.location_id').isNull() | F.col('metadata.location_name').isNull()
    if source=='earthquakes':
        predicates.update({'feed_type':~F.col('raw_response.type').eqNullSafe(F.lit('FeatureCollection')),'features':F.col('raw_response.features').isNull(),'generated':F.col('raw_response.metadata.generated').isNull(),'feed_count':~F.col('raw_response.metadata.count').eqNullSafe(F.size('raw_response.features').cast('long'))})
    if rescued_policy=='fail': predicates['rescued']=F.col('_rescued_data').isNotNull()
    if source!='earthquakes':
        from importlib import import_module
        module=import_module('src.ingestion.'+source+'_api')
        for name in getattr(module,('WEATHER_' if source=='weather' else 'AQ_')+'CURRENT_VARS').split(','):
            require_columns(df,{'raw_response.current.'+name:None})
        predicates['current_timestamp']=F.to_timestamp('raw_response.current.time').isNull()
        for section in ('hourly','daily') if source=='weather' else ('hourly',):
            time_field=f'raw_response.{section}.time'
            require_columns(df,{time_field:None})
            predicates[section+':time']=F.col(time_field).isNull() | (F.size(time_field)==0)
            variable_name=('WEATHER_' if source=='weather' else 'AQ_')+section.upper()+'_VARS'
            for name in getattr(module,variable_name).split(','):
                field=f'raw_response.{section}.{name}'
                require_columns(df,{field:None})
                predicates[field+':length']=F.col(field).isNull() | (F.size(field)!=F.size(f'raw_response.{section}.time'))
    result=check_frame(scope,stage='bronze:'+source,keys=['_source_file_path'],required=required,timestamps=['_ingest_timestamp'],predicates=predicates)
    actual={r[0] for r in scope.select('_source_file_path').distinct().collect()}
    require(actual==set(paths),'Expected raw paths missing from Bronze (already checkpointed files must still exist in target)')
    if rescued_policy=='warn' and scope.filter(F.col('_rescued_data').isNotNull()).limit(1).count(): result.warnings.append('Rescued fields preserved; not promoted. Silver rejects this scope until explicit remediation.')
    return result.report()
