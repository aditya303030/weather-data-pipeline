"""Validate exact committed membership; optional arrays remain legitimately nullable."""
from src.contracts import variables
from .common import check_frame, require_columns, require

def validate_bronze(df,source,manifest,config=None,*,rescued_policy='fail'):
    from pyspark.sql import functions as F
    from src.config import load_config
    from src.validation.raw import validate_manifest
    from src.transformations.scope import scoped_frame
    cfg=config or load_config();validate_manifest(manifest,cfg)
    require(source in manifest['sources'] and rescued_policy in ('fail','warn'),'Invalid Bronze source/policy',stage='bronze')
    scope=scoped_frame(df,manifest,source,with_expectations=True)
    required={'metadata.source':('string',),'metadata.run_id':('string',),'metadata.retrieval_timestamp_utc':('string',),'raw_response':None,'_source_file_path':('string',),'_ingest_timestamp':('timestamp',),'_rescued_data':('string',)}
    predicates={'source':~F.col('metadata.source').eqNullSafe(F.col('_expected_source')),'run_id':~F.col('metadata.run_id').eqNullSafe(F.col('_expected_run')),'retrieval_time':F.expr('try_cast(metadata.retrieval_timestamp_utc as timestamp)').isNull()}
    if source!='earthquakes':
        required.update({'metadata.location_id':('bigint',),'raw_response.current.time':('string',),'raw_response.elevation':('double',),'raw_response.current.interval':('bigint',)})
        predicates['identity']=~F.col('metadata.location_id').eqNullSafe(F.col('_expected_id'))
        predicates['current_time']=F.expr('try_cast(raw_response.current.time as timestamp)').isNull()
        for section in ('hourly','daily') if source=='weather' else ('hourly',):
            time='raw_response.'+section+'.time'
            require_columns(df,{time:('array<string>',)})
            predicates[section+':time']=F.col(time).isNull() | (F.size(time)==0)
            predicates[section+':timestamps']=F.exists(F.col(time),lambda x:x.isNull()) # parsed timestamps are checked in transformed output
            for name in variables(source,section):
                field='raw_response.'+section+'.'+name
                require_columns(df,{field:None})
                predicates[field+':length']=F.col(field).isNotNull() & (F.size(field)!=F.size(time))
    else:
        required.update({'raw_response.features':None,'raw_response.metadata.generated':('bigint',),'raw_response.metadata.count':('bigint',)})
        predicates.update({'feed_type':~F.col('raw_response.type').eqNullSafe(F.lit('FeatureCollection')),'features':F.col('raw_response.features').isNull(),'generated':F.col('raw_response.metadata.generated').isNull(),'feed_count':~F.col('raw_response.metadata.count').eqNullSafe(F.size('raw_response.features').cast('long'))})
    if rescued_policy=='fail':predicates['rescued']=F.col('_rescued_data').isNotNull()
    result=check_frame(scope,stage='bronze',keys=['_source_file_path'],required=required,timestamps=['_ingest_timestamp'],predicates=predicates)
    expected=sum(e['source']==source for e in manifest['files'])
    require(result.checks['rows']==expected,'Expected committed files missing from Bronze; historical schemas/checkpoints require explicit migration',stage='bronze',check='file_reconciliation',context={'run_id':manifest['run_id'],'source':source})
    if rescued_policy=='warn' and scope.filter(F.col('_rescued_data').isNotNull()).limit(1).count():result.warnings.append('Rescued data preserved; Silver blocks until explicit remediation')
    result.context.update(run_id=manifest['run_id'],source=source)
    return result
