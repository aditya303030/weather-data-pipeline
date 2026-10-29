"""Registry boundary checks; transformations stay in location_discovery."""
from .common import check_frame, require_columns, require, reconcile, ValidationResult

FLAGS=('is_active','weather_enabled','air_quality_enabled','created_at')
def validate_activation_arguments(location_ids,limit,weather,air_quality,distributed):
    require(all(isinstance(x,bool) for x in (weather,air_quality,distributed)), 'Activation flags must be booleans')
    require(weather or air_quality,'Select at least one source to enable')
    require((location_ids is None)!=(limit is None),'Specify exactly one of location_ids or limit')
    if location_ids is not None:
        require(bool(location_ids) and all(type(i) is int and i>0 for i in location_ids),'IDs must be positive integers')
    if limit is not None: require(type(limit) is int and limit>0,'limit must be positive')

def validate_geonames_input(df):
    require_columns(df,{n:None for n in ('geonameid','name','latitude','longitude','country_code','feature_class','feature_code')})
    return ValidationResult('geonames_structure',{'valid':True})

def validate_registry(df, existing=None):
    from pyspark.sql import functions as F
    required={'location_id':('bigint',),'geonames_id':('bigint',),'location_name':('string',),'country_code':('string',)}
    required.update({f:('boolean',) for f in FLAGS[:3]})
    predicates={'country': F.col('country_code')!='US', 'stable_id':(F.col('location_id')<=0) | ~F.col('location_id').eqNullSafe(F.col('geonames_id'))}
    predicates.update({f:F.col(f).isNull() for f in FLAGS})
    result=check_frame(df,stage='registry',keys=['location_id'],required=required,ranges={'latitude':(-90,90),'longitude':(-180,180)},timestamps=['created_at','updated_at'],nonempty=True,predicates=predicates)
    require(not df.filter(F.col('location_name').isNull() | (F.trim('location_name')=='') | F.col('latitude').isNull() | F.col('longitude').isNull() | F.col('country_code').isNull() | F.col('geonames_id').isNull()).limit(1).count(),'Missing required registry values')
    if existing is not None:
        joined=df.alias('n').join(existing.alias('o'),'location_id')
        bad=None
        for f in FLAGS:
            cond=~F.col('n.'+f).eqNullSafe(F.col('o.'+f))
            bad=cond if bad is None else bad|cond
        require(not joined.filter(bad).limit(1).count(),'Registry refresh changed existing activation/creation settings')
    return result

def validate_registry_counts(stats):
    reconcile(stats['class_p_total']-stats['excluded_by_filter'],stats['after_filter'],'filter')
    reconcile(stats['after_filter']-stats['invalid_records'],stats['after_validation'],'rejections')
    reconcile(stats['after_validation']-stats['duplicates_removed'],stats['after_dedup'],'dedup')
    reconcile(stats['after_dedup'],stats['final_registry_count'],'publication')
    require(stats['source_total']>=stats['class_p_total'],'Invalid input reconciliation')
    return ValidationResult('registry_counts',stats)

def valid_geonames_predicate():
    from pyspark.sql import functions as F
    c=F.col
    return (c('geonameid').isNotNull() & (c('geonameid')>0) & c('name').isNotNull() & (F.trim(c('name'))!='') & c('latitude').between(-90,90) & c('longitude').between(-180,180) & (c('country_code')=='US'))

def validate_merge_input(locations):
    from .common import numeric
    require(bool(locations),'Empty registry input rejected')
    for loc in locations:
        require(type(loc.get('location_id')) is int and loc['location_id']>0,'Stable location_id required; migrate historical null IDs explicitly')
        require(isinstance(loc.get('location_name'),str) and bool(loc['location_name'].strip()),'Missing location name')
        require(loc.get('country_code')=='US','Expected US country')
        numeric(loc.get('latitude'),-90,90,nullable=False)
        numeric(loc.get('longitude'),-180,180,nullable=False)

def validate_geonames_file(spark,file_path):
    """Inspect token counts before CSV parsing can truncate extra fields."""
    from pyspark.sql import functions as F
    source=spark.read.text(file_path)
    row=source.agg(F.count('*').alias('rows'),F.sum(F.when(F.size(F.split('value','\t',-1))!=19,1).otherwise(0)).alias('malformed')).first()
    require(row.rows>0,'Empty GeoNames source')
    require(not row.malformed,'GeoNames input must contain exactly 19 tab-separated fields per row')
    return ValidationResult('geonames_file',{'rows':row.rows})

def validate_selection_input(locations,limit,slot):
    from .common import numeric
    require(type(limit) is int and limit>0 and type(slot) is int and slot>=0,'Positive limit and nonnegative slot required',stage='registry',check='selection_arguments')
    ids=[]
    for loc in locations:
        require(isinstance(loc,dict) and type(loc.get('location_id')) is int and loc['location_id']>0,'Invalid stable selection ID',stage='registry')
        require(isinstance(loc.get('name'),str) and bool(loc['name'].strip()),'Missing selection name',stage='registry')
        numeric(loc.get('latitude'),-90,90,nullable=False);numeric(loc.get('longitude'),-180,180,nullable=False)
        ids.append(loc['location_id'])
    require(len(ids)==len(set(ids)),'Duplicate selection identities',stage='registry')

def validate_selection_completeness(selected_ids,enabled_ids):
    """Selection must cover every enabled registry location exactly.

    Rotation subsetting across runs is forbidden for ingestion: a run that would
    miss enabled identities, repeat identities, or include disabled/unknown
    identities fails here instead of publishing an apparently complete run.
    This also guarantees weather and air_quality process the same enabled set.
    """
    require(bool(selected_ids) and bool(enabled_ids),'Selection requires enabled registry locations',stage='registry',check='selection_completeness')
    require(len(selected_ids)==len(set(selected_ids)),'Duplicate selection identities',stage='registry',check='selection_completeness')
    missing=sorted(set(enabled_ids)-set(selected_ids));unexpected=sorted(set(selected_ids)-set(enabled_ids))
    require(not missing and not unexpected,
            'Selection must cover every enabled registry location exactly; refusing rotation subsetting. missing (sample): '+str(missing[:10])+' unexpected (sample): '+str(unexpected[:10]),
            stage='registry',check='selection_completeness')

def validate_activation_selection(registry,ids):
    from pyspark.sql import functions as F
    require(bool(ids),'No active IDs selected',stage='registry')
    requested=registry.sparkSession.createDataFrame([(i,) for i in sorted(set(ids))], 'location_id long')
    matches=requested.join(registry.select('location_id','is_active'),'location_id','left')
    bad=matches.filter(F.col('is_active').isNull() | ~F.col('is_active')).limit(20).collect()
    require(not bad,'Requested activation IDs are missing/inactive (sample): '+str([r.location_id for r in bad]),stage='registry',check='activation_membership')

def validate_recovery_registry(spark,plan,config):
    """Revalidate current registry for missing requests before an explicit HTTP resume."""
    from pathlib import Path
    pending=[e for e in plan['requests'] if e['source']!='earthquakes' and not Path(e['path']).exists()]
    if not pending:return
    require(spark is not None,'Spark registry validation required before fetching missing historical requests',stage='recovery')
    from pyspark.sql import functions as F
    for source in sorted({e['source'] for e in pending}):
        entries=[e['location'] for e in pending if e['source']==source]
        wanted=spark.createDataFrame([(e['location_id'],e['name'],e['latitude'],e['longitude']) for e in entries],'location_id long, _wanted_name string, _wanted_lat double, _wanted_lon double')
        joined=wanted.join(spark.table(config.registry),'location_id','left')
        validate_registry(joined.select(*spark.table(config.registry).columns))
        mismatch=(~F.col('is_active').eqNullSafe(F.lit(True)) | ~F.col(source+'_enabled').eqNullSafe(F.lit(True)) | ~F.col('location_name').eqNullSafe(F.col('_wanted_name')) | ~F.col('latitude').eqNullSafe(F.col('_wanted_lat')) | ~F.col('longitude').eqNullSafe(F.col('_wanted_lon')))
        require(not joined.filter(mismatch).limit(1).count(),'Recovery registry differs from saved request plan or is no longer enabled; review explicitly',stage='recovery',check='registry_membership',context={'source':source,'run_id':plan['run_id']})
