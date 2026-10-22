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
    return ValidationResult('geonames_structure',{'valid':True}).report()

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
    return ValidationResult('registry_counts',stats).report()

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
    return ValidationResult('geonames_file',{'rows':row.rows}).report()
