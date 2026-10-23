"""Air quality permits regional/seasonal null measurements, never null keys."""
from src.contracts import AQI_FIELDS
from .common import check_frame, reconcile, scoped_target
KEYS={'current':['location_id','snapshot_at'],'hourly':['location_id','snapshot_at','forecast_for']}

def validate_air_quality(df,kind,*,context=None):
    from pyspark.sql import functions as F
    ranges={k:(0,None) for k in ('pm2_5','pm10','carbon_monoxide','nitrogen_dioxide','sulphur_dioxide','ozone','dust','us_aqi','european_aqi','ammonia','alder_pollen','birch_pollen','grass_pollen','mugwort_pollen','olive_pollen','ragweed_pollen','uv_index','uv_index_clear_sky','aerosol_optical_depth') if k in df.columns}
    ranges.update({n:(0,None) for n in AQI_FIELDS if n in df.columns})
    ranges.update({'latitude':(-90,90),'longitude':(-180,180)})
    result=check_frame(df,stage='air_quality_'+kind,keys=KEYS[kind],required={'location_id':('bigint','int'),'snapshot_at':('timestamp',),'pm2_5':('double','float'),'us_aqi':('int','bigint')},ranges=ranges,timestamps=['snapshot_at','observed_at' if kind=='current' else 'forecast_for'],predicates={'rescued':F.col('_rescued_data').isNotNull(),'identity':F.col('location_id')<=0,'coordinates':F.col('latitude').isNull() | F.col('longitude').isNull()})
    if context and 'expected_'+kind in context: reconcile(result.checks['rows'],context['expected_'+kind],'air quality '+kind)
    return result

def validate_air_quality_current(df,context=None): return validate_air_quality(df,'current',context=context)
def validate_air_quality_hourly(df,context=None): return validate_air_quality(df,'hourly',context=context)

def source_context(bronze):
    from pyspark.sql import functions as F
    r=bronze.agg(F.count('*').alias('n'),F.sum(F.size('raw_response.hourly.time')).alias('h')).first()
    return {'expected_current':r.n,'expected_hourly':r.h or 0}

def validate_air_quality_target(source,target,kind): return scoped_target(source,target,KEYS[kind])
