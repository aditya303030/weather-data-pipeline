"""Weather source and scoped persisted-output validation."""
from src.contracts import DAILY_RELATIONSHIPS
from .common import check_frame, reconcile, scoped_target

KEYS={'current':['location_id','snapshot_at'],'hourly':['location_id','snapshot_at','forecast_for'],'daily':['location_id','snapshot_at','forecast_date']}

def validate_weather(df, kind, *, context=None):
    from pyspark.sql import functions as F
    ts=['snapshot_at']+(['observed_at'] if kind=='current' else (['forecast_for'] if kind=='hourly' else []))
    required={'location_id':('bigint','int'),'snapshot_at':('timestamp',),'latitude':('double','float'),'longitude':('double','float')}
    if kind=='daily': required['forecast_date']=('date',)
    ranges={'latitude':(-90,90),'longitude':(-180,180)}
    specs={'relative_humidity_2m':(0,100),'cloud_cover':(0,100),'temperature_2m':(-150,100),'temperature_2m_max':(-150,100),'temperature_2m_min':(-150,100),'precipitation':(0,None),'precipitation_sum':(0,None),'wind_speed_10m':(0,None),'rain':(0,None),'rain_sum':(0,None),'snowfall':(0,None),'snowfall_sum':(0,None),'snow_depth':(0,None),'wind_gusts_10m':(0,None),'wind_speed_10m_max':(0,None),'wind_gusts_10m_max':(0,None),'precipitation_probability_max':(0,100),'wind_direction_10m':(0,360),'wind_direction_10m_dominant':(0,360)}
    ranges.update({k:v for k,v in specs.items() if k in df.columns})
    relationships={low+'_le_'+high:F.col(low).isNotNull() & F.col(high).isNotNull() & (F.col(low)>F.col(high)) for low,high in DAILY_RELATIONSHIPS} if kind=='daily' else {}
    result=check_frame(df,stage='weather_'+kind,keys=KEYS[kind],required=required,ranges=ranges,timestamps=ts,predicates={**relationships,'rescued':F.col('_rescued_data').isNotNull(),'identity':F.col('location_id')<=0,'coordinates':F.col('latitude').isNull() | F.col('longitude').isNull()})
    if context and 'expected_'+kind in context: reconcile(result.checks['rows'],context['expected_'+kind],'weather '+kind)
    return result

def validate_weather_current(df,context=None): return validate_weather(df,'current',context=context)
def validate_weather_hourly(df,context=None): return validate_weather(df,'hourly',context=context)
def validate_weather_daily(df,context=None): return validate_weather(df,'daily',context=context)

def source_context(bronze):
    from pyspark.sql import functions as F
    r=bronze.agg(F.count('*').alias('n'),F.sum(F.size('raw_response.hourly.time')).alias('h'),F.sum(F.size('raw_response.daily.time')).alias('d')).first()
    return {'expected_current':r.n,'expected_hourly':r.h or 0,'expected_daily':r.d or 0}

def validate_weather_target(source,target,kind): return scoped_target(source,target,KEYS[kind])
