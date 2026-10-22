"""Single source contract for API variables, Bronze schema and stable Silver types.

Measurements, elevation and current.interval are optional/nullable. Structural
sections, identity, time arrays and earthquake epochs are mandatory. Raw JSON is
never filled/rewritten; explicit Bronze schemas supply missing optional nulls.
"""
WEATHER_CURRENT_VARS = 'temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m'
WEATHER_HOURLY_SPECS = [('time', 'forecast_for', 'timestamp'), ('temperature_2m', 'temperature_2m', 'double'), ('relative_humidity_2m', 'relative_humidity_2m', 'int'), ('apparent_temperature', 'apparent_temperature', 'double'), ('precipitation', 'precipitation', 'double'), ('rain', 'rain', 'double'), ('snowfall', 'snowfall', 'double'), ('snow_depth', 'snow_depth', 'double'), ('weather_code', 'weather_code', 'int'), ('pressure_msl', 'pressure_msl', 'double'), ('surface_pressure', 'surface_pressure', 'double'), ('cloud_cover', 'cloud_cover', 'int'), ('cloud_cover_low', 'cloud_cover_low', 'int'), ('cloud_cover_mid', 'cloud_cover_mid', 'int'), ('cloud_cover_high', 'cloud_cover_high', 'int'), ('dew_point_2m', 'dew_point_2m', 'double'), ('is_day', 'is_day', 'boolean'), ('shortwave_radiation', 'shortwave_radiation', 'double'), ('direct_radiation', 'direct_radiation', 'double'), ('direct_normal_irradiance', 'direct_normal_irradiance', 'double'), ('diffuse_radiation', 'diffuse_radiation', 'double'), ('terrestrial_radiation', 'terrestrial_radiation', 'double'), ('sunshine_duration', 'sunshine_duration', 'double'), ('uv_index', 'uv_index', 'double'), ('uv_index_clear_sky', 'uv_index_clear_sky', 'double'), ('vapour_pressure_deficit', 'vapour_pressure_deficit', 'double'), ('visibility', 'visibility', 'double'), ('wet_bulb_temperature_2m', 'wet_bulb_temperature_2m', 'double'), ('wind_speed_10m', 'wind_speed_10m', 'double'), ('wind_speed_80m', 'wind_speed_80m', 'double'), ('wind_direction_10m', 'wind_direction_10m', 'int'), ('wind_direction_80m', 'wind_direction_80m', 'int'), ('wind_gusts_10m', 'wind_gusts_10m', 'double'), ('soil_temperature_0cm', 'soil_temperature_0cm', 'double'), ('soil_temperature_6cm', 'soil_temperature_6cm', 'double'), ('soil_temperature_18cm', 'soil_temperature_18cm', 'double'), ('soil_temperature_54cm', 'soil_temperature_54cm', 'double'), ('soil_moisture_0_to_1cm', 'soil_moisture_0_to_1cm', 'double'), ('soil_moisture_1_to_3cm', 'soil_moisture_1_to_3cm', 'double'), ('soil_moisture_3_to_9cm', 'soil_moisture_3_to_9cm', 'double'), ('soil_moisture_9_to_27cm', 'soil_moisture_9_to_27cm', 'double'), ('soil_moisture_27_to_81cm', 'soil_moisture_27_to_81cm', 'double')]
WEATHER_DAILY_SPECS = [('time', 'forecast_date', 'date'), ('temperature_2m_max', 'temperature_2m_max', 'double'), ('temperature_2m_min', 'temperature_2m_min', 'double'), ('apparent_temperature_max', 'apparent_temperature_max', 'double'), ('apparent_temperature_min', 'apparent_temperature_min', 'double'), ('sunrise', 'sunrise', 'timestamp'), ('sunset', 'sunset', 'timestamp'), ('daylight_duration', 'daylight_duration', 'double'), ('sunshine_duration', 'sunshine_duration', 'double'), ('uv_index_max', 'uv_index_max', 'double'), ('precipitation_sum', 'precipitation_sum', 'double'), ('rain_sum', 'rain_sum', 'double'), ('shortwave_radiation_sum', 'shortwave_radiation_sum', 'double'), ('snowfall_sum', 'snowfall_sum', 'double'), ('precipitation_hours', 'precipitation_hours', 'double'), ('precipitation_probability_max', 'precipitation_probability_max', 'int'), ('weather_code', 'weather_code', 'int'), ('wind_speed_10m_max', 'wind_speed_10m_max', 'double'), ('wind_gusts_10m_max', 'wind_gusts_10m_max', 'double'), ('wind_direction_10m_dominant', 'wind_direction_10m_dominant', 'int'), ('et0_fao_evapotranspiration', 'et0_fao_evapotranspiration', 'double')]
AIR_QUALITY_HOURLY_SPECS = [('time', 'forecast_for', 'timestamp'), ('pm2_5', 'pm2_5', 'double'), ('pm10', 'pm10', 'double'), ('carbon_monoxide', 'carbon_monoxide', 'double'), ('nitrogen_dioxide', 'nitrogen_dioxide', 'double'), ('sulphur_dioxide', 'sulphur_dioxide', 'double'), ('ozone', 'ozone', 'double'), ('dust', 'dust', 'double'), ('aerosol_optical_depth', 'aerosol_optical_depth', 'double'), ('european_aqi', 'european_aqi', 'int'), ('us_aqi', 'us_aqi', 'int'), ('us_aqi_carbon_monoxide', 'us_aqi_carbon_monoxide', 'int'), ('us_aqi_nitrogen_dioxide', 'us_aqi_nitrogen_dioxide', 'int'), ('us_aqi_ozone', 'us_aqi_ozone', 'int'), ('us_aqi_pm10', 'us_aqi_pm10', 'int'), ('us_aqi_pm2_5', 'us_aqi_pm2_5', 'int'), ('us_aqi_sulphur_dioxide', 'us_aqi_sulphur_dioxide', 'int'), ('uv_index', 'uv_index', 'double'), ('uv_index_clear_sky', 'uv_index_clear_sky', 'double'), ('alder_pollen', 'alder_pollen', 'double'), ('ammonia', 'ammonia', 'double'), ('birch_pollen', 'birch_pollen', 'double'), ('grass_pollen', 'grass_pollen', 'double'), ('mugwort_pollen', 'mugwort_pollen', 'double'), ('olive_pollen', 'olive_pollen', 'double'), ('ragweed_pollen', 'ragweed_pollen', 'double')]

WEATHER_HOURLY_VARS = ','.join(n for n,_,_ in WEATHER_HOURLY_SPECS if n!='time')
WEATHER_DAILY_VARS = ','.join(n for n,_,_ in WEATHER_DAILY_SPECS if n!='time')
AQ_HOURLY_VARS = ','.join(n for n,_,_ in AIR_QUALITY_HOURLY_SPECS if n!='time')
AQ_CURRENT_VARS = AQ_HOURLY_VARS

CURRENT_TYPES = {
    'weather': {name: dict((a,t) for a,_,t in WEATHER_HOURLY_SPECS).get(name,'double') for name in WEATHER_CURRENT_VARS.split(',')},
    'air_quality': {name: ('int' if 'aqi' in name else 'double') for name in AQ_CURRENT_VARS.split(',')},
}
CURRENT_TYPES['weather']['is_day']='boolean'
LINEAGE_TYPES = {'location_name':'string','location_id':'long','latitude':'double','longitude':'double','elevation_m':'double','snapshot_at':'timestamp','source':'string','api_endpoint':'string','source_file_path':'string','ingested_at':'timestamp','_rescued_data':'string'}
EQ_STRINGS = 'code magType place title type status alert net url detail ids sources types'.split()
EQ_DOUBLES = 'mag gap dmin rms cdi mmi'.split()
EQ_LONGS = 'time updated sig tsunami nst felt tz'.split()
EQ_TYPES = dict.fromkeys('source_file_path _rescued_data api_endpoint source feed_api_version feed_title event_id event_code magnitude_type place_description event_title event_type review_status alert_level seismic_network event_page_url detail_api_url geometry_type'.split(),'string')
EQ_TYPES.update(dict.fromkeys('ingested_at snapshot_at feed_generated_at event_time updated_at'.split(),'timestamp'))
EQ_TYPES.update(dict.fromkeys('feed_generated_ms event_time_ms updated_at_ms significance station_count felt_report_count timezone_offset'.split(),'long'))
EQ_TYPES.update(dict.fromkeys('magnitude azimuthal_gap distance_to_nearest_station_deg rms_travel_time_residual community_intensity mercalli_intensity longitude latitude depth_km'.split(),'double'))
EQ_TYPES.update({'tsunami_flag':'boolean','all_event_ids':'array<string>','all_sources':'array<string>','available_product_types':'array<string>'})

def specs(source,kind):
    if source=='weather': return WEATHER_DAILY_SPECS if kind=='daily' else WEATHER_HOURLY_SPECS
    return AIR_QUALITY_HOURLY_SPECS

def output_types(source,kind):
    if source=='earthquakes': return EQ_TYPES.copy()
    result=LINEAGE_TYPES.copy()
    if kind=='current': result.update(CURRENT_TYPES[source],observed_at='timestamp',interval_seconds='int')
    else: result.update({out:typ for _,out,typ in specs(source,kind)})
    return result

def variables(source,section):
    if source=='weather': return globals()['WEATHER_'+section.upper()+'_VARS'].split(',')
    return globals()['AQ_'+section.upper()+'_VARS'].split(',')
