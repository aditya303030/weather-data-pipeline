"""Explicit GeoJSON schema handles legitimate zero-event initial feeds."""
def earthquake_envelope():
    from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType,ArrayType
    def struct(fields): return StructType([StructField(n,t,True) for n,t in fields])
    strings='code magType place title type status alert net url detail ids sources types'
    doubles='mag gap dmin rms cdi mmi'
    longs='time updated sig tsunami nst felt tz'
    props=struct([(x,StringType()) for x in strings.split()]+[(x,DoubleType()) for x in doubles.split()]+[(x,LongType()) for x in longs.split()])
    feature=struct([('type',StringType()),('id',StringType()),('properties',props),('geometry',struct([('type',StringType()),('coordinates',ArrayType(DoubleType()))]))])
    feed=struct([('type',StringType()),('metadata',struct([('api',StringType()),('title',StringType()),('generated',LongType()),('count',LongType()),('status',LongType()),('url',StringType())])),('features',ArrayType(feature)),('bbox',ArrayType(DoubleType()))])
    metadata=struct([(x,StringType()) for x in ('source','run_id','retrieval_timestamp_utc','api_endpoint')])
    return struct([('metadata',metadata),('raw_response',feed),('_rescued_data',StringType())])
