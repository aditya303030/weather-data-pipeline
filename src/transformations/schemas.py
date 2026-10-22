"""Explicit nullable Bronze contracts, including absent optional initial fields."""
from src.contracts import variables, EQ_STRINGS, EQ_DOUBLES, EQ_LONGS

def envelope_schema(source):
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, ArrayType, MapType
    def struct(fields): return StructType([StructField(n,t,True) for n,t in fields])
    metadata=struct([(n,StringType()) for n in ('source','run_id','retrieval_timestamp_utc','api_endpoint','location_name')]+[('location_id',LongType()),('latitude',DoubleType()),('longitude',DoubleType())])
    if source=='earthquakes':
        props=struct([(n,StringType()) for n in EQ_STRINGS]+[(n,DoubleType()) for n in EQ_DOUBLES]+[(n,LongType()) for n in EQ_LONGS])
        feature=struct([('type',StringType()),('id',StringType()),('properties',props),('geometry',struct([('type',StringType()),('coordinates',ArrayType(DoubleType()))]))])
        response=struct([('type',StringType()),('metadata',struct([('api',StringType()),('title',StringType()),('generated',LongType()),('count',LongType()),('status',LongType()),('url',StringType())])),('features',ArrayType(feature)),('bbox',ArrayType(DoubleType()))])
    else:
        current=struct([('time',StringType()),('interval',LongType())]+[(n,DoubleType()) for n in variables(source,'current')])
        sections=[('latitude',DoubleType()),('longitude',DoubleType()),('elevation',DoubleType()),('utc_offset_seconds',LongType()),('current',current)]
        for section in ('hourly','daily') if source=='weather' else ('hourly',):
            sections.append((section,struct([('time',ArrayType(StringType()))]+[(n,ArrayType(StringType() if n in ('sunrise','sunset') else DoubleType())) for n in variables(source,section)])))
        # Units and API bookkeeping remain in rescued data when not modeled;
        # include their expected schema so normal API metadata is not drift.
        sections += [(n,StringType()) for n in ('timezone','timezone_abbreviation')]+[('generationtime_ms',DoubleType())]
        for section in ('current','hourly','daily') if source=='weather' else ('current','hourly'):
            sections.append((section+'_units', MapType(StringType(),StringType())))
        response=struct(sections)
    return struct([('metadata',metadata),('raw_response',response),('_rescued_data',StringType())])

def earthquake_envelope(): return envelope_schema('earthquakes')
