"""Air-quality transformations; nullable optional measurements retain typed nulls."""
from .measurements import transform_measurements

def transform(spark,input_df): return transform_measurements(spark,input_df,'air_quality')
