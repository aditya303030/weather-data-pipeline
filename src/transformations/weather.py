"""Weather transformations; no persistent side effects on import or invocation."""
from .measurements import transform_measurements

def transform(spark,input_df): return transform_measurements(spark,input_df,'weather')
