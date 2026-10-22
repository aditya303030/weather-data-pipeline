"""Bounded, exact manifest membership represented as a Spark relation."""
from src.validation.common import require

def membership(spark,manifest,source):
    require(source in manifest['sources'],'Source absent from committed run',stage='scope')
    rows=[(e['path'],manifest['run_id'],source,e['location_id']) for e in manifest['files'] if e['source']==source]
    return spark.createDataFrame(rows,'_source_file_path string, _expected_run string, _expected_source string, _expected_id long')

def scoped_frame(df,manifest,source,*,with_expectations=False):
    from pyspark.sql import functions as F
    selected=df.join(F.broadcast(membership(df.sparkSession,manifest,source)),'_source_file_path','inner')
    return selected if with_expectations else selected.select(*df.columns)
