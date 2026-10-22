"""Earthquake keys, precision, latest-version and persisted-scope checks."""
from .common import check_frame, require, reconcile, scoped_target

def validate_earthquakes(df,kind,context=None):
    from pyspark.sql import functions as F
    keys=['event_id'] if kind=='latest' else ['event_id','snapshot_at']
    required={'event_id':('string',),'event_time':('timestamp',),'updated_at':('timestamp',),'event_time_ms':('bigint',),'updated_at_ms':('bigint',),'feed_generated_ms':('bigint',)}
    predicates={'rescued':F.col('_rescued_data').isNotNull(),'epoch_precision':(F.expr('unix_millis(event_time)')!=F.col('event_time_ms')) | (F.expr('unix_millis(updated_at)')!=F.col('updated_at_ms')) | (F.expr('unix_millis(feed_generated_at)')!=F.col('feed_generated_ms')),'coordinates':F.col('latitude').isNull() | F.col('longitude').isNull()}
    result=check_frame(df,stage='earthquakes_'+kind,keys=keys,required=required,ranges={'latitude':(-90,90),'longitude':(-180,180),'depth_km':(-20,1000),'magnitude':(-10,12)},timestamps=['event_time','updated_at','snapshot_at','feed_generated_at'],predicates=predicates)
    if context:
        expected=context['latest' if kind=='latest' else 'history']
        reconcile(result.checks['rows'],expected,'earthquakes '+kind)
    return result

def validate_latest_selection(history,latest):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    expected=history.withColumn('_rank',F.row_number().over(Window.partitionBy('event_id').orderBy(F.col('updated_at').desc(),F.col('snapshot_at').desc()))).filter('_rank = 1').drop('_rank')
    require(not expected.exceptAll(latest).limit(1).count() and not latest.exceptAll(expected).limit(1).count(),'Incorrect latest event selection')

def validate_earthquake_target(source,target,kind):
    if kind=='history': return scoped_target(source,target,['event_id','snapshot_at'])
    from pyspark.sql import functions as F
    scope=target.join(source.select('event_id'),'event_id')
    validate_earthquakes(scope,'latest')
    require(scope.count()==source.count(),'Missing latest events')
    pairs=source.alias('s').join(target.alias('t'),'event_id')
    require(not pairs.filter((F.col('t.updated_at')<F.col('s.updated_at')) | ((F.col('t.updated_at')==F.col('s.updated_at')) & (F.col('t.snapshot_at')<F.col('s.snapshot_at')))).limit(1).count(),'Older event replaced newer version')


def source_context(bronze,history):
    from pyspark.sql import functions as F
    expected=bronze.agg(F.sum(F.size('raw_response.features')).alias('n')).first().n or 0
    latest=history.agg(F.countDistinct('event_id').alias('n')).first().n
    return {'history':expected,'latest':latest}
