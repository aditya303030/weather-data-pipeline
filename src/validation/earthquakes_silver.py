"""Earthquake keys, precision, latest-version and persisted-scope checks."""
from .common import check_frame, require, reconcile, scoped_target, ValidationResult, ValidationError

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

def newer_target_rows(source,target):
    from pyspark.sql import functions as F
    pairs=source.select('event_id','updated_at','snapshot_at').alias('s').join(target.alias('t'),'event_id')
    newer=(F.col('t.updated_at')>F.col('s.updated_at')) | ((F.col('t.updated_at')==F.col('s.updated_at')) & (F.col('t.snapshot_at')>F.col('s.snapshot_at')))
    return pairs.filter(newer).select('t.*')


def validate_earthquake_target(source,target,kind,*,history=None):
    """Version-aware reconciliation. history must be independent verified evidence.

    Same version compares every persisted column with the incoming trusted row.
    A newer target compares against its exact (event, updated, snapshot) history
    version, never against an older source or against itself. Missing evidence fails.
    """
    if kind=='history':return scoped_target(source,target,['event_id','snapshot_at'])
    from pyspark.sql import functions as F
    from functools import reduce
    scope=target.join(source.select('event_id').distinct(),'event_id')
    validate_earthquakes(scope,'latest')
    require(not source.select('event_id').join(scope.select('event_id'),'event_id','left_anti').limit(1).count(),
            'Missing target earthquake event',stage='earthquake_target',check='missing_event')
    pairs=source.alias('s').join(scope.alias('t'),'event_id')
    older=(F.col('t.updated_at')<F.col('s.updated_at')) | ((F.col('t.updated_at')==F.col('s.updated_at')) & (F.col('t.snapshot_at')<F.col('s.snapshot_at')))
    require(not pairs.filter(older).limit(1).count(),'Target earthquake version older than incoming',stage='earthquake_target',check='version_order')
    same=F.col('t.updated_at').eqNullSafe(F.col('s.updated_at')) & F.col('t.snapshot_at').eqNullSafe(F.col('s.snapshot_at'))
    equal=reduce(lambda a,b:a & b,(F.col('t.'+c).eqNullSafe(F.col('s.'+c)) for c in source.columns),F.lit(True))
    require(not pairs.filter(same & ~equal).limit(1).count(),'Same-version earthquake content differs',stage='earthquake_target',check='same_version_content')
    newer=newer_target_rows(source,scope)
    if newer.limit(1).count():
        require(history is not None,'Newer target requires verified history evidence; select its newer committed scope',stage='earthquake_target',check='history_evidence')
        version_keys=['event_id','updated_at','snapshot_at']
        evidence=history.join(newer.select(*version_keys),version_keys,'inner').select(*source.columns)
        check_frame(evidence,stage='earthquake_history_evidence',keys=version_keys)
        try:scoped_target(evidence,newer,['event_id'])
        except ValidationError as exc:
            exc.result.details['message']='Newer target differs from verified history; select and repair its newer committed scope'
            exc.result.check='newer_version_content'
            raise ValidationError(exc.result.details['message'],exc.result) from None
        require(not newer.select(*version_keys).join(evidence.select(*version_keys),version_keys,'left_anti').limit(1).count(),
                'Missing exact newer history version; select its newer committed scope',stage='earthquake_target',check='history_evidence')
    return ValidationResult('earthquake_target',{'version_order_and_content':True})


def verified_newer_history(spark,source,target,config,*,max_runs=10):
    """Reconstruct newer evidence from exact committed raw/Bronze scopes.

    Persisted history is a locator, not trusted simply because it is persisted.
    Each located scope must pass manifest digests, Bronze membership and history
    reconciliation against a fresh production transform before it becomes evidence.
    No table/state writes and no live APIs occur here.
    """
    from pathlib import Path
    from functools import reduce
    from src.validation.raw import load_committed
    from src.validation.bronze import validate_bronze
    from src.transformations.incremental import read_scope
    from src.transformations.earthquakes import transform
    from src.transformations.schemas import envelope_schema
    from pyspark.sql import functions as F
    newer=newer_target_rows(source,target)
    if not newer.limit(1).count():return None
    table=config.silver('earthquakes','history')
    require(spark.catalog.tableExists(table),'Newer target history missing; select its newer committed scope',stage='earthquake_target',check='history_evidence')
    keys=['event_id','updated_at','snapshot_at']
    history=spark.table(table).join(newer.select(*keys),keys,'inner')
    check_frame(history,stage='history_locator',keys=keys)
    require(not newer.select(*keys).join(history.select(*keys),keys,'left_anti').limit(1).count(),'Exact newer history missing; select the newer committed scope',stage='earthquake_target')
    paths=[r.source_file_path for r in history.select('source_file_path').distinct().limit(max_runs+1).collect()]
    require(len(paths)<=max_runs,'Newer evidence exceeds bound; explicitly recover relevant newer scopes first',stage='earthquake_target')
    frames=[]
    for path in paths:
        try:parts=Path(path).relative_to(Path(config.raw_root)/'earthquakes').parts
        except (ValueError,TypeError):raise ValidationError('Historical evidence needs explicit committed adoption') from None
        require(len(parts)==2,'Invalid history file lineage',stage='earthquake_target')
        manifest=load_committed(str(config.manifest_path(parts[0])),config,'earthquakes')
        require(any(e['path']==path for e in manifest['files']),'History lineage not in committed manifest',stage='earthquake_target')
        bronze=read_scope(spark,config.bronze('earthquakes'),manifest,'earthquakes')
        try:
            validate_bronze(bronze,'earthquakes',manifest,config)
            # Payload truth comes from digest-verified raw JSON, not mutable latest
            # or history values. Bronze contributes only validated ingestion lineage.
            raw=(spark.read.schema(envelope_schema('earthquakes')).option('multiLine','true').json([path])
                 .withColumn('_source_file_path',F.regexp_replace(F.col('_metadata.file_path'),'^(dbfs:|file:)/+','/')))
            trusted=raw.join(bronze.select('_source_file_path','_ingest_timestamp'),'_source_file_path','inner')
            expected=transform(spark,trusted)['history']
            validate_earthquakes(expected,'history')
            exact=expected.join(history.select(*keys),keys,'inner').select(*expected.columns)
            try:scoped_target(exact,history.join(exact.select(*keys),keys,'inner'),['event_id','snapshot_at'])
            except ValidationError as exc:
                exc.result.details['message']='Persisted newer history differs from committed input; select and repair the newer committed scope'
                exc.result.details['manifest_path']=str(config.manifest_path(manifest['run_id']))
                raise ValidationError(exc.result.details['message'],exc.result) from None
            frames.append(exact)
        finally:bronze.unpersist()
    require(bool(frames),'No trusted newer history evidence',stage='earthquake_target')
    evidence=reduce(lambda a,b:a.unionByName(b),frames)
    require(not newer.select(*keys).join(evidence.select(*keys),keys,'left_anti').limit(1).count(),'Newer history cannot be reconstructed; select relevant newer scope',stage='earthquake_target')
    return evidence


def source_context(bronze,history):
    from pyspark.sql import functions as F
    expected=bronze.agg(F.sum(F.size('raw_response.features')).alias('n')).first().n or 0
    latest=history.agg(F.countDistinct('event_id').alias('n')).first().n
    return {'history':expected,'latest':latest}
