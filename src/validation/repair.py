"""Read-only repair safety checks; completion rows are never deleted or bypassed."""
from .common import require
from .raw import load_committed

def validate_missing_targets(spark,targets,manifest,source,config,*,repair=False,rebuild_missing=False,reconstruction_manifests=None):
    from pyspark.sql import functions as F
    from functools import reduce
    from src.transformations.scope import membership
    missing=[t for t in targets.values() if not spark.catalog.tableExists(t)]
    if not missing:return []
    ledger_exists=spark.catalog.tableExists(config.ledger)
    known=spark.table(config.ledger).filter(F.col('source')==source) if ledger_exists else None
    has_history=ledger_exists and bool(known.limit(1).count())
    if not repair and not has_history:return missing # Initial publication, not reconstruction.
    require(repair and rebuild_missing,'Missing historical target table: use explicit bounded recover_committed(repair=True, rebuild_missing=True) with all completed source scopes',stage='repair',check='missing_table')
    require(has_history,'Cannot establish reconstruction coverage without verified completion history; use reviewed adoption/new targets',stage='repair',check='coverage')
    paths=reconstruction_manifests or [str(config.manifest_path(manifest['run_id']))]
    require(isinstance(paths,(list,tuple)) and 0<len(paths)<=100 and len(set(paths))==len(paths),'Explicit unique bounded reconstruction manifests required',stage='repair')
    scopes=[load_committed(p,config,source) for p in paths]
    require(manifest['run_id'] in {m['run_id'] for m in scopes},'Selected manifest absent from reconstruction scope',stage='repair')
    relations=[membership(spark,m,source).select(F.col('_source_file_path').alias('path'),F.col('_expected_run').alias('run_id')) for m in scopes]
    covered=reduce(lambda a,b:a.unionByName(b),relations)
    require(not known.select('path','run_id').join(covered,['path','run_id'],'left_anti').limit(1).count(),
            'Incomplete reconstruction scope: include every completed source manifest; no table was recreated',stage='repair',check='coverage')
    return missing
