"""Exact committed run → validate → all-target preflight → writes → reconcile → state."""
from importlib import import_module
from src.config import load_config, SILVER_TABLES
from src.validation.raw import load_committed
from src.validation.bronze import validate_bronze
from src.validation.common import ValidationResult, ValidationError, contextualize, require, measurement_completeness
from src.validation.repair import validate_missing_targets
from .incremental import pending_files, read_scope, merge_output, complete_files, preflight_outputs

TABLES=SILVER_TABLES

def run_silver(spark,source,manifest_path,config=None,*,mark_completed=True,validate_only=False,report=True,require_targets=False,repair=False,rebuild_missing=False,reconstruction_manifests=None):
    require(all(type(flag) is bool for flag in (repair,rebuild_missing,validate_only,mark_completed)), 'Processing/repair flags must be booleans',stage='repair')
    require(not (repair and validate_only),'repair=True is incompatible with validate_only=True',stage='repair')
    require(not rebuild_missing or repair,'rebuild_missing requires explicit repair',stage='repair')
    cfg=(config or load_config()).validate()
    manifest=load_committed(manifest_path,cfg,source)
    spark.conf.set('spark.sql.session.timeZone','UTC')
    bronze=read_scope(spark,cfg.bronze(source),manifest,source)
    outputs={}
    try:
        # Always verify committed Bronze scope, even when ledger already records it.
        validate_bronze(bronze,source,manifest,cfg)
        outputs=import_module('src.transformations.'+source).transform(spark,bronze)
        outputs={k:v.cache() for k,v in outputs.items()}
        validators=import_module('src.validation.'+source+'_silver')
        context=validators.source_context(bronze,outputs['history']) if source=='earthquakes' else validators.source_context(bronze)
        if source=='earthquakes':
            for kind,df in outputs.items():validators.validate_earthquakes(df,kind,context)
            validators.validate_latest_selection(outputs['history'],outputs['latest'])
        else:
            for kind,df in outputs.items():getattr(validators,'validate_'+source)(df,kind,context=context)
        keys={k:((['event_id'] if k=='latest' else ['event_id','snapshot_at']) if source=='earthquakes' else validators.KEYS[k]) for k in outputs}
        targets={k:cfg.silver(source,k) for k in outputs}
        completeness=[] if source=='earthquakes' else [measurement_completeness(df,source,kind) for kind,df in outputs.items()]
        if require_targets:
            require(all(spark.catalog.tableExists(t) for t in targets.values()),'Missing required persisted Silver outputs',stage='target',check='target_existence')
        missing=validate_missing_targets(spark,targets,manifest,source,cfg,repair=repair,rebuild_missing=rebuild_missing,reconstruction_manifests=reconstruction_manifests)
        preflight_outputs(spark,outputs,targets,keys)
        evidence=None
        if source=='earthquakes' and spark.catalog.tableExists(targets['latest']):
            evidence=validators.verified_newer_history(spark,outputs['latest'],spark.table(targets['latest']),cfg)
            # Predictably corrupt newer versions cannot be repaired by an older scope.
            newer=validators.newer_target_rows(outputs['latest'],spark.table(targets['latest']))
            if newer.limit(1).count():
                incoming_older=outputs['latest'].join(newer.select('event_id'),'event_id')
                validators.validate_earthquake_target(incoming_older,newer,'latest',history=evidence)
        pending=pending_files(spark,manifest,source,cfg)
        has_pending=bool(pending.limit(1).count())
        if not validate_only and (has_pending or repair):
            spark.sql(f'CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.silver_schema}')
            # Replay the entire immutable scope after partial failure, not a moving backlog.
            for kind,df in outputs.items():merge_output(spark,df,targets[kind],keys[kind],latest=(source=='earthquakes' and kind=='latest'))
        # validate_only checks persisted targets when present; source validation/preflight
        # still works before first publication without creating any schemas or tables.
        for kind,df in outputs.items():
            if not has_pending or not validate_only or spark.catalog.tableExists(targets[kind]):
                target=spark.table(targets[kind])
                if source=='earthquakes':validators.validate_earthquake_target(df,target,kind,history=evidence if kind=='latest' else None)
                else:getattr(validators,'validate_'+source+'_target')(df,target,kind)
        if not validate_only and mark_completed:complete_files(spark,source,manifest,cfg)
        result=ValidationResult('silver',{'validated':True,'written':not validate_only and (has_pending or repair),'repair_requested':repair,'reconciled':True,'recreated_tables':missing if not validate_only and (has_pending or repair) else [],'restored_scope_run_ids':[manifest['run_id']], 'full_history_restored':False,'marked_completed':not validate_only and mark_completed},context={'run_id':manifest['run_id'],'source':source})
        result.checks['completeness']=[r.to_dict() for r in completeness]
        result.warnings.extend(w for r in completeness for w in r.warnings)
        return result.report() if report else result
    except ValidationError as exc:
        raise contextualize(exc,run_id=manifest['run_id'],source=source) from None
    finally:
        for df in outputs.values():df.unpersist()
        bronze.unpersist()
