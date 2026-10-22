"""Exact committed run → validate → all-target preflight → writes → reconcile → state."""
from importlib import import_module
from src.config import load_config, SILVER_TABLES
from src.validation.raw import load_committed
from src.validation.bronze import validate_bronze
from src.validation.common import ValidationResult, ValidationError, contextualize, require
from .incremental import pending_files, read_scope, merge_output, complete_files, preflight_outputs

TABLES=SILVER_TABLES

def run_silver(spark,source,manifest_path,config=None,*,mark_completed=True,validate_only=False,report=True,require_targets=False):
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
        if require_targets:
            require(all(spark.catalog.tableExists(t) for t in targets.values()),'Missing required persisted Silver outputs',stage='target',check='target_existence')
        preflight_outputs(spark,outputs,targets,keys)
        pending=pending_files(spark,manifest,source,cfg)
        has_pending=bool(pending.limit(1).count())
        if not validate_only and has_pending:
            spark.sql(f'CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.silver_schema}')
            # Replay the entire immutable scope after partial failure, not a moving backlog.
            for kind,df in outputs.items():merge_output(spark,df,targets[kind],keys[kind],latest=(source=='earthquakes' and kind=='latest'))
        # validate_only checks persisted targets when present; source validation/preflight
        # still works before first publication without creating any schemas or tables.
        for kind,df in outputs.items():
            if not has_pending or not validate_only or spark.catalog.tableExists(targets[kind]):
                target=spark.table(targets[kind])
                if source=='earthquakes':validators.validate_earthquake_target(df,target,kind)
                else:getattr(validators,'validate_'+source+'_target')(df,target,kind)
        if not validate_only and mark_completed:complete_files(spark,source,manifest,cfg)
        result=ValidationResult('silver',{'validated':True,'written':not validate_only and has_pending,'marked_completed':not validate_only and mark_completed},context={'run_id':manifest['run_id'],'source':source})
        return result.report() if report else result
    except ValidationError as exc:
        raise contextualize(exc,run_id=manifest['run_id'],source=source) from None
    finally:
        for df in outputs.values():df.unpersist()
        bronze.unpersist()
