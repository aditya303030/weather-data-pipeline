"""Explicit bounded recovery from operator-selected commit records, never Bronze history."""
from src.validation.common import require, ValidationResult
from src.validation.raw import load_committed
from .silver import run_silver

def recover_committed(spark,manifest_paths,source,config,*,max_runs=10,validate_only=False,repair=False,rebuild_missing=False):
    require(all(type(flag) is bool for flag in (repair,rebuild_missing,validate_only)),'Recovery flags must be booleans',stage='repair')
    require(not (repair and validate_only),'Repair cannot be read-only',stage='repair')
    require(not rebuild_missing or repair,'rebuild_missing requires repair',stage='repair')
    require(type(max_runs) is int and 0<max_runs<=100,'Invalid recovery bound',stage='recovery')
    # Accept an explicit bounded sequence, not an unbounded iterator or latest-run scan.
    require(isinstance(manifest_paths,(list,tuple)) and 0<len(manifest_paths)<=max_runs,'Supply a bounded explicit manifest list',stage='recovery')
    require(len(set(manifest_paths))==len(manifest_paths),'Duplicate recovery scopes',stage='recovery')
    records=[(load_committed(p,config,source)['run_id'],p) for p in manifest_paths]
    results=[run_silver(spark,source,p,config,validate_only=validate_only,repair=repair,rebuild_missing=rebuild_missing,reconstruction_manifests=list(manifest_paths) if rebuild_missing else None) for _,p in sorted(records)]
    ValidationResult('recovery',{'repair_requested':repair,'reconciled':True,'restored_scope_run_ids':[run for run,_ in sorted(records)],'full_history_restored':False},context={'source':source}).report()
    return results
