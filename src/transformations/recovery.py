"""Explicit bounded recovery from operator-selected commit records, never Bronze history."""
from src.validation.common import require
from src.validation.raw import load_committed
from .silver import run_silver

def recover_committed(spark,manifest_paths,source,config,*,max_runs=10,validate_only=False):
    require(type(max_runs) is int and 0<max_runs<=100,'Invalid recovery bound',stage='recovery')
    # Accept an explicit bounded sequence, not an unbounded iterator or latest-run scan.
    require(isinstance(manifest_paths,(list,tuple)) and 0<len(manifest_paths)<=max_runs,'Supply a bounded explicit manifest list',stage='recovery')
    require(len(set(manifest_paths))==len(manifest_paths),'Duplicate recovery scopes',stage='recovery')
    records=[(load_committed(p,config,source)['run_id'],p) for p in manifest_paths]
    return [run_silver(spark,source,p,config,validate_only=validate_only) for _,p in sorted(records)]
