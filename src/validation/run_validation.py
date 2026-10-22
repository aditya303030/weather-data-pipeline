"""Read-only validation of an explicit committed run; never initializes processing state."""
import argparse
import json
from src.config import load_config
from .common import require, ValidationError
from .raw import load_committed, validate_raw_files
from .bronze import validate_bronze
from .location_registry import validate_registry

def run(stage,source,*,spark=None,manifest=None,registry_backup=None,run_id=None,config=None):
    cfg=(config or load_config()).validate()
    require(stage in ('registry','ingestion','raw','bronze','silver','all'),'Unknown validation stage')
    require(source in ('weather','air_quality','earthquakes','all'),'Unknown source')
    stages=['registry','raw','bronze','silver'] if stage=='all' else [stage]
    m=None
    if stage!='registry':
        require(isinstance(manifest,str),'Explicit durable manifest_path required',stage='manifest')
        m=load_committed(manifest,cfg,None if source=='all' else source)
        require(run_id is None or run_id==m['run_id'],'Requested run ID differs from manifest',stage='manifest')
    results=[]
    for current in stages:
        if current=='registry':
            require(spark is not None,'Spark session required')
            results.append(validate_registry(spark.table(cfg.registry),spark.table(registry_backup) if registry_backup else None))
        elif current in ('raw','ingestion'):results.append(validate_raw_files(m,cfg))
        else:
            require(spark is not None,'Spark session required')
            spark.conf.set('spark.sql.session.timeZone','UTC')
            for src in m['sources'] if source=='all' else [source]:
                if current=='bronze':results.append(validate_bronze(spark.table(cfg.bronze(src)),src,m,cfg))
                else:
                    # Same orchestration/contract, strictly no persistent writes.
                    from src.transformations.silver import run_silver
                    results.append(run_silver(spark,src,manifest,cfg,validate_only=True,report=False,require_targets=True))
    for result in results:result.report()
    return results

def main(argv=None,spark=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stage',required=True,choices=['registry','ingestion','raw','bronze','silver','all'])
    p.add_argument('--source',default='all',choices=['weather','air_quality','earthquakes','all'])
    p.add_argument('--manifest');p.add_argument('--registry-backup');p.add_argument('--run-id');p.add_argument('--config')
    a=p.parse_args(argv)
    if spark is None and a.stage not in ('raw','ingestion'):
        from pyspark.sql import SparkSession
        spark=SparkSession.getActiveSession()
        require(spark is not None,'No active Spark session; pass spark explicitly')
    return run(a.stage,a.source,spark=spark,manifest=a.manifest,registry_backup=a.registry_backup,run_id=a.run_id,config=load_config(a.config))

if __name__=='__main__':
    try:main()
    except ValidationError as exc:
        from dataclasses import asdict
        print(json.dumps(asdict(exc.result),default=str));raise SystemExit(1)
