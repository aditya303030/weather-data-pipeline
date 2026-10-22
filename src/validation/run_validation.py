"""Read-only standalone validation. Never fetch APIs, publish data, or commit state.

Run with python -m src.validation.run_validation --stage raw --manifest PATH.
API checks replay saved payloads; registry preservation additionally needs the
pre-refresh backup table. Silver checks reconstruct only manifest-scoped inputs.
"""
import argparse
import json
from pathlib import Path
from importlib import import_module
from .common import require, ValidationError
from .raw import validate_raw_files, read_json_file
from .bronze import validate_bronze
from .location_registry import validate_registry

def run(stage,source,*,spark=None,manifest=None,registry_backup=None,run_id=None):
    require(stage in ('registry','ingestion','raw','bronze','silver','all'),'Unknown validation stage')
    require(source in ('weather','air_quality','earthquakes','all'),'Unknown source')
    stages=['registry','raw','bronze','silver'] if stage=='all' else [stage]
    results=[]
    if any(s!='registry' for s in stages):
        require(manifest is not None,'An explicit completed-run manifest is required')
        if isinstance(manifest,(str,Path)):
            manifest=read_json_file(manifest)
        require(run_id is None or manifest.get('run_id')==run_id,'Manifest does not match requested run ID')
    for current in stages:
        if current=='registry':
            require(spark is not None,'Registry validation requires Spark')
            old=spark.table(registry_backup) if registry_backup else None
            results.append(validate_registry(spark.table('weather_api.reference.location_registry'),old))
        elif current in ('raw','ingestion'):
            results.append(validate_raw_files(manifest))
        else:
            require(spark is not None,current+' validation requires Spark')
            spark.conf.set('spark.sql.session.timeZone','UTC')
            sources=manifest['sources'] if source=='all' else [source]
            for src in sources:
                require(src in manifest['sources'],'Source absent from manifest')
                paths=[f['path'] for f in manifest['files'] if f['source']==src]
                bronze=spark.table(f'weather_api.bronze.{src}_raw')
                from pyspark.sql import functions as F
                scope=bronze.filter(F.col('_source_file_path').isin(paths)).cache()
                outputs={}
                try:
                    results.append(validate_bronze(scope,src,paths))
                    if current=='silver':
                        from src.transformations.silver import TABLES
                        validators=import_module('src.validation.'+src+'_silver')
                        outputs=import_module('src.transformations.'+src).transform(spark,scope)
                        outputs={k:v.cache() for k,v in outputs.items()}
                        context=validators.source_context(scope) if src!='earthquakes' else validators.source_context(scope,outputs['history'])
                        for kind,df in outputs.items():
                            target=spark.table('weather_api.silver.'+TABLES[src][kind])
                            if src=='earthquakes':
                                results.append(validators.validate_earthquakes(df,kind,context))
                                results.append(validators.validate_earthquake_target(df,target,kind))
                            else:
                                results.append(getattr(validators,'validate_'+src)(df,kind,context=context))
                                results.append(getattr(validators,'validate_'+src+'_target')(df,target,kind))
                        if src=='earthquakes': validators.validate_latest_selection(outputs['history'],outputs['latest'])
                finally:
                    for df in outputs.values(): df.unpersist()
                    scope.unpersist()
    return results

def main(argv=None,spark=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage',required=True,choices=['registry','ingestion','raw','bronze','silver','all'])
    parser.add_argument('--source',default='all',choices=['weather','air_quality','earthquakes','all'])
    parser.add_argument('--manifest'); parser.add_argument('--registry-backup'); parser.add_argument('--run-id')
    args=parser.parse_args(argv)
    if spark is None and args.stage not in ('raw','ingestion'):
        from pyspark.sql import SparkSession
        spark=SparkSession.getActiveSession()
        require(spark is not None,'No active Spark session; pass spark explicitly in Databricks')
    return run(args.stage,args.source,spark=spark,manifest=args.manifest,registry_backup=args.registry_backup,run_id=args.run_id)

if __name__=='__main__':
    try: main()
    except ValidationError as exc:
        print(json.dumps({'status':'failed','error':str(exc)}))
        raise SystemExit(1)
