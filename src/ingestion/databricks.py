"""Small job handoff adapter. Publication errors in jobs must propagate."""
from src.validation.common import require

def publish_task_values(dbutils,result,job_execution=None):
    if job_execution is None:
        # Interactive contexts have no jobId. Missing adapter also means interactive.
        if not hasattr(dbutils,'notebook') or not hasattr(dbutils.notebook,'entry_point'):
            return
        context=dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        job_execution=context.jobId().isDefined()
    if not job_execution:return
    for key in ('run_id','manifest_path'):
        saved=dbutils.jobs.taskValues.set(key=key,value=result[key])
        require(saved is not False,'Databricks task-value publication failed',stage='handoff',check=key,context={'run_id':result['run_id']})
