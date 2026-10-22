"""Structured, blocking runtime checks. Spark imports are deliberately lazy."""
from dataclasses import dataclass, field, asdict
import json
import math
from datetime import datetime

@dataclass
class CheckResult:
    stage: str
    check: str
    severity: str = 'error'
    passed: bool = True
    affected_rows: int | None = None
    details: dict = field(default_factory=dict)
    context: dict = field(default_factory=dict)

class ValidationError(ValueError):
    def __init__(self, message, result=None):
        self.result = result or CheckResult('validation', 'requirement', passed=False, details={'message':message})
        super().__init__(json.dumps(asdict(self.result), default=str, sort_keys=True))

@dataclass
class ValidationResult:
    stage: str
    checks: dict = field(default_factory=dict)
    warnings: list = field(default_factory=list)
    context: dict = field(default_factory=dict)

    def to_dict(self):
        records=[asdict(CheckResult(self.stage, name, affected_rows=(value if type(value) is int and name.startswith(('key:','range:','timestamp:','duplicate')) else None), details={'value':value}, context=self.context)) for name,value in self.checks.items()]
        records += [asdict(CheckResult(self.stage, 'warning', severity='warning', passed=False, details={'message':w}, context=self.context)) for w in self.warnings]
        return {'stage':self.stage, 'results':records, 'context':self.context}

    def report(self):
        print(json.dumps(self.to_dict(), default=str, sort_keys=True))
        return self

def require(condition, message, *, stage='validation', check='requirement', affected_rows=None, context=None):
    if not condition:
        raise ValidationError(message, CheckResult(stage,check,passed=False,affected_rows=affected_rows,details={'message':message},context=context or {}))


def timestamp(value, *, offset_required=False):
    require(isinstance(value, str) and bool(value.strip()), 'Missing timestamp')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValidationError('Invalid ISO timestamp') from exc
    require(not offset_required or parsed.tzinfo is not None, 'Timestamp must include UTC offset')
    return parsed

def numeric(value, lower=None, upper=None, *, nullable=True):
    if value is None and nullable:
        return
    require(isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value), 'Invalid numeric measurement')
    require(lower is None or value >= lower, 'Measurement below permitted range')
    require(upper is None or value <= upper, 'Measurement above permitted range')

def reconcile(actual, expected, label):
    require(actual == expected, f'{label}: actual={actual}, expected={expected}')

def require_columns(df, required):
    """required maps dotted field names to compatible Spark simpleString types."""
    for name, types in required.items():
        dtype = df.schema
        try:
            for part in name.split('.'):
                dtype = next(f.dataType for f in dtype.fields if f.name == part)
            actual = dtype.simpleString()
        except (AttributeError, StopIteration) as exc:
            raise ValidationError(f'Missing required column: {name}; historical identity/schema changes require explicit backfill') from exc
        if types:
            require(actual in types, f'{name}: incompatible type {actual}; expected {types}. Run an explicit data-preserving migration.')


def check_frame(df, *, stage, keys, required=None, ranges=None, timestamps=(), nonempty=False, predicates=None):
    """One aggregate for compatible null/range/timestamp checks; one grouped key check.

    Reject the entire processing scope on failure; never silently drop bad records.
    """
    from pyspark.sql import functions as F
    require_columns(df, required or {})
    conditions = {}
    for key in keys:
        require_columns(df, {key: None})
        conditions['key:' + key] = F.col(key).isNull() | (F.trim(F.col(key).cast('string')) == '')
    for name in timestamps:
        require_columns(df, {name: ('timestamp',)})
        conditions['timestamp:' + name] = F.col(name).isNull()
    for name, (lo, hi) in (ranges or {}).items():
        require_columns(df, {name: ('double','float','int','bigint','smallint','decimal(10,0)')})
        c = F.col(name)
        bad = F.isnan(c) | (F.abs(c) == float('inf'))
        if lo is not None: bad = bad | (c < lo)
        if hi is not None: bad = bad | (c > hi)
        conditions['range:' + name] = c.isNotNull() & bad
    conditions.update(predicates or {})
    exprs = [F.count('*').alias('rows')]
    exprs += [F.sum(F.when(v, 1).otherwise(0)).alias('c'+str(i)) for i,v in enumerate(conditions.values())]
    stats = df.agg(*exprs).first().asDict()
    require(not nonempty or stats['rows'] > 0, stage + ': empty output is forbidden')
    failures = {name: stats['c'+str(i)] for i,name in enumerate(conditions) if stats['c'+str(i)]}
    require(not failures, f'{stage}: invalid records: {failures}',stage=stage,check='record_quality',affected_rows=sum(failures.values()))
    if keys:
        duplicates = df.groupBy(*keys).count().filter(F.col('count') > 1).agg(F.sum('count').alias('affected')).first().affected or 0
        require(not duplicates, stage + ': duplicate source keys ' + str(keys),stage=stage,check='duplicate_keys',affected_rows=duplicates)
    return ValidationResult(stage, {'rows': stats['rows'], 'keys': keys, **{name:stats['c'+str(i)] or 0 for i,name in enumerate(conditions)}})

def compatible_target(source, target):
    """Fail before writes rather than dropping/recreating an existing table."""
    src = {f.name:f.dataType.simpleString() for f in source.schema.fields}
    dst = {f.name:f.dataType.simpleString() for f in target.schema.fields}
    require(src == dst, 'Target schema differs from source. Back up and explicitly migrate/backfill the target schema and stable keys before retrying; no table was dropped.')

def scoped_target(source, target, keys):
    from pyspark.sql import functions as F
    wanted = source.select(*keys).distinct()
    actual = target.join(wanted, keys, 'inner')
    check_frame(actual, stage='target_scope', keys=keys)
    require(not wanted.join(actual.select(*keys), keys, 'left_anti').limit(1).count(), 'Missing target keys in processing scope')
    require_columns(target, {k:None for k in source.columns})
    # Reconcile complete content, including nullable measurements, not only counts.
    require(not source.select(*source.columns).exceptAll(actual.select(*source.columns)).limit(1).count(), 'Target content differs within processing scope')
    return ValidationResult('target_reconciliation', {'matched': True})

def normalize_output(df, types):
    """Explicit output types. Invalid non-null casts block before writes, in one scan."""
    from pyspark.sql import functions as F
    require(set(df.columns)==set(types),'Transform columns differ from output contract',stage='silver',check='output_contract')
    casted={n:F.expr(f'try_cast(`{n}` as {typ})') for n,typ in types.items()}
    invalid={n:F.col(n).isNotNull() & c.isNull() for n,c in casted.items()}
    # Integer/boolean domain checks prevent truncation/coercion of malformed values.
    for n,typ in types.items():
        if typ in ('int','long','boolean'):
            raw=F.col(n).cast('double')
            invalid[n]=invalid[n] | (F.col(n).isNotNull() & (raw != (casted[n].cast('int') if typ=='boolean' else casted[n]).cast('double')))
    row=df.agg(*[F.sum(F.when(p,1).otherwise(0)).alias(n) for n,p in invalid.items()]).first().asDict()
    failures={n:v for n,v in row.items() if v}
    require(not failures,'Invalid casts: '+str(failures),stage='silver',check='casts',affected_rows=sum(failures.values()))
    return df.select(*[c.alias(n) for n,c in casted.items()])

# Context is attached at orchestration boundaries without repeating it in every
# primitive check. Nested calls retain run/source diagnostics on raised failures.
def contextualize(error, **context):
    error.result.context.update(context)
    return ValidationError(error.result.details.get('message','Validation failed'),error.result)
