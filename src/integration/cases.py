"""Spark Connect-compatible regression fixtures and assertions; no local session API."""
from datetime import datetime, timezone, timedelta
from src.contracts import EQ_TYPES, AQI_FIELDS, DAILY_RELATIONSHIPS
from src.validation.common import ValidationError, require

def expect_failure(call, check=None):
    try:
        call()
    except ValidationError as exc:
        if check:
            require(check in str(exc), 'Unexpected validation failure in regression: ' + str(exc), stage='integration')
        return exc.result
    raise AssertionError('Expected blocking ValidationError')

def earthquake_frame(spark, overrides):
    rows = []
    base = datetime(2026, 10, 21, tzinfo=timezone.utc)
    for values in overrides:
        row = dict.fromkeys(EQ_TYPES)
        row.update(event_id='event', source='earthquakes', api_endpoint='external-api', source_file_path='/fixture.json', review_status='automatic', magnitude=1.7, longitude=-90.0, latitude=40.0, depth_km=5.0, ingested_at=base, snapshot_at=base + timedelta(seconds=2), event_time=base, updated_at=base + timedelta(seconds=1), feed_generated_at=base)
        row.update(values)
        for field, ms in [('event_time', 'event_time_ms'), ('updated_at', 'updated_at_ms'), ('feed_generated_at', 'feed_generated_ms')]:
            row[ms] = int(row[field].replace(tzinfo=timezone.utc).timestamp() * 1000)
        rows.append(tuple((row[n] for n in EQ_TYPES)))
    return spark.createDataFrame(rows, ', '.join((n + ' ' + t for n, t in EQ_TYPES.items())))

def earthquake_content_cases(spark, observations=None):
    from src.validation.earthquakes_silver import validate_earthquake_target
    base = datetime(2026, 10, 21, tzinfo=timezone.utc)
    source = earthquake_frame(spark, [{}])
    if observations is not None:
        observations.append({'case': 'earthquake_content', 'source_overrides': [{}], 'same_version_mutations': [{'magnitude': 9.0}, {'latitude': 41.0}, {'review_status': 'reviewed'}], 'version_order': 'updated_at then snapshot_at', 'missing_and_empty': True, 'mixed_versions': True})
    validate_earthquake_target(source, earthquake_frame(spark, [{}]), 'latest')
    for change in ({'magnitude': 9.0}, {'latitude': 41.0}, {'review_status': 'reviewed'}):
        expect_failure(lambda: validate_earthquake_target(source, earthquake_frame(spark, [change]), 'latest'), 'same_version_content')
    older = earthquake_frame(spark, [{'updated_at': base}])
    expect_failure(lambda: validate_earthquake_target(source, older, 'latest'), 'version_order')
    newer_values = {'updated_at': base + timedelta(seconds=3), 'snapshot_at': base + timedelta(seconds=4), 'magnitude': 2.5}
    newer = earthquake_frame(spark, [newer_values])
    trusted = earthquake_frame(spark, [dict(newer_values)])
    validate_earthquake_target(source, newer, 'latest', history=trusted)
    expect_failure(lambda: validate_earthquake_target(source, newer, 'latest'), 'history_evidence')
    expect_failure(lambda: validate_earthquake_target(source, newer, 'latest', history=earthquake_frame(spark, [dict(newer_values, magnitude=8.0)])))
    expect_failure(lambda: validate_earthquake_target(source, earthquake_frame(spark, []), 'latest'), 'missing_event')
    validate_earthquake_target(earthquake_frame(spark, []), earthquake_frame(spark, []), 'latest')
    multi = earthquake_frame(spark, [{'event_id': 'a'}, {'event_id': 'b'}])
    mixed = earthquake_frame(spark, [{'event_id': 'a'}, dict(newer_values, event_id='b')])
    validate_earthquake_target(multi, mixed, 'latest', history=earthquake_frame(spark, [dict(newer_values, event_id='b')]))
    for change in ({'updated_at':base+timedelta(seconds=3),'snapshot_at':base}, {'snapshot_at':base+timedelta(seconds=3)}):
        later=earthquake_frame(spark,[change])
        validate_earthquake_target(source,later,'latest',history=earthquake_frame(spark,[dict(change)]))
    expect_failure(lambda:validate_earthquake_target(source,earthquake_frame(spark,[{'snapshot_at':base}]),'latest'),'version_order')
    bad_mixed=earthquake_frame(spark,[{'event_id':'a'},{'event_id':'b','updated_at':base}])
    expect_failure(lambda:validate_earthquake_target(multi,bad_mixed,'latest'),'version_order')
    return {'version_reconciliation':'passed','mixed_and_tied_versions':True}

def metric_cases(spark, observations=None):
    from pyspark.sql import functions as F
    from src.validation.common import check_frame, normalize_output
    for rows, expected, occurrences in [([(1, 100.0, 200.0, None)], 1, 2), ([(1, 100.0, 200.0, True), (2, 100.0, 0.0, None)], 2, 4)]:
        if observations is not None:
            observations.append({'case': 'metrics', 'rows': rows, 'expected_distinct_affected': expected, 'expected_occurrences': occurrences})
        df = spark.createDataFrame(rows, 'id long, latitude double, longitude double, flag boolean')
        exc = expect_failure(lambda: check_frame(df, stage='metrics', keys=['id'], ranges={'latitude': (-90, 90), 'longitude': (-180, 180)}, predicates={'flag': F.col('flag')}))
        _check(exc.affected_rows == expected, (exc, expected))
        _check(exc.details['failed_check_occurrences'] == occurrences, "exc.details['failed_check_occurrences'] == occurrences")
    for rows in ([], [(1, 0.0, 0.0, None)]):
        df = spark.createDataFrame(rows, 'id long, latitude double, longitude double, flag boolean')
        result = check_frame(df, stage='metrics', keys=['id'], predicates={'nullable': F.col('flag')})
        _check(result.checks['affected_rows'] == 0, "result.checks['affected_rows'] == 0")
    df = spark.createDataFrame([('bad', 'bad')], 'a string,b string')
    exc = expect_failure(lambda: normalize_output(df, {'a': 'double', 'b': 'double'}))
    _check(exc.affected_rows == 1 and exc.details['failed_check_occurrences'] == 2, "exc.affected_rows == 1 and exc.details['failed_check_occurrences'] == 2")
    return {'cases': 5}

def domain_cases(spark, observations=None):
    from src.validation.air_quality_silver import validate_air_quality
    from src.validation.weather_silver import validate_weather
    from src.validation.common import measurement_completeness
    base = datetime(2026, 10, 21, tzinfo=timezone.utc)
    for kind in ('current', 'hourly'):
        timefield = 'observed_at' if kind == 'current' else 'forecast_for'
        schema = 'location_id long,snapshot_at timestamp,latitude double,longitude double,pm2_5 double,us_aqi int,_rescued_data string,' + timefield + ' timestamp,' + ','.join((n + ' int' for n in AQI_FIELDS if n != 'us_aqi'))
        names = [f.name for f in spark.createDataFrame([], schema).schema.fields]
        for field in AQI_FIELDS:
            for value in (-1, 0, 10, None):
                row = {n: None for n in names}
                row.update(location_id=1, snapshot_at=base, latitude=40.0, longitude=-90.0, pm2_5=1.0, us_aqi=1)
                row[timefield] = base
                row[field] = value
                if observations is not None:
                    observations.append({'case': 'aqi', 'kind': kind, 'field': field, 'value': value, 'row': row, 'expected_blocking': value == -1})
                df = spark.createDataFrame([tuple((row[n] for n in names))], schema)
                if value == -1:
                    expect_failure(lambda: validate_air_quality(df, kind))
                else:
                    validate_air_quality(df, kind)
    for low, high in DAILY_RELATIONSHIPS:
        for a, b in ((10.0, 30.0), (10.0, 10.0), (30.0, 10.0), (None, 10.0), (10.0, None), (None, None)):
            schema = 'location_id long,snapshot_at timestamp,latitude double,longitude double,forecast_date date,_rescued_data string,temperature_2m_min double,temperature_2m_max double,apparent_temperature_min double,apparent_temperature_max double'
            names = [f.name for f in spark.createDataFrame([], schema).schema.fields]
            row = dict.fromkeys(names)
            row.update(location_id=1, snapshot_at=base, latitude=40.0, longitude=-90.0, forecast_date=base.date())
            row[low] = a
            row[high] = b
            if observations is not None:
                observations.append({'case': 'daily_relationship', 'row': row, 'low': low, 'high': high, 'expected_blocking': a is not None and b is not None and (a > b)})
            df = spark.createDataFrame([tuple((row[n] for n in names))], schema)
            if a is not None and b is not None and (a > b):
                expect_failure(lambda: validate_weather(df, 'daily'), low + '_le_' + high)
            else:
                validate_weather(df, 'daily')
    result = measurement_completeness(spark.createDataFrame([(None,), (1.0,)], 'temperature_2m double'), 'weather')
    _check(result.checks['measurement_empty_rows'] == 1 and result.warnings, "result.checks['measurement_empty_rows'] == 1 and result.warnings")
    return {'nullable_domains': True}

def spark_timestamp_cases(spark, observations=None):
    from pyspark.sql import functions as F
    from src.validation.common import timestamp
    for value in ('2026-10-21T00:00', '2026-10-21T00:00:00Z', '2026-10-21T00:00:00+00:00', '2026-10-21T05:30:00+05:30', '2026-10-21T00:00:00.123456Z'):
        if observations is not None:
            observations.append({'case': 'timestamp_parse', 'input': value, 'expected_utc': timestamp(value).isoformat()})
        parsed = spark.range(1).select(F.to_timestamp(F.lit(value)).alias('t')).selectExpr('unix_micros(t) as micros').first().micros
        delta=timestamp(value)-datetime(1970,1,1,tzinfo=timezone.utc)
        expected=(delta.days*86400+delta.seconds)*1000000+delta.microseconds
        _check(parsed==expected,(value,parsed,expected))
    value = '2026-10-21'
    _check(spark.range(1).select(F.to_date(F.lit(value)).alias('t')).first().t == timestamp(value, kind='date'), "spark.range(1).select(F.to_date(F.lit(value)).alias('t')).first().t == timestamp(value, kind='date')")
    return {'spark_parsing': True}

def _check(condition, message):
    if not condition:
        raise AssertionError(message)
