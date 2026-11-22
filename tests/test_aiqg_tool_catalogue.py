"""Tool catalogue (Plan #22 T.6/T.7): proxy com.tas.aiqg.tool_catalogue.v1 and
gateway response tools_offered -> aiqg.tool_catalogue.

Same three layers as test_aiqg_tool_calls.py: source checks, the Spark
projection on a local SparkSession (skipped without Java), and the insert SQL
against a TimescaleDB with aether-shared migration 009 applied (skipped unless
TOOL_CALL_TEST_DSN is set).
"""

import datetime
import importlib.util
import json
import os
import shutil
import sys
import uuid

import pytest

_JOBS = os.path.join(os.path.dirname(__file__), '..', 'jobs', 'aiqg_aggregator')


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_JOBS, filename))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_schema = _load('aiqg_schema_cat', 'schema.py')
_saved = sys.modules.get('schema')
sys.modules['schema'] = _schema
try:
    cat = _load('aiqg_tool_catalogue', 'tool_catalogue.py')
finally:
    if _saved is not None:
        sys.modules['schema'] = _saved
    else:
        del sys.modules['schema']


# --------------------------------------------------------------------------
# Schema / source
# --------------------------------------------------------------------------

def _migration_009():
    root = os.environ.get('AETHER_SHARED_DIR',
                          os.path.join(os.path.dirname(__file__), '..', '..', 'aether-shared'))
    mig = os.path.join(root, 'k8s-shared-infrastructure', 'timescaledb-migrations', '009_tool_call.sql')
    if not os.path.exists(mig):
        pytest.skip('aether-shared 009 not checked out beside this repo')
    return open(mig).read()


def test_columns_and_key_match_migration_009():
    sql = _migration_009()
    ddl = sql.split('CREATE TABLE IF NOT EXISTS aiqg.tool_catalogue (')[1].split('CONSTRAINT')[0]
    table_cols = {line.split()[0] for line in ddl.splitlines() if line.strip() and not line.strip().startswith('--')}
    assert set(cat.COLUMNS) == table_cols
    key = sql.split('ON aiqg.tool_catalogue (')[1].split(')')[0]
    assert tuple(c.strip() for c in key.split(',')) == cat.SNAPSHOT_KEY
    assert 'NULLS NOT DISTINCT' in sql.split('tool_catalogue_snapshot_key')[1].split(';')[0]


def test_insert_sql_is_on_conflict_do_nothing_on_the_key():
    sql = cat.insert_statement('stage_x')
    assert f"ON CONFLICT ({', '.join(cat.SNAPSHOT_KEY)}) DO NOTHING" in sql
    assert "date_trunc('hour', snapshot_at, 'UTC')" in sql
    assert 'FROM stage_x' in sql


def test_main_runs_a_third_query_and_still_stops_on_any_failure():
    src = open(os.path.join(_JOBS, 'main.py')).read()
    assert 'foreachBatch(write_tool_catalogue_batch)' in src
    assert '"spark-aiqg-aggregator-tool-catalogue"' in src
    assert '/tool_catalogue/' in src
    assert src.count('.start()') == 3
    assert 'awaitAnyTermination' in src
    # The response projection is untouched by the catalogue.
    assert 'tools_offered' not in src.split('def parse_response_envelopes')[1].split('def _jdbc')[0]


def test_response_envelope_schema_unchanged_by_catalogue():
    names = {f.name for f in _schema.RESPONSE_DATA.fields}
    assert 'tools_offered' not in names


# --------------------------------------------------------------------------
# Spark projection
# --------------------------------------------------------------------------

@pytest.fixture(scope='module')
def spark():
    if not shutil.which('java'):
        pytest.skip('no Java for a local SparkSession')
    os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
    import time
    os.environ['TZ'] = 'UTC'
    time.tzset()
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master('local[1]').appName('tool-catalogue-tests')
         .config('spark.sql.session.timeZone', 'UTC').config('spark.ui.enabled', 'false').getOrCreate())
    yield s
    s.stop()


def _proxy_event(**over):
    data = {'snapshot_at': '2026-11-21T10:00:00Z', 'server_id': 'napkin-mcp', 'observed_by': 'proxy',
            'tenant_id': None, 'tools': [{'name': 'draw', 'schema_bytes': 310},
                                         {'name': 'erase', 'schema_bytes': 120}]}
    data.update(over.pop('data', {}))
    ev = {'specversion': '1.0', 'type': cat.TOOL_CATALOGUE_TYPE, 'source': 'urn:tas:service:tas-mcp',
          'id': str(uuid.uuid4()), 'time': '2026-11-21T10:05:00Z', 'data': data}
    ev.update(over)
    return json.dumps(ev)


def _response_event(**over):
    data = {'tenant_id': 't1', 'response_event_id': str(uuid.uuid4()), 'vendor': 'anthropic',
            'tools_schema_bytes': 200,
            'tools_offered': [{'name': 'get_weather', 'schema_bytes': 150}, {'name': 'draw', 'schema_bytes': 50}]}
    data.update(over.pop('data', {}))
    ev = {'specversion': '1.0', 'type': 'com.tas.aiqg.response.v1', 'source': 'urn:tas:service:tas-llm-router',
          'id': str(uuid.uuid4()), 'time': '2026-11-21T10:42:17.5Z', 'data': data}
    ev.update(over)
    return json.dumps(ev)


def _parse(spark, values):
    rows = [(datetime.datetime(2026, 11, 21, 12, 30, 0), bytearray(v.encode())) for v in values]
    df = spark.createDataFrame(rows, 'timestamp timestamp, value binary')
    return sorted((r.asDict() for r in cat.parse_tool_catalogue_envelopes(df).collect()),
                  key=lambda r: (r['observed_by'], r['tool_name']))


def test_proxy_event_is_one_row_per_tool(spark):
    out = _parse(spark, [_proxy_event()])
    assert [(r['tool_name'], r['schema_bytes']) for r in out] == [('draw', 310), ('erase', 120)]
    for r in out:
        assert r['server_id'] == 'napkin-mcp' and r['observed_by'] == 'proxy' and r['tenant_id'] is None
        assert r['snapshot_at'] == datetime.datetime(2026, 11, 21, 10, 0)
    assert set(out[0]) == set(cat.COLUMNS)


def test_gateway_tools_offered_explodes_with_event_time_and_tenant(spark):
    out = _parse(spark, [_response_event()])
    assert [(r['tool_name'], r['schema_bytes']) for r in out] == [('draw', 50), ('get_weather', 150)]
    for r in out:
        assert r['observed_by'] == 'gateway' and r['server_id'] is None and r['tenant_id'] == 't1'
        assert r['snapshot_at'] == datetime.datetime(2026, 11, 21, 10, 42, 17, 500000)  # truncated in SQL


def test_response_without_tools_offered_yields_nothing(spark):
    # The usual case: the gateway sends tools_offered once per toolset per hour.
    assert _parse(spark, [_response_event(data={'tools_offered': None}),
                          _response_event(data={'tools_offered': []})]) == []


def test_gateway_server_id_is_never_taken_from_the_event(spark):
    out = _parse(spark, [_response_event(data={'server_id': 'x'})])
    assert {r['server_id'] for r in out} == {None}


def test_malformed_is_dropped_not_fatal(spark):
    out = _parse(spark, [
        'not json',
        _proxy_event(specversion='0.3'),
        _proxy_event(type='com.tas.aiqg.request.v1'),
        _proxy_event(data={'server_id': None}),                  # proxy row needs a server
        _proxy_event(data={'server_id': ''}),
        _proxy_event(data={'observed_by': 'both'}),               # CHECK would reject
        _proxy_event(data={'observed_by': None}),
        _proxy_event(data={'tools': {'name': 'draw'}}),           # not an array
        _proxy_event(data={'tools': 'draw'}),
        _response_event(data={'tenant_id': None}),                # gateway row needs a tenant
        _response_event(data={'tenant_id': ''}),
        _response_event(data={'tools_offered': [None, {'name': ''}, {'name': '  '}, {'schema_bytes': 9},
                                                {'name': 'keep_gw', 'schema_bytes': 'big'}]}),
        _proxy_event(data={'tools': [{'name': 'keep_px', 'schema_bytes': 99999999999}]}),
    ])
    assert [(r['observed_by'], r['tool_name'], r['schema_bytes']) for r in out] == [
        ('gateway', 'keep_gw', None), ('proxy', 'keep_px', 99999999999)]  # range-checked in SQL


def test_snapshot_at_falls_back_to_ce_time_then_kafka(spark):
    a = _parse(spark, [_proxy_event(data={'snapshot_at': None, 'tools': [{'name': 'a'}]})])[0]
    assert a['snapshot_at'] == datetime.datetime(2026, 11, 21, 10, 5)
    b = _parse(spark, [_proxy_event(data={'snapshot_at': 'garbage', 'tools': [{'name': 'a'}]})])[0]
    assert b['snapshot_at'] == datetime.datetime(2026, 11, 21, 10, 5)
    c = _parse(spark, [_proxy_event(time=None, data={'snapshot_at': None, 'tools': [{'name': 'a'}]})])[0]
    assert c['snapshot_at'] == datetime.datetime(2026, 11, 21, 12, 30)
    d = _parse(spark, [_response_event(time=None)])[0]
    assert d['snapshot_at'] == datetime.datetime(2026, 11, 21, 12, 30)


def test_proxy_tenant_is_kept_when_present(spark):
    out = _parse(spark, [_proxy_event(data={'tenant_id': 't9', 'tools': [{'name': 'a'}]})])
    assert out[0]['tenant_id'] == 't9'


# --------------------------------------------------------------------------
# Insert SQL against TimescaleDB
# --------------------------------------------------------------------------

STAGE_DDL = """
CREATE TABLE {t} (snapshot_at timestamptz, server_id text, tool_name text,
                  schema_bytes bigint, observed_by text, tenant_id text)
"""


@pytest.fixture
def db():
    dsn = os.environ.get('TOOL_CALL_TEST_DSN')
    if not dsn:
        pytest.skip('TOOL_CALL_TEST_DSN not set')
    import psycopg2
    conn = psycopg2.connect(dsn)
    tag = 'cat-test-' + uuid.uuid4().hex[:8]
    yield conn, tag
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute('DELETE FROM aiqg.tool_catalogue WHERE tenant_id LIKE %s OR server_id LIKE %s',
                    (tag + '%', tag + '%'))
    conn.commit()
    conn.close()


def _row(**kw):
    r = dict.fromkeys(cat.COLUMNS)
    r.update(snapshot_at='2026-11-21T10:00:00Z', tool_name='draw', schema_bytes=100, observed_by='proxy')
    r.update(kw)
    return r


def _batch(conn, rows):
    t = 'aiqg_tool_catalogue_stage_t' + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(STAGE_DDL.format(t=t))
        for r in rows:
            cur.execute(f"INSERT INTO {t} ({', '.join(cat.COLUMNS)}) VALUES ({', '.join(['%s'] * len(cat.COLUMNS))})",
                        [r[c] for c in cat.COLUMNS])
    conn.commit()
    n = cat.apply_batch(conn, t)
    with conn.cursor() as cur:
        cur.execute('SELECT to_regclass(%s)', (t,))
        assert cur.fetchone()[0] is None, 'staging table left behind'
    return n


def _rows(conn, tag):
    from psycopg2.extras import RealDictCursor
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT * FROM aiqg.tool_catalogue WHERE tenant_id LIKE %s OR server_id LIKE %s '
                    'ORDER BY observed_by, tool_name, snapshot_at',
                    (tag + '%', tag + '%'))
        return cur.fetchall()


def test_proxy_rows_with_null_tenant_dedupe_on_redelivery(db):
    conn, tag = db
    srv = tag + '-napkin'
    rows = [_row(server_id=srv, tool_name='draw'), _row(server_id=srv, tool_name='erase', schema_bytes=50)]
    assert _batch(conn, rows) == 2
    assert _batch(conn, rows) == 0                       # re-delivery: NULL tenant is still a duplicate
    assert _batch(conn, rows + rows) == 0                # and within one batch
    got = _rows(conn, tag)
    assert [(r['tool_name'], r['schema_bytes'], r['tenant_id']) for r in got] == [
        ('draw', 100, None), ('erase', 50, None)]


def test_gateway_rows_truncate_to_the_hour_and_dedupe_across_replicas(db):
    conn, tag = db
    t = tag + '-tenant'
    # Three replicas report the same toolset within one hour, at different times.
    rows = [_row(observed_by='gateway', tenant_id=t, snapshot_at=ts, tool_name='get_weather')
            for ts in ('2026-11-21T10:42:17.5Z', '2026-11-21T10:05:00Z', '2026-11-21T10:59:59.999Z')]
    assert _batch(conn, rows) == 1
    assert _batch(conn, [_row(observed_by='gateway', tenant_id=t, snapshot_at='2026-11-21T11:00:00Z',
                              tool_name='get_weather')]) == 1     # next hour is a new snapshot
    got = _rows(conn, tag)
    assert [r['snapshot_at'] for r in got] == [datetime.datetime(2026, 11, 21, 10, tzinfo=datetime.timezone.utc),
                                               datetime.datetime(2026, 11, 21, 11, tzinfo=datetime.timezone.utc)]
    assert {r['server_id'] for r in got} == {None}


def test_truncation_is_utc_whatever_the_session_zone(db):
    conn, tag = db
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'Asia/Kolkata'")          # +05:30
    _batch(conn, [_row(server_id=tag + '-s', snapshot_at='2026-11-21T10:20:00Z')])
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    assert _rows(conn, tag)[0]['snapshot_at'] == datetime.datetime(2026, 11, 21, 10, tzinfo=datetime.timezone.utc)


def test_same_tool_from_proxy_and_gateway_are_two_rows(db):
    conn, tag = db
    assert _batch(conn, [_row(server_id=tag + '-s'),
                         _row(observed_by='gateway', tenant_id=tag + '-t')]) == 2


def test_bad_sizes_are_null_and_bad_rows_skipped_not_fatal(db):
    conn, tag = db
    srv = tag + '-s'
    n = _batch(conn, [
        _row(server_id=srv, tool_name='neg', schema_bytes=-1),
        _row(server_id=srv, tool_name='huge', schema_bytes=99999999999),
        _row(server_id=srv, tool_name='max', schema_bytes=2147483647),
        _row(server_id=srv, tool_name='nosize', schema_bytes=None),
        _row(server_id=srv, tool_name='both', observed_by='both'),     # CHECK
        _row(server_id=srv, tool_name=None),                           # NOT NULL
        _row(server_id=srv, tool_name=''),
        _row(server_id=srv, tool_name='notime', snapshot_at=None),     # NOT NULL
        _row(server_id='', tenant_id=tag + '-t', tool_name='blank_server'),
    ])
    assert n == 5
    got = {r['tool_name']: r for r in _rows(conn, tag)}
    assert set(got) == {'neg', 'huge', 'max', 'nosize', 'blank_server'}
    assert got['neg']['schema_bytes'] is None and got['huge']['schema_bytes'] is None
    assert got['max']['schema_bytes'] == 2147483647
    assert got['blank_server']['server_id'] is None


def test_first_write_of_a_key_wins(db):
    conn, tag = db
    srv = tag + '-s'
    _batch(conn, [_row(server_id=srv, schema_bytes=100)])
    _batch(conn, [_row(server_id=srv, schema_bytes=999)])
    assert [r['schema_bytes'] for r in _rows(conn, tag)] == [100]
