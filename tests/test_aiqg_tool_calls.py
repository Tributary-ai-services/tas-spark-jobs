"""Tool-call events (Plan #22 T.7): parse com.tas.aiqg.tool_call.v1 and merge
the proxy's and the gateway's sighting of one call into one row.

Three layers:
  * schema / source checks (always run);
  * the Spark projection, on a local SparkSession (skipped without Java);
  * the merge SQL, against a real TimescaleDB with aether-shared migration 009
    applied (skipped unless TOOL_CALL_TEST_DSN is set), e.g.
      docker run -d --name ts -e POSTGRES_PASSWORD=test -p 55439:5432 \\
          timescale/timescaledb:2.13.0-pg15
      psql ... -f aether-shared/.../timescaledb-migrations/009_tool_call.sql
      TOOL_CALL_TEST_DSN="host=localhost port=55439 dbname=postgres user=postgres password=test" \\
          python -m pytest tests/test_aiqg_tool_calls.py
"""

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


# tool_calls.py does `from schema import ...` (the job runs with its own
# directory on sys.path). tests/test_schema.py loads a different `schema`, so
# swap the AIQG one in only while tool_calls is imported.
_schema = _load('aiqg_schema_tc', 'schema.py')
_saved = sys.modules.get('schema')
sys.modules['schema'] = _schema
try:
    tc = _load('aiqg_tool_calls', 'tool_calls.py')
finally:
    if _saved is not None:
        sys.modules['schema'] = _saved
    else:
        del sys.modules['schema']

CONTRACT_FIELDS = (
    'tenant_id', 'token_id', 'owner_sub', 'user_id', 'agent_id', 'source_app',
    'flow_id', 'step_id', 'tool_call_id', 'tool_name', 'server_id', 'observed_by',
    'outcome', 'duration_ms', 'round_trip_ms', 'result_bytes', 'result_tokens_est',
    'reduced_bytes', 'scan_skipped', 'occurred_at',
)  # + findings, taken from the raw JSON


# --------------------------------------------------------------------------
# Schema / source
# --------------------------------------------------------------------------

def test_tool_call_data_has_every_contract_field():
    names = {f.name: f for f in _schema.TOOL_CALL_DATA.fields}
    for n in CONTRACT_FIELDS:
        assert n in names, n
        assert names[n].nullable, n


def test_envelope_has_ce_id():
    """event_id (part of the PK) is the CloudEvent id."""
    assert 'id' in {f.name for f in _schema.AIQG_TOOL_CALL_ENVELOPE.fields}


def test_columns_match_migration_009():
    """Every column the writer sets exists in aether-shared's 009, and back."""
    root = os.environ.get('AETHER_SHARED_DIR',
                          os.path.join(os.path.dirname(__file__), '..', '..', 'aether-shared'))
    mig = os.path.join(root, 'k8s-shared-infrastructure', 'timescaledb-migrations', '009_tool_call.sql')
    if not os.path.exists(mig):
        pytest.skip('aether-shared 009 not checked out beside this repo')
    ddl = open(mig).read().split('CREATE TABLE IF NOT EXISTS aiqg.tool_call_events (')[1].split('CONSTRAINT')[0]
    table_cols = {line.split()[0] for line in ddl.splitlines() if line.strip() and not line.strip().startswith('--')}
    assert set(tc.COLUMNS) == table_cols - {'ingested_at'}


def test_merge_sql_names_every_column_and_the_uuid_guard():
    sql = '\n'.join(tc.merge_statements('stage_x'))
    for c in tc.COLUMNS:
        assert c in sql, c
    assert f"token_id ~* '{tc.UUID_RE}'" in sql
    assert 'FROM stage_x' in sql


def test_uuid_guard_regex():
    import re
    rx = re.compile(tc.UUID_RE, re.IGNORECASE)
    assert rx.match('8f29197c-818b-4b69-bf59-200f5c3228f5')
    for bad in ('', 'tok-1', "x'; drop table t; --"):
        assert not rx.match(bad)


def test_main_runs_a_separate_tool_call_query():
    src = open(os.path.join(_JOBS, 'main.py')).read()
    assert 'foreachBatch(write_tool_call_batch)' in src
    assert '"spark-aiqg-aggregator-tool-calls"' in src
    assert '/tool_calls/' in src
    assert 'awaitAnyTermination' in src


# --------------------------------------------------------------------------
# Spark projection
# --------------------------------------------------------------------------

def _event(**over):
    data = {
        'tenant_id': 't1', 'token_id': '8f29197c-818b-4b69-bf59-200f5c3228f5',
        'owner_sub': 'kc-a', 'tool_call_id': 'call_1', 'tool_name': 'draw',
        'server_id': 'napkin-mcp', 'observed_by': 'proxy', 'outcome': 'ok',
        'duration_ms': 120, 'result_bytes': 2048, 'result_tokens_est': 512,
        'reduced_bytes': 100, 'findings': {'email': 2, 'ssn': 1}, 'scan_skipped': False,
        'occurred_at': '2026-11-21T10:00:00Z',
    }
    data.update(over.pop('data', {}))
    ev = {'specversion': '1.0', 'type': tc.TOOL_CALL_TYPE, 'source': 'urn:tas:service:tas-mcp',
          'id': str(uuid.uuid4()), 'time': '2026-11-21T10:00:01Z', 'data': data}
    ev.update(over)
    return json.dumps(ev)


@pytest.fixture(scope='module')
def spark():
    if not shutil.which('java'):
        pytest.skip('no Java for a local SparkSession')
    # Workers must run the same Python minor version as this driver.
    os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
    # collect() turns timestamps into naive local datetimes; pin the local zone.
    import time
    os.environ['TZ'] = 'UTC'
    time.tzset()
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master('local[1]').appName('tool-call-tests')
         .config('spark.sql.session.timeZone', 'UTC').config('spark.ui.enabled', 'false').getOrCreate())
    yield s
    s.stop()


def _parse(spark, values):
    import datetime
    rows = [(datetime.datetime(2026, 11, 21, 12, 0, 0), bytearray(v.encode())) for v in values]
    df = spark.createDataFrame(rows, 'timestamp timestamp, value binary')
    return [r.asDict() for r in tc.parse_tool_call_envelopes(df).collect()]


def test_parse_projects_contract(spark):
    out = _parse(spark, [_event(id='ev-1')])
    assert len(out) == 1
    r = out[0]
    assert r['event_id'] == 'ev-1' and r['tenant_id'] == 't1' and r['tool_name'] == 'draw'
    assert r['observed_by'] == 'proxy' and r['outcome'] == 'ok' and r['duration_ms'] == 120
    assert json.loads(r['findings']) == {'email': 2, 'ssn': 1}
    assert r['occurred_at'].isoformat().startswith('2026-11-21T10:00:00')
    assert set(r) == set(tc.COLUMNS) - {'gateway_event_id'}


def test_parse_drops_what_the_table_would_reject(spark):
    out = _parse(spark, [
        _event(type='com.tas.aiqg.response.v1'),
        _event(data={'tenant_id': None, 'observed_by': 'gateway'}),
        _event(data={'tool_name': ''}),
        _event(data={'observed_by': 'both'}),     # only the writer may say 'both'
        _event(data={'outcome': 'success'}),
        _event(id=None),
        _event(id='keep'),
    ])
    assert [r['event_id'] for r in out] == ['keep']


def test_parse_keeps_untokened_proxy_calls_as_unattributed(spark):
    out = _parse(spark, [
        _event(id='p1', data={'tenant_id': None, 'token_id': None, 'owner_sub': None, 'observed_by': 'proxy'}),
        _event(id='p2', data={'tenant_id': '', 'observed_by': 'proxy'}),
    ])
    assert sorted(r['event_id'] for r in out) == ['p1', 'p2']
    assert {r['tenant_id'] for r in out} == {tc.UNATTRIBUTED}


def test_parse_time_falls_back_to_ce_time_then_kafka(spark):
    a = _parse(spark, [_event(data={'occurred_at': None})])[0]
    assert a['occurred_at'].isoformat().startswith('2026-11-21T10:00:01')
    b = _parse(spark, [_event(time=None, data={'occurred_at': None})])[0]
    assert b['occurred_at'].isoformat().startswith('2026-11-21T12:00:00')


def test_parse_accepts_no_result_basis_without_storing_it(spark):
    out = _parse(spark, [_event(id='g1', data={'observed_by': 'gateway', 'outcome': 'no_result',
                                               'no_result_basis': 'timeout'}),
                         _event(id='g2', data={'observed_by': 'gateway', 'outcome': 'no_result',
                                               'no_result_basis': 'sibling'})])
    assert sorted(r['event_id'] for r in out) == ['g1', 'g2']
    assert {r['outcome'] for r in out} == {'no_result'}
    assert 'no_result_basis' not in out[0]


def test_parse_gateway_event_without_server_or_findings(spark):
    r = _parse(spark, [_event(data={'observed_by': 'gateway', 'server_id': None, 'findings': None,
                                    'duration_ms': None, 'round_trip_ms': 3400})])[0]
    assert r['server_id'] is None and r['findings'] is None and r['round_trip_ms'] == 3400


# --------------------------------------------------------------------------
# Merge SQL against TimescaleDB
# --------------------------------------------------------------------------

STAGE_DDL = """
CREATE TABLE {t} (
    occurred_at timestamptz, tenant_id text, event_id text, token_id text, owner_sub text,
    user_id text, agent_id text, source_app text, flow_id text, step_id text,
    tool_call_id text, tool_name text, server_id text, observed_by text, outcome text,
    duration_ms int, round_trip_ms int, result_bytes bigint, result_tokens_est int,
    reduced_bytes bigint, findings text, scan_skipped boolean)
"""
STAGE_COLS = ('occurred_at', 'tenant_id', 'event_id', 'token_id', 'owner_sub', 'user_id', 'agent_id',
              'source_app', 'flow_id', 'step_id', 'tool_call_id', 'tool_name', 'server_id',
              'observed_by', 'outcome', 'duration_ms', 'round_trip_ms', 'result_bytes',
              'result_tokens_est', 'reduced_bytes', 'findings', 'scan_skipped')


@pytest.fixture
def db():
    dsn = os.environ.get('TOOL_CALL_TEST_DSN')
    if not dsn:
        pytest.skip('TOOL_CALL_TEST_DSN not set')
    import psycopg2
    conn = psycopg2.connect(dsn)
    tenant = 'tc-test-' + uuid.uuid4().hex[:8]
    yield conn, tenant
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute('DELETE FROM aiqg.tool_call_events WHERE tenant_id = %s', (tenant,))
    conn.commit()
    conn.close()


def _row(tenant, **kw):
    r = dict.fromkeys(STAGE_COLS)
    r.update(occurred_at='2026-11-21T10:00:00Z', tenant_id=tenant, event_id=str(uuid.uuid4()),
             tool_name='draw', observed_by='proxy', outcome='ok')
    r.update(kw)
    return r


def _batch(conn, rows):
    t = 'aiqg_tool_call_stage_t' + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(STAGE_DDL.format(t=t))
        for r in rows:
            cur.execute(f"INSERT INTO {t} ({', '.join(STAGE_COLS)}) VALUES ({', '.join(['%s'] * len(STAGE_COLS))})",
                        [r[c] for c in STAGE_COLS])
    conn.commit()
    tc.apply_batch(conn, t)


def _rows(conn, tenant):
    from psycopg2.extras import RealDictCursor
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT * FROM aiqg.tool_call_events WHERE tenant_id = %s ORDER BY occurred_at, event_id',
                    (tenant,))
        return cur.fetchall()


def _proxy(tenant, **kw):
    base = dict(event_id='p1', tool_call_id='call_1', server_id='napkin-mcp', observed_by='proxy',
                outcome='ok', duration_ms=120, result_bytes=2048, result_tokens_est=512,
                findings='{"email": 2}', scan_skipped=False, token_id='8f29197c-818b-4b69-bf59-200f5c3228f5',
                owner_sub='kc-proxy', occurred_at='2026-11-21T10:00:00Z')
    base.update(kw)
    return _row(tenant, **base)


def _gateway(tenant, **kw):
    base = dict(event_id='g1', tool_call_id='call_1', tool_name='mcp__napkin__draw', observed_by='gateway',
                outcome='ok', round_trip_ms=3400, result_tokens_est=600, findings='{"email": 5}',
                scan_skipped=True, owner_sub='kc-gw', agent_id='agent-7', occurred_at='2026-11-21T10:00:05Z')
    base.update(kw)
    return _row(tenant, **base)


def _assert_merged(r):
    assert r['observed_by'] == 'both'
    assert r['event_id'] == 'p1' and r['gateway_event_id'] == 'g1'
    assert r['occurred_at'].isoformat().startswith('2026-11-21T10:00:00')   # the proxy's time
    assert r['tool_name'] == 'draw' and r['server_id'] == 'napkin-mcp'        # proxy wins
    assert r['owner_sub'] == 'kc-proxy' and r['result_tokens_est'] == 512
    assert r['findings'] == {'email': 2} and r['scan_skipped'] is False      # counted once, at the proxy
    assert r['duration_ms'] == 120 and r['round_trip_ms'] == 3400            # each from its source
    assert r['agent_id'] == 'agent-7'                                        # gateway fills a proxy gap


def test_gateway_then_proxy_across_batches_merge(db):
    conn, t = db
    _batch(conn, [_gateway(t)])
    assert [r['observed_by'] for r in _rows(conn, t)] == ['gateway']
    _batch(conn, [_proxy(t)])
    rows = _rows(conn, t)
    assert len(rows) == 1
    _assert_merged(rows[0])


def test_proxy_then_gateway_and_both_in_one_batch_merge(db):
    conn, t = db
    _batch(conn, [_proxy(t)])
    _batch(conn, [_gateway(t)])
    rows = _rows(conn, t)
    assert len(rows) == 1
    _assert_merged(rows[0])

    conn2, t2 = conn, t + '-b'
    try:
        _batch(conn2, [_gateway(t2), _proxy(t2)])
        rows = _rows(conn2, t2)
        assert len(rows) == 1
        _assert_merged(rows[0])
    finally:
        with conn2.cursor() as cur:
            cur.execute('DELETE FROM aiqg.tool_call_events WHERE tenant_id = %s', (t2,))
        conn2.commit()


def test_redelivery_is_idempotent(db):
    conn, t = db
    _batch(conn, [_proxy(t), _gateway(t)])
    first = _rows(conn, t)
    _batch(conn, [_proxy(t)])
    _batch(conn, [_gateway(t)])
    _batch(conn, [_proxy(t), _gateway(t), _proxy(t)])
    again = _rows(conn, t)
    assert len(again) == 1
    strip = lambda r: {k: v for k, v in r.items() if k != 'ingested_at'}  # noqa: E731
    assert strip(again[0]) == strip(first[0])


def test_proxy_outcome_wins_over_gateway_no_result(db):
    """A gateway no_result can be the gateway losing its own echo work, so a
    proxy outcome always wins; no_result lands only on gateway-only rows."""
    conn, t = db
    _batch(conn, [_proxy(t), _gateway(t, outcome='no_result')])
    rows = _rows(conn, t)
    assert len(rows) == 1 and rows[0]['outcome'] == 'ok' and rows[0]['observed_by'] == 'both'
    # either arrival order, across batches, and under re-delivery
    _batch(conn, [_gateway(t, outcome='no_result')])
    _batch(conn, [_proxy(t)])
    rows = _rows(conn, t)
    assert len(rows) == 1 and rows[0]['outcome'] == 'ok' and rows[0]['observed_by'] == 'both'

    _batch(conn, [_gateway(t, event_id='g3', tool_call_id='call_3', outcome='no_result')])
    _batch(conn, [_proxy(t, event_id='p3', tool_call_id='call_3')])
    r3 = [r for r in _rows(conn, t) if r['tool_call_id'] == 'call_3']
    assert len(r3) == 1 and r3[0]['outcome'] == 'ok' and r3[0]['observed_by'] == 'both'

    _batch(conn, [_gateway(t, event_id='g4', tool_call_id='call_4', outcome='no_result')])
    r4 = [r for r in _rows(conn, t) if r['tool_call_id'] == 'call_4']
    assert len(r4) == 1 and r4[0]['outcome'] == 'no_result' and r4[0]['observed_by'] == 'gateway'

    _batch(conn, [_proxy(t, event_id='p2', tool_call_id='call_2', outcome='tool_error'),
                  _gateway(t, event_id='g2', tool_call_id='call_2', outcome='no_result')])
    r2 = [r for r in _rows(conn, t) if r['tool_call_id'] == 'call_2']
    assert len(r2) == 1 and r2[0]['outcome'] == 'tool_error' and r2[0]['observed_by'] == 'both'


def test_non_uuid_token_is_null_not_a_failed_batch(db):
    conn, t = db
    _batch(conn, [_proxy(t, token_id='tok-handwritten'), _proxy(t, event_id='p9', tool_call_id='c9', token_id='')])
    assert [r['token_id'] for r in _rows(conn, t)] == [None, None]


def test_rows_without_tool_call_id_are_never_merged(db):
    conn, t = db
    _batch(conn, [_proxy(t, tool_call_id=None), _gateway(t, tool_call_id=None),
                  _proxy(t, event_id='p3', tool_call_id='')])
    rows = _rows(conn, t)
    assert sorted(r['observed_by'] for r in rows) == ['gateway', 'proxy', 'proxy']
    _batch(conn, [_proxy(t, tool_call_id=None)])  # re-delivery: no-op
    assert len(_rows(conn, t)) == 3


def test_reused_tool_call_id_is_not_merged(db):
    """Two proxy calls with one id (a client reusing "call_0") stay two calls."""
    conn, t = db
    _batch(conn, [_proxy(t, tool_call_id='call_0')])
    _batch(conn, [_proxy(t, event_id='p2', tool_call_id='call_0', occurred_at='2026-11-21T10:01:00Z'),
                  _gateway(t, tool_call_id='call_0')])
    rows = _rows(conn, t)
    assert len(rows) == 3
    assert sorted(r['observed_by'] for r in rows) == ['gateway', 'proxy', 'proxy']


def test_partner_outside_window_is_not_merged(db):
    conn, t = db
    _batch(conn, [_gateway(t, occurred_at='2026-11-19T10:00:00Z')])
    _batch(conn, [_proxy(t)])
    assert sorted(r['observed_by'] for r in _rows(conn, t)) == ['gateway', 'proxy']


def test_merge_is_per_tenant(db):
    conn, t = db
    other = t + '-o'
    try:
        _batch(conn, [_gateway(other)])
        _batch(conn, [_proxy(t)])
        assert [r['observed_by'] for r in _rows(conn, t)] == ['proxy']
        assert [r['observed_by'] for r in _rows(conn, other)] == ['gateway']
    finally:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM aiqg.tool_call_events WHERE tenant_id = %s', (other,))
        conn.commit()


def test_malformed_findings_are_dropped_not_fatal(db):
    conn, t = db
    _batch(conn, [_proxy(t, findings='[1,2]'), _proxy(t, event_id='p5', tool_call_id='c5', findings='')])
    assert [r['findings'] for r in _rows(conn, t)] == [None, None]
