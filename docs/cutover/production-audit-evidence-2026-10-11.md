# Production audit evidence — 2026-10-11

Class: campaign evidence. Retires with [the assessment](production-audit-2026-10-11.md); archive them together. This is recorded evidence at main `07be1dc930711bb927204f3df99d08526c9aaef6`, fork `076d5f982d1d2ef352c3bb5d76d02cff27bc164a`. Reproductions used only synthetic credentials and task-owned local directories. No cloud writes or remote security probes occurred.

## Build and execution conditions

A managed worktree isolates branch `docs/production-bugs-audit-2026-10-11` from the dirty original checkout. The initial disk check reported 733 GiB available. Before local Python tests it reported 703 GiB. Builds reused the original repository's Cargo target and used four build jobs. Shared build caches are preserved.

`uv sync --locked --no-install-package repark --no-install-package repark-parity --package repark --extra numpy --extra pandas --extra polars --extra ml-ext` provisioned this worktree's environment. `CARGO_TARGET_DIR=<shared-target> CARGO_BUILD_JOBS=4 make develop` built the current editable module. The first develop attempt lacked a worktree virtual environment; that setup issue was resolved before engine probes. The successful build reported:

```text
Finished dev profile [unoptimized + debuginfo]
Built wheel repark-1.5.4-cp312-abi3-linux_x86_64.whl
Installed repark-1.5.4
repark.__version__: 1.5.4
repark._native: <audit-worktree>/python/repark/src/repark/_native.abi3.so
```

No timing from this development build is used as a release performance baseline. Runtime probes were separate processes. Run scripts from the pinned repository root with its fresh native environment, except where a different oracle/dependency environment is explicitly stated. Use new temporary fixture paths when repeating probes; memory-catalog runs leave local files, so a reused warehouse is not a clean control.

## Final validation status

The targeted current-build suite completed: **526 passed, 33 warnings in 370.89 seconds** (six minutes). It covered `test_ice_tstzns_wall_1.py`, `test_ice_tsns_merge_wall_1.py`, `test_cache_persist.py` and `test_enc_1.py`. Existing baseline-preservation tests and declared refusals are part of that pass count; it does not overturn the reproduced findings.

`make check-map-sync check-ledger-grammar check-ledgers` completed with exit 0: 374 maps clean, 328 live ledgers checked, frozen ledger rules clean. These counts are measurements of the staged audit docs before the performance report was assembled.

The first full `make verify` attempt stopped during compilation without a captured final status when the session changed models. It is **incomplete**, not a pass or a diagnosed repository failure. The next attempt stopped on an audit-assembly missing link before the performance document was transferred. After transfer, the assembled `make verify` completed with exit 2: its Rust test command returned 101 on PA-15. No source or test repair is included in this branch.

## PA-01: dbt profile diagnostic

The current source module was loaded directly; the original checkout virtual environment supplied only dbt dependencies. The native engine was not called. Both strings are synthetic sentinels.

```python
import importlib.util
from types import SimpleNamespace
from pathlib import Path
p=Path("python/dbt-repark/src/dbt/adapters/repark/session.py")
spec=importlib.util.spec_from_file_location("audited_dbt_session",p)
m=importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
first=SimpleNamespace(database="ice", warehouse=None, catalog_properties={"s3.secret-access-key":"AUDIT_LIVE_SENTINEL"},session_properties={})
second=SimpleNamespace(database="other", warehouse=None, catalog_properties={"s3.secret-access-key":"AUDIT_REQUEST_SENTINEL"},session_properties={})
m._ACTIVE.update(session=object(),key=m.session_key(first))
try:
 m.acquire_session(second)
except Exception as error:
 text=str(error)
 print("live_secret_in_exception=", "AUDIT_LIVE_SENTINEL" in text)
 print("requested_secret_in_exception=", "AUDIT_REQUEST_SENTINEL" in text)
 print("exception_type=",type(error).__name__)
```

Recorded stdout:

```text
live_secret_in_exception= True
requested_secret_in_exception= True
exception_type= DbtRuntimeError
```

## PA-02–04: local-file policy controls

Run with the fresh editable module. The direct refusal, wrapped write, allowed warehouse write, symlink case and Unicode error were measured in one session. Random output filenames may differ.

```python
from pathlib import Path
import json
import os
import repark
from repark import ReparkSession

root = Path('/tmp/repark-production-audit-2026-10-11/local-guard-fixture')
root.mkdir(exist_ok=True)
warehouse = root / 'warehouse'
outside = root / 'outside'
warehouse.mkdir(exist_ok=True)
outside.mkdir(exist_ok=True)
link = warehouse / 'link'
if not link.exists():
    link.symlink_to(outside, target_is_directory=True)
session = ReparkSession.builder.appName('local-production-review').getOrCreate()
session.register_memory_catalog('ice', str(warehouse))
print('version', repark.__version__)
os.chdir(root)
cases = {
    'direct_default': f"COPY (SELECT 1 AS a) TO '{outside / 'direct'}' STORED AS PARQUET",
    'explain_analyze_default': f"EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '{outside / 'wrapped'}' STORED AS PARQUET",
    'warehouse_control': f"COPY (SELECT 1 AS a) TO '{warehouse / 'control'}' STORED AS PARQUET",
    'symlink_missing_parents': f"COPY (SELECT 1 AS a) TO '{link / 'new' / 'sub' / 'export'}' STORED AS PARQUET",
    'unicode_location': "COPY (SELECT 1 AS a) TO 'aaaaaaé' STORED AS PARQUET",
}
for name, sql in cases.items():
    try:
        table = session.sql(sql).to_arrow()
        print(json.dumps({'case': name, 'result': 'accepted', 'rows': table.num_rows}))
    except BaseException as error:
        print(json.dumps({'case': name, 'result': 'error', 'type': type(error).__name__, 'message': str(error)[:1200]}))
print('files', json.dumps([str(p.relative_to(root)) for p in root.rglob('*.parquet')]))
session.stop()
```

Recorded stdout:

```text
version 1.5.4
{"case": "direct_default", "result": "error", "type": "AnalysisException", "message": "Error during planning: COPY TO to local filesystem path `/tmp/repark-production-audit-2026-10-11/local-guard-fixture/outside/direct` is disabled by default. Set conf `repark.sql.allowLocalFilesystemDDL` = true to allow local CREATE EXTERNAL TABLE / COPY TO outside the session warehouse root, or use a path under a registered warehouse (grandfather). Remote locations (s3://, s3a://) are unaffected."}
{"case": "explain_analyze_default", "result": "accepted", "rows": 1}
{"case": "warehouse_control", "result": "accepted", "rows": 1}
{"case": "symlink_missing_parents", "result": "accepted", "rows": 1}
{"case": "unicode_location", "result": "error", "type": "PySparkException", "message": "repark internal error in PyReparkSession.sql: end byte index 7 is not a char boundary; it is inside '\u00e9' (bytes 6..8 of string) (a Rust panic was caught at the Python boundary; this is a bug \u2014 please report it)"}
files ["warehouse/control/YtBMjyXxR1zySYzK_0.parquet", "outside/wrapped/xzUJ4efljmJ7FY2S_0.parquet", "outside/new/sub/export/I9WMYflugUHIZfMU_0.parquet"]
```

Recorded stderr:

```text

thread '<unnamed>' (165553) panicked at crates/repark-spark/src/local_fs_ddl.rs:53:42:
end byte index 7 is not a char boundary; it is inside 'é' (bytes 6..8 of string)
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
```

## PA-05: executing write carriers

Run with the fresh module. The existing fixture helper seeds Arrow inputs. Stored values are read from Parquet files, not formatted timestamp strings.

```python
from pathlib import Path
import sys
import json
sys.path.insert(0, str(Path.cwd() / 'python/repark/tests'))
import _ice_tsns_merge_wall_1_doors as doors
import pyarrow as pa
import pyarrow.parquet as pq

root = Path('/tmp/repark-production-audit-2026-10-11/write-carrier-fixture')
root.mkdir(exist_ok=True)
session = doors.open_session('America/New_York', root)
for target in ('control', 'analyzed', 'prepared'):
    session.sql(f"CREATE TABLE ice.ns.{target} (v timestamptz_ns) USING iceberg TBLPROPERTIES ('format-version'='3')")
source = "SELECT ns AS v FROM ice.ns.src WHERE id = 1"
commands = (
    f'INSERT INTO ice.ns.control {source}',
    f'EXPLAIN ANALYZE INSERT INTO ice.ns.analyzed {source}',
    f'PREPARE review_write AS INSERT INTO ice.ns.prepared {source}',
    'EXECUTE review_write',
)
for command in commands:
    try:
        session.sql(command).to_arrow()
        print(json.dumps({'command': command.split()[0:3], 'result': 'accepted'}))
    except Exception as error:
        print(json.dumps({'command': command.split()[0:3], 'error': str(error)[:400]}))
for target in ('control', 'analyzed', 'prepared'):
    files = session.sql(f'SELECT content, file_path FROM ice.ns.{target}.files').to_arrow().to_pylist()
    ticks = []
    for file in files:
        if file['content'] == 0:
            ticks.extend(pq.read_table(file['file_path'], columns=['v']).column('v').cast(pa.int64()).to_pylist())
    print(json.dumps({'table': target, 'ticks': ticks}))
session.stop()
```

Recorded stdout:

```text
{"command": ["INSERT", "INTO", "ice.ns.control"], "result": "accepted"}
{"command": ["EXPLAIN", "ANALYZE", "INSERT"], "result": "accepted"}
{"command": ["PREPARE", "review_write", "AS"], "result": "accepted"}
{"command": ["EXECUTE", "review_write"], "result": "accepted"}
{"table": "control", "ticks": [1767341045123456789]}
{"table": "analyzed", "ticks": [1767323045123456789]}
{"table": "prepared", "ticks": [1767323045123456789]}
```

## PA-06 and PA-08: existing correctness gaps

Run with the fresh module. The NTZ output is microseconds. Bare-NULL output is microseconds with a zone; typed-NULL output is nanoseconds without a zone. The precision and zone change are both visible.

```python
from pathlib import Path
import json
import pyarrow as pa
from repark import ReparkSession
root = Path('/tmp/repark-production-audit-2026-10-11/known-correctness-fixture')
root.mkdir(exist_ok=True)
session = ReparkSession.builder.config('spark.sql.session.timeZone', 'America/New_York').config('repark.sql.allowCreateFormatVersion3', 'true').getOrCreate()
session.register_memory_catalog('ice', str(root))
session.sql('CREATE NAMESPACE ice.ns')
session.sql("CREATE TABLE ice.ns.nested (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>) USING iceberg TBLPROPERTIES ('format-version'='3')")
literal = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'"
session.sql(f"INSERT INTO ice.ns.nested VALUES (1, named_struct('v', {literal}, 'n', 1)), (2, named_struct('v', NULL, 'n', 1))")
session.sql(f"UPDATE ice.ns.nested SET st.v = {literal} WHERE id = 2")
result = session.sql('SELECT id, st.v AS v FROM ice.ns.nested ORDER BY id').to_arrow()
print('nested_ntz', json.dumps({'ids': result.column('id').to_pylist(), 'ticks': result.column('v').cast(pa.int64()).to_pylist()}))
for expression in ("coalesce(CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns), NULL)", "coalesce(CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns), CAST(NULL AS timestamp_ns))"):
    result = session.sql(f'SELECT {expression} AS v').to_arrow()
    print('null_coercion', json.dumps({'expression': expression, 'type': str(result.column('v').type), 'ticks': result.column('v').cast(pa.int64()).to_pylist()}))
session.stop()
```

Recorded stdout:

```text
nested_ntz {"ids": [1, 2], "ticks": [1767323045123456, 1767305045123456]}
null_coercion {"expression": "coalesce(CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns), NULL)", "type": "timestamp[us, tz=UTC]", "ticks": [1767341045123456]}
null_coercion {"expression": "coalesce(CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns), CAST(NULL AS timestamp_ns))", "type": "timestamp[ns]", "ticks": [1767323045123456789]}
```

## PA-12: fresh oracle classification

Run with the fresh module using argument repark. Run the same script with argument spark using the existing pinned Spark environment, Java 17 and SPARK_LOCAL_IP=127.0.0.1, removing ambient SPARK_HOME and PYSPARK_PYTHON. The banner below is from a live session, not package metadata.

```python
import json
import sys

cases = ('array(1, 2, 3)', 'array(1, NULL, 3)', 'CAST(array() AS ARRAY<INT>)', 'CAST(NULL AS ARRAY<INT>)')
if sys.argv[1] == 'spark':
    from pyspark.sql import SparkSession
    session = SparkSession.builder.master('local[2]').appName('production-audit-shapes').config('spark.sql.session.timeZone', 'UTC').getOrCreate()
    print('oracle_banner', session.version, session.conf.get('spark.sql.session.timeZone'))
    for ansi in ('true', 'false'):
        session.conf.set('spark.sql.ansi.enabled', ansi)
        for value in cases:
            frame = session.sql(f'SELECT transform({value}, (x, i) -> x + i) AS r')
            print(json.dumps({'ansi': ansi, 'case': value, 'schema': frame.schema.jsonValue(), 'values': [r.r for r in frame.collect()]}))
    session.stop()
else:
    import repark
    from repark import ReparkSession, functions as F
    print('repark', repark.__version__)
    for ansi in ('true', 'false'):
        session = ReparkSession.builder.config('spark.sql.ansi.enabled', ansi).getOrCreate()
        for value in cases:
            sql = session.sql(f'SELECT transform({value}, (x, i) -> x + i) AS r').to_arrow()
            column = session.sql(f'SELECT {value} AS a').select(F.transform('a', lambda x, i: x + i).alias('r')).to_arrow()
            print(json.dumps({'ansi': ansi, 'case': value, 'sql_schema': str(sql.schema), 'column_schema': str(column.schema), 'sql_values': sql.to_pylist(), 'column_values': column.to_pylist()}))
        session.stop()
```

Recorded stdout:

```text
repark 1.5.4
{"ansi": "true", "case": "array(1, 2, 3)", "sql_schema": "r: list<item: int32 not null> not null\n  child 0, item: int32 not null", "column_schema": "r: list<item: int32 not null> not null\n  child 0, item: int32 not null", "sql_values": [{"r": [1, 3, 5]}], "column_values": [{"r": [1, 3, 5]}]}
{"ansi": "true", "case": "array(1, NULL, 3)", "sql_schema": "r: list<item: int32> not null\n  child 0, item: int32", "column_schema": "r: list<item: int32> not null\n  child 0, item: int32", "sql_values": [{"r": [1, null, 5]}], "column_values": [{"r": [1, null, 5]}]}
{"ansi": "true", "case": "CAST(array() AS ARRAY<INT>)", "sql_schema": "r: list<item: int32> not null\n  child 0, item: int32", "column_schema": "r: list<item: int32> not null\n  child 0, item: int32", "sql_values": [{"r": []}], "column_values": [{"r": []}]}
{"ansi": "true", "case": "CAST(NULL AS ARRAY<INT>)", "sql_schema": "r: list<item: int32>\n  child 0, item: int32", "column_schema": "r: list<item: int32>\n  child 0, item: int32", "sql_values": [{"r": null}], "column_values": [{"r": null}]}
{"ansi": "false", "case": "array(1, 2, 3)", "sql_schema": "r: list<item: int32 not null> not null\n  child 0, item: int32 not null", "column_schema": "r: list<item: int32 not null> not null\n  child 0, item: int32 not null", "sql_values": [{"r": [1, 3, 5]}], "column_values": [{"r": [1, 3, 5]}]}
{"ansi": "false", "case": "array(1, NULL, 3)", "sql_schema": "r: list<item: int32> not null\n  child 0, item: int32", "column_schema": "r: list<item: int32> not null\n  child 0, item: int32", "sql_values": [{"r": [1, null, 5]}], "column_values": [{"r": [1, null, 5]}]}
{"ansi": "false", "case": "CAST(array() AS ARRAY<INT>)", "sql_schema": "r: list<item: int32> not null\n  child 0, item: int32", "column_schema": "r: list<item: int32> not null\n  child 0, item: int32", "sql_values": [{"r": []}], "column_values": [{"r": []}]}
{"ansi": "false", "case": "CAST(NULL AS ARRAY<INT>)", "sql_schema": "r: list<item: int32>\n  child 0, item: int32", "column_schema": "r: list<item: int32>\n  child 0, item: int32", "sql_values": [{"r": null}], "column_values": [{"r": null}]}
```

Live Spark stdout for the same shape script:

```text
oracle_banner 4.1.2 UTC
{"ansi": "true", "case": "array(1, 2, 3)", "schema": {"type": "struct", "fields": [{"name": "r", "type": {"type": "array", "elementType": "integer", "containsNull": false}, "nullable": false, "metadata": {}}]}, "values": [[1, 3, 5]]}
{"ansi": "true", "case": "array(1, NULL, 3)", "schema": {"type": "struct", "fields": [{"name": "r", "type": {"type": "array", "elementType": "integer", "containsNull": true}, "nullable": false, "metadata": {}}]}, "values": [[1, null, 5]]}
{"ansi": "true", "case": "CAST(array() AS ARRAY<INT>)", "schema": {"type": "struct", "fields": [{"name": "r", "type": {"type": "array", "elementType": "integer", "containsNull": true}, "nullable": false, "metadata": {}}]}, "values": [[]]}
{"ansi": "true", "case": "CAST(NULL AS ARRAY<INT>)", "schema": {"type": "struct", "fields": [{"name": "r", "type": {"type": "array", "elementType": "integer", "containsNull": true}, "nullable": true, "metadata": {}}]}, "values": [null]}
{"ansi": "false", "case": "array(1, 2, 3)", "schema": {"type": "struct", "fields": [{"name": "r", "type": {"type": "array", "elementType": "integer", "containsNull": false}, "nullable": false, "metadata": {}}]}, "values": [[1, 3, 5]]}
{"ansi": "false", "case": "array(1, NULL, 3)", "schema": {"type": "struct", "fields": [{"name": "r", "type": {"type": "array", "elementType": "integer", "containsNull": true}, "nullable": false, "metadata": {}}]}, "values": [[1, null, 5]]}
{"ansi": "false", "case": "CAST(array() AS ARRAY<INT>)", "schema": {"type": "struct", "fields": [{"name": "r", "type": {"type": "array", "elementType": "integer", "containsNull": true}, "nullable": false, "metadata": {}}]}, "values": [[]]}
{"ansi": "false", "case": "CAST(NULL AS ARRAY<INT>)", "schema": {"type": "struct", "fields": [{"name": "r", "type": {"type": "array", "elementType": "integer", "containsNull": true}, "nullable": true, "metadata": {}}]}, "values": [null]}
```

## Dependency and remote-run evidence

`make audit` completed with exit 0. Cargo advisory/bans/license/source checks reported `advisories ok, bans ok, licenses ok, sources ok`. Python dependency auditing reported `No known vulnerabilities found`. This is scanner evidence dated 2026-10-11; it does not establish application security.

The inspected remote [parity-live run](https://github.com/TRO-Wolf/repark/actions/runs/38121782754) at `2c33f289c177edc439de6c58a6beef45c53e7ba1` ended:

```text
10 failed, 16934 passed, 23 skipped, 153 xfailed, 232 warnings in 6871.36s (1:54:31)
```

Failures named six missing `/tmp/sparkenv/bin/python` fixture subprocesses, one Spark race-fixture mismatch, two indexed-transform width expectations, and one compatibility-smoke subprocess. That subprocess retained the minute/second `UNSUPPORTED_TIME_TYPE` details and reported three failures in total; the third detail is not recoverable from its retained tail. The assessment records the fresh indexed-transform classification; the other cells are not claimed newly re-measured.

The inspected [AWS acceptance run](https://github.com/TRO-Wolf/repark/actions/runs/38126286100) at the same revision was cancelled, with its dbt gold step skipped. The [fast CI run](https://github.com/TRO-Wolf/repark/actions/runs/38159266673) at the pinned current main succeeded. A docs-only current-main success is not substituted for completed live acceptance. Remote workflow access was read-only.

## Interpretation limits

These probes establish the specific triggers in the assessment. They are not exhaustive route matrices or production soak tests. No runtime regression tests were added because this unit records findings rather than fixes. Follow-up remediation must turn the named controls into tracked, per-door pins and demonstrate their failing baseline before repair.

## Sol 6.1 High performance review provenance

The requested independent reviewer was dispatched after the primary assessment was written, with model `gpt-6.1-sol` and reasoning effort `high`. It reviewed a shared-object scratch clone at the same pinned revision, on the same-named docs branch. Its [performance report](../perf/production-performance-audit-2026-10-11.md) and map were transferred onto the attached audit branch without runtime changes or commits.

The report contains twelve ranked improvement candidates, nine isolated development samples with value/type checks, four EXPLAIN controls, historical-fix exclusions and per-candidate acceptance experiments. The orchestrator spot-checked the ordered-sort `TaskContext::default()` path, complete input drain, retained MoR update state and full-series window materialization against current source. No before/after speedup is claimed.

The performance probe was run in separate child processes with the environment recorded in that report. Its exact task-owned script is preserved here so reproduction does not depend on temporary files:

```python
import json, os, platform, resource, sys, time
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import repark, repark._native as native
from repark.spark.sql import SparkSession

def status():
    return {key: int(value.split()[0]) for key, value in (line.split(':',1) for line in Path('/proc/self/status').read_text().splitlines() if ':' in line) if key in ('VmRSS','VmHWM','VmSize','Threads')}

mode=sys.argv[1]
size=int(sys.argv[2])
initial=status()
session=SparkSession.builder.config('datafusion.execution.target_partitions','4').config('datafusion.execution.batch_size','8192').config('datafusion.runtime.memory_limit','64M').getOrCreate()
frame=session.range(size).selectExpr('id','id * 3 AS triple','CAST(id AS DOUBLE) / 2 AS half')
frame.limit(1).to_arrow()
start_status=status()
start=time.perf_counter()
if mode=='arrow':
    result=frame.to_arrow()
    wall=time.perf_counter()-start
    count=result.num_rows
    sums=[pc.sum(result.column(i)).as_py() for i in range(3)]
    types=[str(field.type) for field in result.schema]
    retained=result.nbytes
elif mode=='collect':
    result=frame.collect()
    wall=time.perf_counter()-start
    count=len(result)
    sums=[sum(row[i] for row in result) for i in range(3)]
    types=[type(result[0][i]).__name__ for i in range(3)]
    retained=None
elif mode=='stream':
    count=0
    sums=[0,0,0.0]
    types=None
    retained=0
    for result in frame.to_arrow_batches():
        count+=result.num_rows
        types=[str(field.type) for field in result.schema]
        retained=max(retained,result.nbytes)
        for i in range(3):
            sums[i]+=pc.sum(result.column(i)).as_py()
    wall=time.perf_counter()-start
else:
    raise ValueError(mode)
expected=size*(size-1)//2
assert count==size and sums==[expected,3*expected,expected/2],(count,sums)
assert types==(['int','int','float'] if mode=='collect' else ['int64','int64','double']),types
print(json.dumps({'mode':mode,'rows':size,'columns':3,'wall_ms':wall*1000,'initial':initial,'before':start_status,'after':status(),'ru_maxrss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'retained_arrow_bytes':retained,'count':count,'sums':sums,'types':types,'version':repark.__version__,'debug':native.__debug_assertions__,'native':native.__file__,'python':platform.python_version(),'platform':platform.platform(),'load':os.getloadavg(),'affinity_cpus':len(os.sched_getaffinity(0)),'tokio_workers':os.environ.get('TOKIO_WORKER_THREADS'),'pool':'64M','target_partitions':4,'batch_rows':8192}))
session.stop()
```

## PA-15: microbatch completion-bound failure

The full Rust execution reported this test FAILED. It was repeated directly against the exact compiled binary, without a rebuild or source changes:

```bash
<shared-target>/debug/deps/repark_core-169d9f016a073d04 \
  --exact microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope \
  --nocapture
```

Isolated stdout:

```text

running 1 test
test microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope ... FAILED

failures:

failures:
    microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1420 filtered out; finished in 14.28s
```

Isolated stderr:

```text

thread 'microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope' (468839) panicked at crates/repark-core/src/microbatch/lifecycle_tests.rs:131:10:
the query ends within the bound: Elapsed(())
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
```

This isolated process overlapped with workspace verification and another build on the host. The runtime bound failure is established; its cause is not. No observed stored-data discrepancy is attributed to this test failure.

### Assembled verification result

The static `make ci` portion completed, including format, clippy, panic/async policy, structure/convention checks, locked dependencies, spelling, docs/maps/ledgers and matrix liveness. Matrix liveness found 94 referenced tests among 7,484 listed names. The source-only example gate skipped native execution because its interpreter did not import the fresh module; the separately executed targeted suite used the fresh worktree environment. No packaged-wheel example or full-facade result is inferred from that skip.

The Rust portion completed these groups before stopping at core:

```text
repark-common: 96 passed
repark-connect lib: 3 passed
repark-connect integration: 159 passed, 60 ignored
repark-core lib: 1419 passed, 1 failed, 1 ignored; 219.86 seconds
```

The core failure detail was:

```text
---- microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope stdout ----

thread 'microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope' (450706) panicked at crates/repark-core/src/microbatch/lifecycle_tests.rs:131:10:
the query ends within the bound: Elapsed(())


failures:
    microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope

test result: FAILED. 1419 passed; 1 failed; 1 ignored; 0 measured; 0 filtered out; finished in 219.86s
```

To cover the crates not reached by the stopping workspace command, a separate no-fail-fast run covered functions, Spark, Iceberg, ML, Python, SQL, TA and distributed. That does not erase the full default gate failure. It completed with exit 0: 5,710 passed, zero failed and six ignored across 66 test/doctest result groups. The optional cluster feature was off; its feature-gated suites are not qualified by this run.


### Final isolated retry

After the broad run completed, the exact same core binary and test command passed without source changes:

```text
running 1 test
test microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1420 filtered out; finished in 2.19s
```

This is a changed execution condition, not a replacement for the two failures above. Scheduling sensitivity is plausible; no causal diagnosis was established. The default full `make verify` remains red in this audit.

## Final artifacts and resource record

Disk checks found 733 GiB free before worktree/build, 703 GiB before native tests, about 656 GiB before broad Rust validation and 623 GiB at handoff. Other host build activity overlapped this work; the change in free space is not attributed solely to this audit.

Task-owned temporary local-policy, write-carrier and correctness fixtures, a compiled UTF-8 helper and generated audit requirements were removed after their evidence was preserved. The attached docs worktree and its fresh environment remain for review and reproduction. The shared Cargo target and dependency caches remain reusable. The performance scratch clone remains because it contains uncommitted review documents. Scripts, logs and diagnostic samples remain under `/tmp/repark-production-audit-2026-10-11`; reproduction scripts and selected outputs are also embedded in this appendix. No other task's artifacts or uncommitted files were deleted.

At the 2026-10-11 handoff, all changes were Markdown documentation on `docs/production-bugs-audit-2026-10-11`, staged but uncommitted. No runtime fixes, commit, push, pull request, cloud mutation or remote security test was performed.


## PR publication — 2026-10-12

The owner authorized committing, pushing and opening a PR for this documentation branch. The original findings and measurements remain pinned to `07be1dc930711bb927204f3df99d08526c9aaef6`; this publication step does not extend the audit to later main commits. At pickup, 791 GiB was available and the previous temporary audit directory was absent. The reproduction scripts and selected outputs embedded above remain available. New publication logs use `/tmp/repark-production-audit-pr-2026-10-12`.


Publication validation: the static gate and full default Rust workspace run passed on 2026-10-12: 7,433 passed, zero failed and 68 ignored across 76 test/doctest groups. The microbatch test passed within that full run. The installed pre-commit hook was explicitly executed and passed. Preflight then rebuilt the current editable native module and started all 17,088 collected Python facade tests. Its long-plan subprocess fixture allows up to 1,500 seconds. The initial PR is a draft while this gate continues; the PR validation record will carry its final result. No full preflight success is claimed at initial publication.
