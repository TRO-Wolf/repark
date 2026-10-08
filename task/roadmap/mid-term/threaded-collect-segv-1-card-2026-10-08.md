# Card THREADED-COLLECT-SEGV-1: collect() on a second thread segfaults with pyarrow 25.0.0

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's measurements. **Source:** the orchestrator's measurements of 2026-10-08, and the C-3 verifier's out-of-scope observation in PR #998.

**Status:** filed, not scheduled.

It is not a regression in v1.5.3: 1.5.1 and 1.5.2 behave the same. A user whose environment pins pyarrow 25.0.0 can crash the process by calling `collect()` from a worker thread.

## Why

**The probe.** Reported first by the C-3 verifier (PR #998) as an out-of-scope observation. The orchestrator then measured it on this box (Linux x86_64, CPython 3.12.3), each run in a fresh virtual environment, under a 64 GiB address-space limit.

```python
import threading, repark
spark = repark.ReparkSession.builder.getOrCreate()
for i in range(6):
    fr = spark.range(0, 300000).selectExpr("id", "id % 1000 as p", "id as v", "repeat('x', 100) as pad")
    def go(): print("rows", i, len(fr.collect()), flush=True)
    t = threading.Thread(target=go, daemon=True); t.start(); t.join()
print("survived")
```

Each iteration collects on a new non-main thread. No Postgres and no Iceberg table is involved.

**Results.**

| repark | pyarrow | result |
|---|---|---|
| 1.5.3 (the manylinux x86_64 wheel built by release run 37705196261, not yet on PyPI) | 25.0.1 | survived, three runs |
| 1.5.2 (PyPI) | 25.0.1 | survived |
| 1.5.1 (PyPI) | 25.0.1 | survived |
| 1.5.3 (the same release wheel) | 25.0.0 | the first thread returns 300000 rows, the second thread dies with SIGSEGV |
| 1.5.2 (PyPI) | 25.0.0 | SIGSEGV on the second thread |
| 1.5.1 (PyPI) | 25.0.0 | SIGSEGV on the second thread |
| four local lane builds (release and debug, including one of main before the ATTR-ID-1 stack), each in a venv holding pyarrow 25.0.0 | 25.0.0 | SIGSEGV on the second thread |
| one of those local builds, loaded into a venv holding pyarrow 25.0.1 | 25.0.1 | survived |

- With pyarrow 25.0.0, four collects of the same frame on the **main** thread succeed.
- The Python traceback at the fault (faulthandler): `repark/spark/dataframe/rows_export.py`, line 235, in `rows_from_arrow_table`, called from `core.py` `_rows_from_arrow_table`, called from `collect`. The verifier names the native function `_native.rows_from_record_batch`.
- The wheel's metadata requires `pyarrow>=25.0.0`. A fresh install today resolves pyarrow 25.0.1.

## What is not known

- Not measured: whether the defect is in pyarrow 25.0.0 itself (fixed in 25.0.1) or in RePark's native row export, with 25.0.1 only masking it.
- Not measured: any pyarrow version below 25.0.0 (the requirement excludes them) or above 25.0.1.
- Not measured: `toArrow()`, `toPandas()`, `show()` or `toLocalIterator()` on a second thread; a thread pool; macOS, Windows or aarch64.
- Not measured: repark releases older than 1.5.1.

## The ask

1. Find the cause: whether pyarrow 25.0.0 itself faults or RePark's native row export does. Read what changed between pyarrow 25.0.0 and 25.0.1, and reproduce it without RePark if a minimal reproduction exists.
2. If the fault is RePark's, fix it and pin it with a threaded-collect test.
3. If the fault is pyarrow 25.0.0's, the owner decides whether to raise the requirement to `pyarrow>=25.0.1`.
4. Either way, measure the doors listed under "What is not known", and add a threaded test to the facade suite.

## Gates

- the probe survives on every supported pyarrow version, or the requirement excludes the versions where it cannot;
- a facade test that collects on a non-main thread;
- an Opus verifier on any product PR.

## Owner decision needed

- The dependency floor: item 3 of the ask, whether the requirement rises to `pyarrow>=25.0.1`.
- Whether a patch release carries it.

## Pointers

- The C-3 verifier's out-of-scope observation, PR #998.
- The orchestrator's measurements of 2026-10-08: the probe, the results table and the traceback above.
