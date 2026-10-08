# Unit ledger — THREADED-COLLECT-SEGV-1 · `collect()` on a second thread segfaults with pyarrow 25.0.0: the cause is pyarrow's bundled mimalloc

## Round 1 (2026-10-08)

**Date:** 2026-10-08 · **Branch:** `fix/threaded-collect-segv-1` · **Base:** `27b18a86`
**Model:** claude-opus-5-5 (delegated build lane) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Card:** [threaded-collect-segv-1-card-2026-10-08.md](../../roadmap/mid-term/threaded-collect-segv-1-card-2026-10-08.md).

No product code changes in this round. The fault is pyarrow 25.0.0's; the dependency floor is
the owner's decision (see "Recommendation").

## PROPOSITION LEDGER — THREADED-COLLECT-SEGV-1 — 2026-10-08

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | On a pyarrow other than 25.0.0, `collect`, `take`, `head`, `first`, `toLocalIterator` and `toArrow` each answer with the right row count on six successive fresh non-main threads, in an isolated interpreter whose exit status is the assertion. | `python/repark/tests/test_threaded_collect_segv_1.py`: 6 passed with repark 1.5.3 (release wheel) + pyarrow 25.0.1; 6 skipped with the reason on pyarrow 25.0.0. | PROVEN | Run against the 1.5.3 release wheel, not a build of this branch (the branch changes no product code and the clone has no build). |
| C-002 | The test exercises the crash's precondition: pyarrow is not loaded on the main thread before the first threaded door runs. | The worker reports `pyarrow_on_main_before_first_thread` and the test asserts it is false. With the skip bypassed on pyarrow 25.0.0 the same worker dies with signal 11 on five of the six doors (`toArrow` survived its six iterations). | PROVEN | This is the mutation stand-in: there is no RePark fix to revert, so the red is the worker on 25.0.0. |

## Cause

**pyarrow 25.0.0's bundled mimalloc. Not RePark.** Upstream: apache/arrow GH-50471, "SIGSEGV in
bundled mimalloc `mi_thread_init` when libarrow is first loaded on a non-main thread that exits
(mimalloc 3.3.x, pyarrow 25.0.0)", fixed in 25.0.1 (10 August 2026) by a bump of the bundled
mimalloc.

**The native stack** (gdb, release wheel 1.5.3, pyarrow 25.0.0, Linux x86_64, CPython 3.12.3):

```
#0  mi_thread_init                                   libarrow.so.2500
#1  _mi_malloc_generic                               libarrow.so.2500
#2  mi_theap_malloc_zero_aligned_at_overalloc        libarrow.so.2500
#3  arrow::BaseMemoryPoolImpl<MimallocAllocator>::Allocate   libarrow.so.2500
#4  PoolAllocationMixin<ExportedSchemaPrivateData>::operator new
#5  arrow::SchemaExporter::Finish
#6  arrow::ExportSchema
#7  arrow::ExportRecordBatch
#8  pyarrow.lib RecordBatch.__arrow_c_array__        lib.cpython-312-x86_64-linux-gnu.so
#11 PyObject_CallMethodObjArgs                       CPython
#12 _native::collect_rows::__pyfunction_rows_from_record_batch   repark/_native.abi3.so
```

- **Faulting frame:** `mi_thread_init+121` in `libarrow.so.2500`, the instruction
  `mov 0x18(%rax),%rax` with `rax = 0`. Fault address `0x18`: a null dereference.
- **Owner of the frame:** `libarrow` (its statically bundled mimalloc). RePark's `_native` is
  eleven frames up and is only the caller of `batch.__arrow_c_array__()`.
- **The state at the fault:** the thread's default heap is mimalloc's static `theap_main`, whose
  `tld` field is null; `tld_main`'s thread id equals the faulting thread's `fs_base`.
- **The mechanism this shows:** mimalloc records the first thread that uses it as its "main"
  thread, by thread id (the thread-control-block address). When libarrow is first loaded on a
  worker thread, that worker becomes mimalloc's main thread. When it exits, mimalloc tears down
  `theap_main`. glibc then gives a later thread the same control-block address; `mi_thread_init`
  matches it to `tld_main`, hands it the torn-down `theap_main`, and reads through its null
  `tld`.
- **Why RePark hits it every time:** the facade imports pyarrow lazily, so in a process that has
  not imported pyarrow itself, the first threaded `collect()` is the first load of libarrow, on a
  worker thread. The first allocation from Arrow's default (mimalloc) pool on a later fresh
  thread is the export of the batch schema inside `rows_from_record_batch`.

**The reproduction without RePark** (no repark installed, pyarrow 25.0.0 alone):

```python
import threading

def go(i):
    import pyarrow as pa
    pa.record_batch({"id": pa.array([1, 2, 3], pa.int64())}).__arrow_c_array__()
    print("ok", i, flush=True)

for i in range(40):
    t = threading.Thread(target=go, args=(i,)); t.start(); t.join()
print("survived")
```

| environment | result over 20 runs |
|---|---|
| pyarrow 25.0.0, no repark | 9 of 20 SIGSEGV, same frame (`mi_thread_init`), same state (`theap_main`, `tld_main` thread id = `fs_base`) |
| pyarrow 25.0.0, repark installed but not imported | 9 of 20 SIGSEGV |
| pyarrow 25.0.1 | 0 of 20 |

Plain pyarrow is intermittent because the crash needs a later thread to be given the dead
thread's control-block address; RePark's probe gets that reuse on nearly every run. With pyarrow
imported on the main thread first, plain pyarrow survived every run.

**25.0.0 against 25.0.1.** The Python sources on this path are unchanged (the diff is
`to_pylist` fast paths in `array.pxi`, a Feather deprecation, version files). `libarrow.so`
differs: `mi_version` returns 30301 in 25.0.0 and 30401 in 25.0.1, and the 25.0.1 library has ten
more mimalloc symbols. The 25.0.1 changelog names GH-50471 and GH-50428 (mimalloc configuration
on macOS). Which mimalloc commit carries the fix was not read.

**What rules RePark out as a contributor of undefined behaviour:**

| candidate | finding |
|---|---|
| GIL | `rows_from_record_batch` holds the GIL for its whole body; it has no detach. |
| stack size | 256 MiB thread stacks (`threading.stack_size`) still crash; the fault is a null read, not a guard page. |
| grown-stack machinery | not on the faulting stack: frames 12 to 22 are the pyo3 trampoline on the thread's own stack. |
| Arrow release callbacks | the import swaps empty structs into the capsules and drops the imported data on the calling thread, GIL held; the fault is before any import, inside pyarrow's export. |
| batch size, offsets | one row crashes; an `id`-only frame crashes at 3,000,000 rows and intermittently below. The string column is not the cause. |
| address-space limit | crashes with no `ulimit -v`, and at 32, 64, 128 and 256 GiB. |
| Arrow memory pool | `ARROW_DEFAULT_MEMORY_POOL=system` or `jemalloc` with pyarrow 25.0.0: survives. |

## Measurements

Linux x86_64, CPython 3.12.3, repark 1.5.3 (release wheel), `ulimit -v 67108864`. Cells are
`exit status / iterations that answered`.

**Threading shape** (`collect`, the card's four-column frame, four iterations; pyarrow 25.0.0):

| shape | result |
|---|---|
| main thread only | 0 / 4 |
| a fresh thread each time, `daemon` off | 139 / 2 |
| a fresh thread each time, `daemon` on | 139 / 2 |
| one reused thread (`ThreadPoolExecutor(1)`) | 0 / 4 |
| `ThreadPoolExecutor(4)`, eight tasks | 0 / 8 |
| main first, then three fresh threads | 0 / 4 |
| one fresh thread, then main three times | 0 / 4 |
| fresh, main, fresh, fresh | 139 / 2 |
| fresh threads with 256 MiB stacks | 139 / 3 |
| fresh threads, pyarrow 25.0.1 | 0 / 4 |

A pool that keeps its threads alive did not crash in these runs. It is not safe by construction:
it crashes once a pool thread that first loaded libarrow exits and a later thread takes its
control-block address.

**Row count** (fresh threads, four-column frame, three runs each, pyarrow 25.0.0): 1, 100, 8192,
8193, 20000, 50000, 100000, 167772, 167773, 200000, 300000 and 1000000 rows all exit 139 on every
run.

**Doors and sources** (six fresh threads; 2,000 rows of `id, id % 10, cast(id as string)`):

| source | door | pyarrow 25.0.0 | pyarrow 25.0.1 | 25.0.0, pyarrow imported on main first |
|---|---|---|---|---|
| range | collect | 139 / 2 | 0 / 6 | 0 / 6 |
| range | toArrow | 0 / 6 | 0 / 6 | 0 / 6 |
| range | toPandas | 139 / 2 | 0 / 6 | 0 / 6 |
| range | show | 0 / 6 | 0 / 6 | 0 / 6 |
| range | count | 0 / 6 | 0 / 6 | 0 / 6 |
| range | toLocalIterator | 139 / 1 | 0 / 6 | 0 / 6 |
| range | take | 139 / 2 | 0 / 6 | 0 / 6 |
| range | head | 139 / 2 | 0 / 6 | 0 / 6 |
| range | first | 139 / 2 | 0 / 6 | 0 / 6 |
| parquet | collect | 139 / 1 | 0 / 6 | 0 / 6 |
| parquet | toArrow | 0 / 6 | 0 / 6 | 0 / 6 |
| parquet | toPandas | 139 / 2 | 0 / 6 | 0 / 6 |
| parquet | show | 0 / 6 | 0 / 6 | 0 / 6 |
| parquet | count | 0 / 6 | 0 / 6 | 0 / 6 |
| parquet | toLocalIterator | 139 / 1 | 0 / 6 | 0 / 6 |
| parquet | take | 139 / 2 | 0 / 6 | 0 / 6 |
| parquet | head | 139 / 2 | 0 / 6 | 0 / 6 |
| parquet | first | 139 / 3 | 0 / 6 | 0 / 6 |
| iceberg | collect | 139 / 1 | 0 / 6 | 0 / 6 |
| iceberg | toArrow | 0 / 6 | 0 / 6 | 0 / 6 |
| iceberg | toPandas | 139 / 2 | 0 / 6 | 0 / 6 |
| iceberg | show | 0 / 6 | 0 / 6 | 0 / 6 |
| iceberg | count | 0 / 6 | 0 / 6 | 0 / 6 |
| iceberg | toLocalIterator | 139 / 2 | 0 / 6 | 0 / 6 |
| iceberg | take | 139 / 2 | 0 / 6 | 0 / 6 |
| iceberg | head | 139 / 2 | 0 / 6 | 0 / 6 |
| iceberg | first | 139 / 1 | 0 / 6 | 0 / 6 |

`toArrow`, `show` and `count` surviving six iterations on 25.0.0 is one run each, not a proof:
`toArrow` does load libarrow on the worker. Why it made no mimalloc-pool allocation on the later
threads was not investigated.

**Older releases** (the card's probe, three runs each):

| repark | requirement in its metadata | pyarrow 25.0.0 | pyarrow 25.0.1 |
|---|---|---|---|
| 1.5.0 (PyPI) | `pyarrow>=25.0.0` | 139 / 1, three runs | 0 / 6, three runs |
| 1.4.2 (PyPI) | `pyarrow>=25.0.0` | 139 / 1, three runs | 0 / 6, three runs |

**Not measured:** macOS, Windows, aarch64; any pyarrow above 25.0.1; Python other than 3.12.3.
The new test was not run against a build of this branch.

## Recommendation

Raise the floor. The one-line change, in `python/repark/pyproject.toml` line 20:

```
dependencies = ["pyarrow>=25.0.1", "pydantic>=2.10,<3"]
```

25.0.1 also fixes GH-50605, silent wrong values decoding a double Parquet column on aarch64 SVE
in pyarrow 25.0.0. That is a second reason for the floor; it was read from the changelog and not
measured here.

A RePark-side alternative exists and is not taken here: importing pyarrow when `repark` is
imported moves the first load of libarrow to the importing thread, which removes the crash when
that thread is the main thread (the fifth column of the door table) and does not when `repark`
itself is first imported on a worker. It changes import cost and the lazy-import design, so it is
the owner's call.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: threaded-collect-segv-1
  categories:
    - id: AT-1
      status: N/A
      justification: No Spark-visible behaviour changes; the unit adds a test and a ledger.
    - id: AT-2
      status: ATTACKED
      evidence: With the skip bypassed on pyarrow 25.0.0 the test's worker dies with signal 11 on
        five of six doors; on 25.0.1 all six pass.
      artifacts: [python/repark/tests/test_threaded_collect_segv_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Nine doors over range, parquet and iceberg sources on both pyarrow versions, and
        repark 1.5.0 and 1.4.2, are tabled in this ledger.
      artifacts: [task/ledgers/staging/threaded-collect-segv-1-ledger.md]
    - id: AT-4
      status: ATTACKED
      evidence: The GIL, thread-local state, the grown-stack machinery and the Arrow release
        callbacks on this path were read and ruled out; the table under Cause records each.
      artifacts: [task/ledgers/staging/threaded-collect-segv-1-ledger.md]
    - id: AT-5
      status: N/A
      justification: No credentials, network or unsafe code change.
    - id: AT-6
      status: N/A
      justification: No error path changes.
    - id: AT-7
      status: N/A
      justification: No performance-relevant change.
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries the test, this ledger, the card's Cause section and the map lines.
      artifacts: [python/repark/tests/map.md, task/ledgers/staging/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: The card carries a dated Cause section that points here.
      artifacts: [task/roadmap/mid-term/threaded-collect-segv-1-card-2026-10-08.md]
    - id: AT-10
      status: ATTACKED
      evidence: The facade suite gains a subprocess-isolated threaded test that CI runs on its
        resolved pyarrow.
      artifacts: [python/repark/tests/test_threaded_collect_segv_1.py]
  complete: true
```
