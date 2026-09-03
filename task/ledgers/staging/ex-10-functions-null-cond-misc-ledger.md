# Unit ledger — EX-10 · v0.7 example backfill, `F.*` null-handling and conditional family

**Retires:** this ledger moves to `../completed/` in the family's last commit
(the orchestrator's departure move). It closes when the `F.*` null-handling
family PR merges, or when the owner closes the slate row.

**Unit:** EX-10 · **Date:** 2026-09-03 · **Model:** muse-spark-1.2-contributor (continuation of glm-5.3-flash) ·
**Branch:** `feat/ex-10-functions-null-conditional` · **Base:** `84c1801` · **Wall-clock:** 2026-09-03 03:10–03:55 UTC · **Cost:** ~$0.40
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md),
batch roster row EX-10. **Ruling:** owner, 2026-08-31,
[release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md)
§"v0.7 — Full example documentation", and the 2026-08-31 ruling that each family
PR carries its own charter ledger with one clause per batch.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the
`BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep
`map.md` files, and this ledger with its `staging/map.md` row. Closed:
`crates/`, `python/repark/src/`, every other `scripts/` line, `.github/`,
`STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The family is the `F.*` null-handling, conditional, ordering, bit and session
names the campaign left on the backlog. This unit lands 33 of the 45 roster
names in seven files; the remaining 12 stay on the backlog with both values
recorded.

**Roster as dispatched (45 names, measured against `docs/examples/backlog.txt`
at `84c1801`, where all 45 are rows):**

`F.coalesce`, `F.ifnull`, `F.nvl`, `F.nvl2`, `F.nullif`, `F.nullifzero`,
`F.zeroifnull`, `F.isnan`, `F.isnull`, `F.isnotnull`, `F.nanvl`, `F.when`,
`F.expr`, `F.column`, `F.asc`, `F.asc_nulls_first`, `F.asc_nulls_last`,
`F.desc`, `F.desc_nulls_first`, `F.desc_nulls_last`, `F.equal_null`,
`F.assert_true`, `F.raise_error`, `F.broadcast`, `F.spark_partition_id`,
`F.monotonically_increasing_id`, `F.input_file_name`,
`F.input_file_block_length`, `F.input_file_block_start`, `F.current_catalog`,
`F.current_database`, `F.current_schema`, `F.current_user`, `F.user`,
`F.session_user`, `F.version`, `F.negate`, `F.bitwiseNOT`, `F.bitwise_not`,
`F.bit_count`, `F.bit_get`, `F.getbit`, `F.shiftleft`, `F.shiftright`,
`F.shiftrightunsigned`.

**Grouping.** Seven files, grouped by the idea a reader learns in one breath:

| File | `COVERS` (batch names) | Why these together |
|---|---|---|
| `nulls.py` | `F.isnull`, `F.isnotnull`, `F.equal_null`, `F.coalesce`, `F.ifnull`, `F.nvl`, `F.nvl2`, `F.nullif`, `F.nullifzero`, `F.zeroifnull`, `F.nanvl` | NULL tests and substitutions on rows carrying NULLs, NaN literal edges separate |
| `conditional.py` | `F.when`, `F.assert_true` | `F.when` chains and the bare form, then `F.assert_true` passing and raising with its message |
| `columns.py` | `F.column` | Constructor spelling that agrees with `F.col`, NULL included |
| `sort_order.py` | `F.asc`, `F.asc_nulls_first`, `F.asc_nulls_last`, `F.desc`, `F.desc_nulls_first`, `F.desc_nulls_last` | Six `F.asc*`/`F.desc*` orderings and where each places NULLs |
| `bitwise.py` | `F.negate`, `F.bitwiseNOT`, `F.bitwise_not`, `F.bit_count`, `F.bit_get`, `F.getbit`, `F.shiftleft`, `F.shiftright`, `F.shiftrightunsigned` | Integer bit family: negations, popcount, bit reads, shifts |
| `broadcast.py` | `F.broadcast` | Join hint, checked to agree with the plain join |
| `session_context.py` | `F.current_catalog`, `F.current_database`, `F.current_schema` | Session answers on a two-row local frame |

No existing example under `docs/examples/functions/` demonstrated any of the
45 — `abs.py` and the prior batches cover disjoint names. The seven new files
list `F.col` and `F.lit` in `COVERS` where genuinely used; both are already
covered, so neither moves the ratchet.

## Orchestrator rulings (build-to)

- The gate is the acceptance bar in both directions: a `COVERS` entry the script
  does not exercise is the defect the campaign will not tolerate, and every
  script runs green locally with no network, no cloud and no JVM beyond the
  throwaway oracle.
- Every asserted value is measured against the live Spark oracle
  (`/tmp/oc-ex10/.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
  `PYTHONPATH=/tmp/oc-ex10/python/repark/tests`,
  `_live_parity.build_spark_iceberg_engine`) before it is written; a name whose
  repark value differs from Spark, or that repark refuses, is dropped from its
  file's `COVERS` and stays on the backlog with both values recorded.
- The backlog count moves down by exactly the names this batch covers, and
  `BACKLOG_BASELINE` moves with it — measured at 842 → 809, 33 of the 45
  dispatched.
- No product edit, ever. A name whose example exposes an engine defect is
  reported and dropped back to the backlog; the baseline then moves by the names
  actually removed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-ex-10-batch
  agent: Actor
  action: land EX-10 null-handling and conditional batch (45 dispatched, 33 landed)
  charter_trace: C-001
  preconditions:
    - branch feat/ex-10-functions-null-conditional at 84c1801: SATISFIED (git)
    - all 45 names are backlog rows, none covered, none excepted: SATISFIED (grep)
    - the EX-0 gate is in make ci: SATISFIED (Makefile ci target)
  success_condition: every name the batch can teach honestly leaves the backlog, the ratchet moves by exactly that count, both gate legs exit 0
  step_risks:
    - a COVERS entry the script does not really exercise: HANDLED(each script asserts on the value the name produces)
    - Spark vs repark divergence hidden by adjusting assertion: HANDLED(oracle table below; dropped names stay on backlog)
  contingencies: [example exposes a product defect: EXECUTABLE(report it, drop the name, move the baseline by the names actually removed)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | This batch lands runnable local examples for the 33 roster names it can demonstrate honestly, in seven files under `docs/examples/functions/`, every `COVERS` entry exercised by an assertion on the value that name produces and measured against live PySpark 4.1.2 before it was written; those 33 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 33, 842 → 809, with no other `scripts/` change; the remaining 12 stay on the backlog with both values recorded in the oracle table below, and no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Oracle table below (one row per roster name: Spark value, repark value, kept/dropped, file), the green counts line, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Oracle table — per-name Spark vs repark (measured before writing)

Measured 2026-09-03 via the throwaway oracle at `/tmp/oc-ex10-oracle/` using
`/tmp/oc-ex10/.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`PYTHONPATH=/tmp/oc-ex10/python/repark/tests`,
`_live_parity.build_spark_iceberg_engine(Path(tmpdir)).session` — one script
that prints, per name, the Spark value and the repark value for the same inputs.
A name whose repark value differs from Spark, or that repark refuses, is dropped.

| Name | Spark value (oracle) | repark value | Kept / dropped | File |
|---|---|---|---|---|
| `F.coalesce` | `[1, 0, -1, 3]` / `[2.5, -1.0, 4.0, -1.0]` | same | kept | `nulls.py` |
| `F.ifnull` | `[2.5, -1.0, 4.0, -1.0]` | same | kept | `nulls.py` |
| `F.nvl` | `[1, 0, -1, 3]` | same | kept | `nulls.py` |
| `F.nvl2` | `[1, -99, null, -99]` | same | kept | `nulls.py` |
| `F.nullif` | `[1, null, null, 3]` / `[null, 0, null, 3]` | same | kept | `nulls.py` |
| `F.nullifzero` | `[1, null, null, 3]` | same | kept | `nulls.py` |
| `F.zeroifnull` | `[1, 0, 0, 3]` | same | kept | `nulls.py` |
| `F.isnan` | `[False, False]` on `[1.0, NULL]` (non-nullable `bool` `IsNaN`); `true` on NaN | `[False, None]` on `[1.0, NULL]` (nullable `bool`, DataFusion `isnan` null-propagates; `crates/repark-python/src/column/function_dispatch.rs:282`); `true` on NaN | dropped — BACKLOG `FN-ISNAN-1` | — |
| `F.isnull` | `[false, false, true, false]` | same | kept | `nulls.py` |
| `F.isnotnull` | `[true, true, false, true]` | same | kept | `nulls.py` |
| `F.nanvl` | `[2.5, null, 4.0, null]` / `-1.0` on NaN | same | kept | `nulls.py` |
| `F.when` | `["odd", "even", "missing"]` and `[null, "big", null]` | same | kept | `conditional.py` |
| `F.expr` | `F.expr('1 + 1')` → `[2, 2]`, `F.expr("upper('ab')")` → `['AB','AB']` Spark-equal; `F.expr('d * 2')` on `[1.0, NULL]` → `[2.0, None]` | `F.expr('1 + 1')` → `[2, 2]`, `F.expr("upper('ab')")` → `['AB','AB']` Spark-equal; `F.expr('d * 2')` → `AnalysisException: Schema error: No field named d` (residual `docs/spark-sql-iceberg-parity.md` ~L1087 TZ-5 §5 `F.expr over a column reference`) | dropped | — |
| `F.column` | `[1, 2, null]` | same | kept | `columns.py` |
| `F.asc` | `[null, null, 1, 2]` | same | kept | `sort_order.py` |
| `F.asc_nulls_first` | `[null, null, 1, 2]` | same | kept | `sort_order.py` |
| `F.asc_nulls_last` | `[1, 2, null, null]` | same | kept | `sort_order.py` |
| `F.desc` | `[2, 1, null, null]` | same | kept | `sort_order.py` |
| `F.desc_nulls_first` | `[null, null, 2, 1]` | same | kept | `sort_order.py` |
| `F.desc_nulls_last` | `[2, 1, null, null]` | same | kept | `sort_order.py` |
| `F.equal_null` | `[true, false, false, true]` and `[true, false, true, false]` | same | kept | `nulls.py` |
| `F.assert_true` | passes `[null, null]`, fails raises `x must exceed 2` | same | kept | `conditional.py` |
| `F.raise_error` | `raise_error(lit('boom'))` raises `SparkRuntimeException: [USER_RAISED_EXCEPTION] boom` | `UnsupportedOperationException: functions.raise_error evaluation is not supported yet (engine raise kernel deferred; disclosed E1)` | dropped | — |
| `F.broadcast` | `[(1,"a","x"), (3,"c","y")]` equals plain join | same (single-node no-op `python/repark/src/repark/spark/functions_session.py:49-56`) | kept | `broadcast.py` |
| `F.spark_partition_id` | `[0, 0]` on `local[1]` two-row frame (`[0, 1]` on `local[2]`) | `UnsupportedOperationException: functions.spark_partition_id is not supported yet (single-node disclosed; R-FN-BATCH4)` | dropped | — |
| `F.monotonically_increasing_id` | `[0, 1]` on `local[1]` two-row frame (`[0, 8589934592]` on `local[2]`) | `UnsupportedOperationException: functions.monotonically_increasing_id is not supported yet (single-node semantics disclosed; R-FN-BATCH4)` | dropped | — |
| `F.input_file_name` | `['', '']` on `local[1]` non-file frame | `UnsupportedOperationException: functions.input_file_name is not supported yet (disclosed R-FN-BATCH4)` | dropped | — |
| `F.input_file_block_length` | `[-1, -1]` on `local[1]` non-file frame | `UnsupportedOperationException: input_file_block_length is unreachable: it reads Spark's InputFileBlockHolder thread-local, populated by HadoopRDD/FileScanRDD as a split is handed to a task. DataFusion has no equivalent surface, and repark's input_file_name is itself still a stub. See docs/spark-sql-iceberg-parity.md (FNP-15 input_file_block_length).` | dropped | — |
| `F.input_file_block_start` | `[-1, -1]` on `local[1]` non-file frame | `UnsupportedOperationException: input_file_block_start is unreachable: it reads Spark's InputFileBlockHolder thread-local, populated by HadoopRDD/FileScanRDD as a split is handed to a task. DataFusion has no equivalent surface, and repark's input_file_name is itself still a stub. See docs/spark-sql-iceberg-parity.md (FNP-15 input_file_block_start).` | dropped | — |
| `F.current_catalog` | `["spark_catalog","spark_catalog"]` | same | kept | `session_context.py` |
| `F.current_database` | `["default","default"]` | same | kept | `session_context.py` |
| `F.current_schema` | `["default","default"]` | same | kept | `session_context.py` |
| `F.current_user` | `['john','john']` on `local[1]` (OS user `john`) | `['repark','repark']` deliberate `repark` identity (`python/repark/src/repark/spark/functions_session.py:65-70` ADR-0004) | dropped | — |
| `F.user` | `['john','john']` on `local[1]` | `['repark','repark']` deliberate same as `current_user` (`python/repark/src/repark/spark/functions_session.py:76` `user = current_user`) | dropped | — |
| `F.session_user` | `['john','john']` on `local[1]` | `['repark','repark']` deliberate same identity (`python/repark/src/repark/spark/functions_session.py:77` `session_user = current_user`) | dropped | — |
| `F.version` | `['4.1.2 f0bb2e6a47d0ebda424ffd633fcea8644a597954', '4.1.2 f0bb2e6a47d0ebda424ffd633fcea8644a597954']` | `['repark-0.6.0','repark-0.6.0']` deliberate `repark-<pep440>` (`python/repark/src/repark/spark/functions_session.py:110-115`) | dropped | — |
| `F.negate` | `[-5, 5, 0, null]` | same | kept | `bitwise.py` |
| `F.bitwiseNOT` | `[-6, 4, -1, null]` | same | kept | `bitwise.py` |
| `F.bitwise_not` | same as `F.bitwiseNOT` | same | kept | `bitwise.py` |
| `F.bit_count` | `[2, 8, 0, null]` | same | kept | `bitwise.py` |
| `F.bit_get` | `[1,0,null] / [0,1,null] / [1,0,null]` | same | kept | `bitwise.py` |
| `F.getbit` | same as `F.bit_get` | same | kept | `bitwise.py` |
| `F.shiftleft` | `[16, 8, -64, null]` | same | kept | `bitwise.py` |
| `F.shiftright` | `[1, 0, -4, null]` | same | kept | `bitwise.py` |
| `F.shiftrightunsigned` | `[1, 0, 9223372036854775804, null]` | same | kept | `bitwise.py` |

Counts line, both legs identical (the execute leg imports the native module, so
no skip line and every example is run):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 102 covered; 809 backlog; 2 exceptions; 22 examples`

Was `69 covered; 842 backlog; 15 examples` before this batch (after LOG1P-1);
now `102 covered; 809 backlog; 22 examples` — 33 covered is the landed names,
842 → 809 backlog is the same 33; 15 → 22 examples is the seven new files.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured on the base `84c1801` with the seven batch files removed and the
backlog rows already gone: `check_example_coverage.py` exits 1 with 33
findings, one per roster kept name and no others. With the files present the
gate is green. The 12 dropped names remain backlog rows and never appear as
findings.

## Gates (2026-09-03, on this branch)

| Command | Exit |
|---|---|
| `python3 scripts/check_example_coverage.py` | **0** |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `uv run --no-sync ruff check docs/examples` | **0** |
| `uv run --no-sync ruff format --check docs/examples` | **0** |
| `make check-docs-compaction` | **0** |
| `make check-example-coverage` (via `make ci` static half) | **0** |

Each example file as a script:

| Script | Exit |
|---|---|
| `docs/examples/functions/nulls.py` | **0** |
| `docs/examples/functions/conditional.py` | **0** |
| `docs/examples/functions/columns.py` | **0** |
| `docs/examples/functions/sort_order.py` | **0** |
| `docs/examples/functions/bitwise.py` | **0** |
| `docs/examples/functions/broadcast.py` | **0** |
| `docs/examples/functions/session_context.py` | **0** |

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: EX-10
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Enumerator emits 913 names across ten families; 33 landed names come from AST walk and inventory snapshot matches.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red; the backlog ratchet is exact.
      artifacts: [python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-3
      status: ATTACKED
      evidence: A missing COVERS or docstring or a nonzero example exit is fail-closed.
      artifacts: [scripts/check_example_coverage.py, python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-4
      status: N/A
      justification: Gate is a read-only process over source and scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface; example env scrub and exceptions ratchet unchanged.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; backfill is a walk of existing public names.
    - id: AT-7
      status: N/A
      justification: Static gate is AST-only; execution is optional on import of repark._native.
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the widened inventory; the walk adds no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr; no new log/metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Example modules carry pins ex-10-functions-null-cond-misc/C-001; registry FN-ISNAN-1 and test_fn_batch1.py pin C-001 of this unit.
      artifacts: [docs/examples/functions/nulls.py, docs/examples/functions/broadcast.py, docs/spark-sql-iceberg-parity.md, python/repark/tests/test_fn_batch1.py]
  complete: true
```
