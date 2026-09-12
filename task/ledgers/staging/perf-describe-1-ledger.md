# Unit ledger — PERF-DESCRIBE-1 · one aggregate pass for `describe` / `summary`

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when PERF-DESCRIBE-1 merges, or when the
owner closes the slate row.

**Unit:** PERF-DESCRIBE-1 · **Date:** 2026-09-11 · **Model:** swe-2-high · **Branch:** `perf/describe-1` · **Base:** `25ca119a` (branch point at dispatch; no merge performed — the orchestrator merges)
**Slate:** card PERF-DESCRIBE-1, `perf/describe-1` build lane, step 1 of 1.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `python/repark/src/repark/spark/dataframe/statistics.py`,
`python/repark/tests/test_perf_describe_1.py` (new pin file), lockstep `map.md` files
(`python/repark/tests/map.md`, `python/repark/src/repark/spark/dataframe/map.md`,
`task/ledgers/staging/map.md`), this ledger, and the measurement scratch under
`scratch/perf-describe-1/` (gitignored). Closed: `crates/`, `scripts/` baselines,
`.github/`, `STATUS.md`, `briefs/next-sequence.md`, every other ledger, every other pin.

## Scope

The S2-21 perf review (`/tmp/oc-worker/grok-rev-descr/report.md`) measured `describe()`
running one full scan per statistic — mean, stddev, min and max were four
`DataSourceExec` passes over the frame inside a five-leg `UNION ALL`, with the row order
carried by a `__repark_sum_ord` ordinal and `ORDER BY`. This unit replaces that shape with
one `AggregateExec` computing count / avg / stddev / min / max per column in a single
pass (D-1), then projects the requested summary rows as literals in Spark's order — the
ordinal wrapper and its sort node are gone because order is carried by construction. The
aggregate is built through the native column API (`frame._plan().aggregate([], exprs)`),
not SQL text: measured on the 50×10k wide frame, a 250-expression SQL aggregate pays
~0.33 s of parse/plan and ~1.4–2.7 ms per `CAST` physical expression (superlinear in the
aggregate context), which erased the one-scan win and regressed wide to ~1.7 s. The
native path plans the same plan in ~0.03 s. `CAST(agg AS VARCHAR)` is emitted only where
engine formatting is load-bearing — `avg`/`stddev` (Float64 ryu text) and `min`/`max` on
Float/Double/Decimal columns — while `count` (Int64) and `min`/`max` on integer and
string columns collect raw and format trivially (`str(int)`, the string itself), which
halves the cast count on the wide shape. `avg`/`stddev` on string columns keep the
`try_cast(col AS DOUBLE)` operand from DF-DESCRIBE-STR-1.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Before measurements on the reviewer's harness — 200k numeric `k,v`, 50 numeric columns × 10k, a 200k string-bearing frame — one warmup plus three timed repetitions, medians recorded, plus EXPLAIN showing the per-statistic scan count. | The C-001 measurement table below; `scratch/perf-describe-1/results.json` holds the raw reps. | **PROVEN** |
| C-002 | The single-pass shape: one `AggregateExec` computes every requested statistic per column, then the summary rows are projected as literals in the requested order; the DF-DESCRIBE-STR-1 ordinal wrapper is gone and the returned frame carries no UNION, no sort, no scan of the source — proven by EXPLAIN, not luck. | `test_describe_scans_the_source_once` plus the EXPLAIN counts in the C-002 note below. | **PROVEN** |
| C-003 | Every describe / summary pin stays green unchanged — string `mean`/`stddev` via `try_cast`, the non-describable skip/refusal, stat subsets, duplicate stats in requested order, display-name overlay on joined frames. | The `-k "describe or summary"` run and the full suite in the gates table; `test_summary_duplicate_stats_keep_requested_order`. | **PROVEN** |
| C-004 | After measurements on the same harness — the unit lands only if every measured shape is faster. | The C-004 measurement table below: all four shapes improved on medians. | **PROVEN** |

`LOGIC_SCORE` = **4/4 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

`test_perf_describe_1.py` at base `25ca119a` with `statistics.py` reverted to the
snapshot (`scratch/perf-describe-1/base_statistics.py`), exit 1:

```
>           assert len(agg_plans) == 1, (
E           AssertionError: expected one aggregate plan; session queries: ['SELECT 0 AS __repark_sum_ord, \'count\' AS summary, CAST(count("k") AS VARCHAR) AS "k", CAST(count("v") AS VARCHAR) AS "v" FROM "datafusion"."public"."__repark_sum_934c307329f447ddb464d589e50ae087" UNION ALL SELECT 1 AS __repark_sum_ord, \'mean\' AS summary, ... UNION ALL SELECT 4 AS __repark_sum_ord, \'max\' AS summary, CAST(max("k") AS VARCHAR) AS "k", CAST(max("v") AS VARCHAR) AS "v" FROM "datafusion"."public"."__repark_sum_934c307329f447ddb464d589e50ae087" ORDER BY __repark_sum_ord']
E           assert 0 == 1
E            +  where 0 = len([])

python/repark/tests/test_perf_describe_1.py:108: AssertionError
FAILED python/repark/tests/test_perf_describe_1.py::test_describe_scans_the_source_once
1 failed, 1 passed in 0.31s
```

The red is the shape itself: the base emits one five-leg `UNION ALL` SQL (five
`TableScan`s of the source view, four physical `DataSourceExec`s — `count` folds) plus an
`ORDER BY` on the stat ordinal; nothing materializes through a single aggregate plan.

## Harness and measurements (no JVM)

`scratch/perf-describe-1/bench.py` reuses the reviewer's method: the pre-change
`statistics.py` is snapshotted to `scratch/perf-describe-1/base_statistics.py` and loaded
by path so base and live run in one session on the same frames. Timed region is
`_summary("count","mean","stddev","min","max")` plus `.collect()` — the aggregate moved
inside `_summary`, so plan-only is not comparable and collect is the load-bearing number
(reviewer's own framing). One warmup, three timed reps per impl per shape, medians.
EXPLAIN legs are captured through a `SessionProxy` on `frame._session` (base emits its
union SQL; live emits only the literal query) plus a `DataFrame.to_arrow` spy that
explains every frame materialized during the call (live's native aggregate is not session
SQL, so it is observed at materialization).

C-001 — before (base) medians and plan counts:

| Shape | Median (s) | logical TableScan | physical DataSourceExec | UnionExec |
|---|---:|---:|---:|---:|
| numeric 200k `k,v` | 0.131859 | 5 | 4 | 1 |
| wide 50 cols × 10k | 1.164347 | 5 | 4 | 1 |
| strings 200k `s,numeric_ish` | 0.194066 | 5 | 4 | 1 |
| mixed 200k `k,v,g,flag` | 0.196635 | 5 | 4 | 1 |

C-002 — after (live) plan shape: exactly one materialized plan carries one `TableScan`,
one `DataSourceExec`, and an `AggregateExec` (mode=Single, all requested aggregates, the
string columns' `avg`/`stddev` over `try_cast`); the only session SQL is the literal
`SELECT CAST(...) FROM (VALUES ...) AS t(summary, …)` leg with zero `TableScan`s; the
returned frame's EXPLAIN shows `Values:` and no `TableScan`, `UnionExec`, `SortExec`, or
`SortPreservingMergeExec`. The ordinal column and its `ORDER BY` are gone — row order is
the literal order by construction. Per-shape counts are identical on all four frames.

C-004 — after (live) medians; every shape faster:

| Shape | base median (s) | live median (s) | Δ |
|---|---:|---:|---:|
| numeric 200k `k,v` | 0.131859 | 0.097671 | −25.9 % |
| wide 50 cols × 10k | 1.164347 | 0.993226 | −14.7 % |
| strings 200k `s,numeric_ish` | 0.194066 | 0.190805 | −1.7 % |
| mixed 200k `k,v,g,flag` | 0.196635 | 0.138857 | −29.4 % |

The strings margin is the thinnest; a five-rep stability probe on the same frame answered
base median 0.2207 s vs live 0.1896 s (live max 0.1964 < base median), so the improvement
is real, not rep noise.

Why the first single-pass attempt still lost on wide is recorded for the eventual reader:
250 `CAST(agg AS VARCHAR)` expressions in one query cost ~0.7 s of physical-expression
overhead (measured: +71 ms per 50 casts over the aggregate; a 250-cast literal SELECT pays
~0.12 s; a 250-cast projection over the big aggregate pays ~0.67 s — superlinear). The
shipped shape casts only `avg`/`stddev` and non-int non-string `min`/`max` — 100 casts on
the wide frame — and passes `count`/integer `min`/`max`/string `min`/`max` through raw,
which is byte-identical because Arrow integer display is `str(int)` and a Utf8→Utf8 cast
is identity. Python-side float formatting was measured and rejected: engine
`CAST(Float64 AS VARCHAR)` uses ryu-style presentation (`'1e16'`, `'0.00001'`,
`'1.23456789e-6'`) that Python `repr` does not reproduce (`'1e+16'`, `'1e-05'`,
`'1.23456789e-06'`).

## Gates (2026-09-11, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_perf_describe_1.py -q` | 0 — 2 passed |
| `.venv/bin/python -m pytest python/repark/tests -q -k "describe or summary"` | 0 — 35 passed, 6 skipped, 6266 deselected |
| `.venv/bin/python -m pytest python/repark/tests -q` | 0 — 5938 passed, 369 skipped in 1076.83 s |
| `make check-docs-links` | 0 — 772 files, 4945 links clean |
| `make check-ledger-grammar` | 0 — 106 live ledgers clean (676 clauses) |
| `make check-lib-py` | 0 — 650 files clean (facade no-stub held) |
| `make verify` | 0 — fmt/clippy/panic-ban/file-size/conventions/docs/owner-ruling/parity-live dual-wire/rust tests all green |
| `git diff --cached \| grep -P '^\+\s*(//\|#(?! noqa))'` | clean — no added comments |

## Cost

The SWE-2 leg started 2026-09-11: read the contract, the DF-DESCRIBE-STR-1 ledger and the
S2-21 perf report, wrote the red-first scan pin, measured the base on the reviewer's
harness, shipped the SQL single-pass variant (red on wide — cast overhead), profiled the
cast/plan costs, re-based the aggregate on the native column API, elided casts whose
formatting is trivially reproducible, and landed all four shapes faster.

## Disk

The clone is a scratch lane; the harness and snapshot live under the gitignored
`scratch/perf-describe-1/` (removable at close). The lane venv carries a prebuilt native;
no Rust rebuild was needed or run.

## Dual-wire

Unchanged by this unit — a facade body, one new pin file, maps, this ledger. `.github/`
is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-describe-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The one-scan pin is red at base 25ca119a with statistics.py reverted — the base emits a five-leg UNION ALL with five TableScans and an ORDER BY ordinal, so no single aggregate plan exists to observe; green after the change.
      artifacts: [python/repark/tests/test_perf_describe_1.py, python/repark/src/repark/spark/dataframe/statistics.py]
    - id: AT-2
      status: ATTACKED
      evidence: Every asserted cell and ordering is the DF-DESCRIBE-STR-1 oracle contract, kept green unchanged — string try_cast mean/stddev, the non-describable skip/refusal, stat subsets, duplicate-stat order, display-name overlay; 35 describe/summary pins pass.
      artifacts: [python/repark/tests/test_perf_describe_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The comment fence runs on the staged diff and prints nothing; make check-lib-py holds statistics.py under its exact baseline.
      artifacts: [python/repark/src/repark/spark/dataframe/statistics.py]
    - id: AT-4
      status: ATTACKED
      evidence: The row order is carried by literal construction, not a sort — the returned frame's EXPLAIN has no SortExec/SortPreservingMergeExec and the duplicate-stat pin keeps requested slots.
      artifacts: [python/repark/src/repark/spark/dataframe/statistics.py, python/repark/tests/test_perf_describe_1.py]
    - id: AT-5
      status: N/A
      justification: No new execution surface, network, or cloud path; the same session and frame APIs are used one layer lower (native aggregate over the frame's own plan).
    - id: AT-6
      status: ATTACKED
      evidence: Describable-column selection and the PySparkValueError refusal are unchanged — target_pairs/kind_by_engine logic untouched; the zero-column AnalysisException still raises.
      artifacts: [python/repark/src/repark/spark/dataframe/statistics.py]
    - id: AT-7
      status: ATTACKED
      evidence: No guessed behavior — the cast-elision set is derived from measured Arrow display semantics (integer str() identical to CAST output; Utf8→Utf8 identity), float formatting divergence was measured and Python-side formatting rejected on that evidence.
      artifacts: [task/ledgers/staging/perf-describe-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: All gate commands ran on the shipped tree with recorded exit codes; no JVM was started and REPARK_PARITY_LIVE stayed unset.
      artifacts: [Makefile]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; errors surface through the existing exception classes.
    - id: AT-10
      status: ATTACKED
      evidence: Every touched directory's map.md updated in the same commit — tests map gains the pin file, dataframe map's statistics.py entry describes the single-pass shape, staging map lists this ledger.
      artifacts: [python/repark/tests/map.md, python/repark/src/repark/spark/dataframe/map.md, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Change: [../../../python/repark/src/repark/spark/dataframe/statistics.py](../../../python/repark/src/repark/spark/dataframe/statistics.py)
- Pins: [../../../python/repark/tests/test_perf_describe_1.py](../../../python/repark/tests/test_perf_describe_1.py)
- Motivation: `/tmp/oc-worker/grok-rev-descr/report.md` (S2-21 reviewer); oracle contract:
  [df-describe-str-1-ledger.md](df-describe-str-1-ledger.md)
