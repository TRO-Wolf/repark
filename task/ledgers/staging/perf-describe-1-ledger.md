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
`AggregateExec` plans computing count / avg / stddev / min / max per column — chunked at
50 columns per aggregate and cross-joined so each plan stays in the linear
expression-cost regime (D-1), then a lazy `mapInArrow` bridge unpivots the aggregate row into the
requested summary rows at action time — the ordinal wrapper and its sort node are gone
because order is carried by construction. The
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
| C-002 | The single-pass shape: one `AggregateExec` (per ≤50-column chunk, cross-joined) computes every requested statistic per column, and a lazy `mapInArrow` bridge unpivots the aggregate row into summary rows in the requested order; the DF-DESCRIBE-STR-1 ordinal wrapper is gone and the returned frame carries no UNION, no sort, no eager scan of the source — proven by EXPLAIN, not luck. | `test_describe_scans_the_source_once` plus the EXPLAIN counts in the C-002 note below. | **PROVEN** |
| C-003 | Every describe / summary pin stays green unchanged — string `mean`/`stddev` via `try_cast`, the non-describable skip/refusal, stat subsets, duplicate stats in requested order, display-name overlay on joined frames. | The `-k "describe or summary"` run and the full suite in the gates table; `test_summary_duplicate_stats_keep_requested_order`. | **PROVEN** |
| C-004 | After measurements on the same harness — the unit lands only if every measured shape is faster. | The C-004 measurement table below: all four shapes improved on medians. | **PROVEN** |
| C-005 | Laziness is a contract (DISPLAY-LAZY-1 / R-22): `describe()`/`summary()` build a plan and run nothing; a source that fails at scan surfaces the failure only at the action. | `test_describe_runs_nothing_until_an_action` — Arrow-export spy empty at call, `Cannot cast` raised only on `collect`; call-time splits in the C-006 table. | **PROVEN** |
| C-006 | The 500-column × 10k shape beats base after remediation — card D-2 holds on every measured shape. | The C-006 measurement table below: 35.19 → 21.50 s median. | **PROVEN** |
| C-007 | No `CAST` on `count` and no `CAST` on integer/string `min`/`max`; engine casts only where float formatting is load-bearing (`mean`/`stddev`, `min`/`max` on Float/Double/Decimal). | Cast inventory in the C-007 note below; `CAST(` count in the bridge parent's EXPLAIN equals the load-bearing set. | **PROVEN** |

`LOGIC_SCORE` = **7/7 `PROVEN`**.

## Remediation round (2026-09-12, ruling S2-21 re-review)

The read-only re-review (`/tmp/oc-worker/grok-rev-perfdesc/report.md`) confirmed the
round-1 numbers and filed three findings, all acted on:

- **P2-2 (contract — treated as P1).** Round 1 ran `to_arrow` inside `_summary`, so the
  call executed the aggregate — a DISPLAY-LAZY-1 violation. The shipped shape is a lazy
  `mapInArrow` bridge: the aggregate stays a plan node as the bridge's `parent`, and
  `_summary_unpivot` projects the five (stat-row × column) cells into the summary grid
  inside the bridge's Arrow callback at action time. Plan-side unpivot alternatives
  were measured and rejected: the grid needs ~5N leaf cell expressions, and this
  engine's per-plan expression cost is superlinear — a 2500-expression struct-grid +
  `unnest` over the 500-column aggregate cost ~95 s in probe, multi-column `UNNEST`
  fails on `OuterReferenceColumn`, `dynamic_flatten(explode_lists=True)` explodes
  lists independently (cross product, not zip), and chained projections carrying
  passthrough refs still time out. The bridge pays none of that: the callback
  stringifies already-string cells and raw integers directly.
- **P1-1.** The single 2500-expression aggregate with ~1000 in-plan `CAST`s regressed
  500×10k to 39.7 s. The aggregate is now chunked at 50 columns per `aggregate` call
  and chunk results are cross-joined on a literal-true condition — each chunk stays in
  the linear expression-cost regime. Joined-chunk collect measured 20.3 s in probe;
  end-to-end median 21.50 s vs base 35.19 s.
- **P2-3.** `count(...)` is Int64 for every column type and is never cast; `min`/`max`
  on integer and string columns pass raw. Only `mean`/`stddev` and `min`/`max` on
  Float/Double/Decimal keep engine `CAST(... AS Utf8)` — Arrow `pc.cast` and Python
  `repr` both diverge from the engine's float presentation (`1e+16`/`1`/`-0` vs
  `1e16`/`1.0`/`0.0`), measured and rejected.
- **P2-1 (moot).** The literal `VALUES` grid and its `CAST` projection are gone with
  the eager shape.

C-005 — laziness evidence (harness call/collect split, medians of 3):

| Shape | base call (s) | live call (s) | live collect (s) |
|---|---:|---:|---:|
| numeric 200k | 0.0297 | 0.0045 | 0.0730 |
| wide 50 × 10k | 0.3519 | 0.0271 | 0.8617 |
| wide 500 × 10k | 10.3315 | 0.6962 | 20.8098 |

The live call builds plans only — 2500 native expressions at 500 columns is ~0.7 s of
pure plan construction; nothing executes (the pin's export spy stays empty, and the
raises-on-scan source does not raise until `collect`).

C-006 — five-shape medians after remediation, base → live:

| Shape | base median (s) | live median (s) | Δ |
|---|---:|---:|---:|
| numeric 200k `k,v` | 0.120015 | 0.077727 | −35.2 % |
| wide 50 × 10k | 1.151518 | 0.893115 | −22.4 % |
| strings 200k | 0.189908 | 0.180398 | −5.0 % |
| mixed 200k | 0.191510 | 0.145438 | −24.1 % |
| wide 500 × 10k | 35.192226 | 21.499439 | −38.9 % |

C-007 — cast inventory on the 50-column all-integer shape: round 1 emitted 100 engine
`CAST`s (mean/stddev per column); the bridge emits the same 100 — `count`, integer
`min`/`max` and string `min`/`max` carry zero casts (150 of 250 aggregate expressions
are cast-free). On an all-integer frame the only `CAST(` occurrences in the parent's
EXPLAIN are `avg`/`stddev` wrappers.

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
by path so base and live run in one session on the same frames. Timed region is split
into `describe()` call time and `.collect()` time — the live call builds the plan only
(laziness evidence) while the collect is the load-bearing number (reviewer's own
framing). One warmup, three timed reps per impl per shape, medians, five shapes
including 500 columns × 10k. Plan counts are taken on the live result's
`_map_bridge["parent"]` — the bridge child itself owns a schema-only placeholder plan,
so the aggregate is inspected at its parent.

C-001 — before (base) medians and plan counts:

| Shape | Median (s) | logical TableScan | physical DataSourceExec | UnionExec |
|---|---:|---:|---:|---:|
| numeric 200k `k,v` | 0.131859 | 5 | 4 | 1 |
| wide 50 cols × 10k | 1.164347 | 5 | 4 | 1 |
| strings 200k `s,numeric_ish` | 0.194066 | 5 | 4 | 1 |
| mixed 200k `k,v,g,flag` | 0.196635 | 5 | 4 | 1 |

C-002 — after (live) plan shape: the result is a `mapInArrow` bridge whose `parent`
plan carries one `TableScan`, one `DataSourceExec`, and one `AggregateExec`
(mode=Single, all requested aggregates, the string columns' `avg`/`stddev` over
`try_cast`) on the ≤50-column pin frame; at 500 columns the parent is ten chunk
aggregates cross-joined (P1-1's sanctioned shape). No `UnionExec`, `SortExec`, or
`SortPreservingMergeExec` anywhere. The ordinal column and its `ORDER BY` are gone —
row order is emitted by the bridge's Python unpivot in requested order.

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

## Gates (2026-09-12 remediation tree; 2026-09-11 rows in git history)

| Command | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_perf_describe_1.py -q` | 0 — 3 passed |
| `.venv/bin/python -m pytest python/repark/tests -q -k "describe or summary"` | 0 — 36 passed, 6 skipped, 6266 deselected |
| `.venv/bin/python -m pytest python/repark/tests -q` | 0 — 5939 passed, 369 skipped in 803.46 s |
| `PYTHONPATH=python/repark-parity/src uv run --no-project python -m pytest python/repark-parity/tests -q -p no:cacheprovider` | 0 — 740 passed, 1 skipped, 11 xfailed in 132.48 s |
| `make check-docs-links` | 0 — 772 files, 4945 links clean |
| `make check-ledger-grammar` | 0 — 106 live ledgers clean (679 clauses, 1309 pinned clause ids) |
| `make check-lib-py` | 0 — 650 files clean (facade no-stub held) |
| `make verify` | 0 — fmt/clippy/panic-ban/file-size/conventions/docs/owner-ruling/parity-live dual-wire/ruff/rust tests all green |
| `git diff --cached \| grep -P '^\+\s*(//\|#(?! noqa))'` | clean — no added comments |

## Cost

The SWE-2 leg started 2026-09-11: read the contract, the DF-DESCRIBE-STR-1 ledger and the
S2-21 perf report, wrote the red-first scan pin, measured the base on the reviewer's
harness, shipped the SQL single-pass variant (red on wide — cast overhead), profiled the
cast/plan costs, re-based the aggregate on the native column API, elided casts whose
formatting is trivially reproducible, and landed all four shapes faster. The 2026-09-12
remediation round re-proved laziness after the S2-21 re-review: plan-side unpivot shapes
(struct-grid unnest, multi-column UNNEST, CASE grids, chained projections, per-chunk
flatten+join) were each measured and rejected on superlinear per-expression cost; the
shipped shape is chunked native aggregates cross-joined under the facade's lazy
mapInArrow bridge, which beats base on all five measured shapes including 500×10k.

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
      evidence: The row order is emitted by the bridge's Python unpivot in requested order, not a sort — the parent plan's EXPLAIN has no SortExec/SortPreservingMergeExec and the duplicate-stat pin keeps requested slots.
      artifacts: [python/repark/src/repark/spark/dataframe/statistics.py, python/repark/tests/test_perf_describe_1.py]
    - id: AT-5
      status: ATTACKED
      evidence: The bridge reuses the facade's existing lazy mapInArrow path — the parent aggregate is a normal plan node and every action path (collect/take/to_arrow/Arrow stream) already routes through the bridge; verified by the laziness pin and the 60-column two-chunk collect probe.
      artifacts: [python/repark/src/repark/spark/dataframe/statistics.py, python/repark/src/repark/spark/dataframe/core.py]
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
      evidence: Every touched directory's map.md updated in the same commit — tests map's pin entry describes the bridge shape and laziness pin, dataframe map's statistics.py entry describes the chunked aggregate + lazy bridge, parity tests map records the SQL-literal inventory row removal, staging map lists this ledger.
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
