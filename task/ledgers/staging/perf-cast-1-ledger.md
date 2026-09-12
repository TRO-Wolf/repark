# Unit ledger — PERF-CAST-1 · where a CAST costs a millisecond

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when PERF-CAST-1 merges, or when the
owner closes the slate row.

**Unit:** PERF-CAST-1 · **Date:** 2026-09-12 · **Model:** grok-4.6 · **Branch:** `perf/cast-1` · **Base:** `dae79c40` (branch point at dispatch; no merge performed — the orchestrator merges)
**Slate:** card PERF-CAST-1, `perf/cast-1` build lane, step 1 of 2 (measurement; no fix).

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `python/repark-parity/bench/cast/` (new),
`python/repark-parity/tests/cast/` (new), `docs/perf/cast-cost-2026-09-12.md`,
`docs/perf/cast-cost-2026-09-12.csv`, lockstep `map.md` files
(`python/repark-parity/bench/map.md`, `python/repark-parity/bench/cast/map.md`,
`python/repark-parity/tests/map.md`, `python/repark-parity/tests/cast/map.md`,
`docs/perf/map.md`, `task/ledgers/staging/map.md`), this ledger. Closed: `crates/`,
`scripts/` baselines, `.github/`, `STATUS.md`, `briefs/next-sequence.md`, every
other ledger, Cargo.toml / lockfiles.

## Scope

Step 1 of the card: measure where a CAST millisecond goes — SQL parse, logical
planning, each optimizer pass, physical expression creation, or per-batch
evaluation — on a 200k-row eager MemTable at 50 / 250 / 2500 CASTs and three
plan shapes (standalone projection, CAST over a wide aggregate, CAST projection
over that aggregate). No product change (D-1). D-2 is answered by measurement:
`datafusion.optimizer.max_passes` 0 / 1 / 3 do not move the wall, so this round
does not set a knob and does not fix; the superlinear phase is DataFusion
logical-plan construction of a wide CAST aggregate (draft issue in the perf
document; the orchestrator files it).

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The bench (`python/repark-parity/bench/cast/run_cast.py`, listed in the bench map.md) builds a 200k-row frame and times three plan shapes at 50 / 250 / 2500 casts — a standalone projection of casts, casts over a wide aggregate, and casts inside a projection over that aggregate — three repetitions after a warmup, medians, one CSV row per cell. | `docs/perf/cast-cost-2026-09-12.csv` (nine rows); `test_cast_cost_golden_csv_has_nine_cells`; the C-001 table in `docs/perf/cast-cost-2026-09-12.md`. | **PROVEN** |
| C-002 | The attribution table: for each shape, where the wall goes — SQL parse, logical planning, each optimizer pass (name the pass), physical expression creation, per-batch evaluation — from `EXPLAIN ANALYZE`, the session's plan-time probes, and a `py-spy`/`perf` profile of the 2500-cast cell. | C-002 tables in `docs/perf/cast-cost-2026-09-12.md`; `bench/cast/attribute.py`; py-spy note (perf refused). | **PROVEN** |
| C-003 | The superlinearity is located: which phase grows faster than linear in the cast count, with the exponent fit from the three sizes. | C-003 exponent table; over_aggregate plan exponent **1.535**; standalone and projection_over_aggregate ≤ 1. | **PROVEN** |
| C-004 | The cost is pinned as it stands: a test in `python/repark-parity/tests/` asserting the 2500-cast standalone projection under a budget 1.5× the measured median on this box, marked so a future fix or DataFusion bump that speeds it up flips a second, tighter pin (spill-golden shape). | `test_cast_2500_standalone_under_regression_budget` (budget 155.115 s); `test_cast_2500_over_aggregate_plan_under_linear_budget` (strict xfail, linear-from-50 plan 5.04 s vs measured 38.969 s). | **PROVEN** |

`LOGIC_SCORE` = **4/4 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

`test_cast_cost_golden_csv_has_nine_cells` on the base tree before the CSV
existed, exit 1:

```
E   AssertionError: CAST-cost golden is missing: /tmp/grok-cast/docs/perf/cast-cost-2026-09-12.csv
E   assert False
E    +  where False = is_file()
E    +    where is_file = PosixPath('/tmp/grok-cast/docs/perf/cast-cost-2026-09-12.csv').is_file

python/repark-parity/tests/cast/test_cast_cost_pin.py:39: AssertionError
FAILED python/repark-parity/tests/cast/test_cast_cost_pin.py::test_cast_cost_golden_csv_has_nine_cells
1 failed in 0.06s
```

The red is the missing golden: no nine-cell CSV, no budget, no flip pin.

## Measurements (no JVM)

Harness: `PYTHONPATH=python/repark-parity/bench .venv/bin/python python/repark-parity/bench/cast/run_cast.py`.
Native `__debug_assertions__ is True` (clone module, 659,878,056 B). Execute
instrument is `EXPLAIN ANALYZE` so a 2500-column projection is not copied into
Python. `+ i` on every CAST keeps expressions unique.

Named superlinear phase: **CAST-over-aggregate logical planning** (`over_aggregate`
`session.sql`), exponent **1.535**. `max_passes` 0 / 1 / 3 = 38.80 / 38.92 /
39.06 s at 2500 — not an optimizer pass. Same-width `SELECT 1 AS ci` is 0.55 s.
`EXPLAIN` of the same query is 111.7 s (physical expression creation + 387 KB
plan text). py-spy `--native` of `session.sql`: foldhash/hashbrown, TreeNode
walk, `TypeCoercionRewriter`; `perf record` refused (`perf_event_paranoid=4`).

## Gates

| Command | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark-parity/tests/cast/test_cast_cost_pin.py::test_cast_cost_golden_csv_has_nine_cells -q` (base, red-first) | 1 — missing CSV |
| `.venv/bin/python -m pytest python/repark-parity/tests/cast/test_cast_cost_pin.py::test_cast_cost_golden_csv_has_nine_cells python/repark-parity/tests/cast/test_cast_cost_pin.py::test_cast_2500_over_aggregate_plan_under_linear_budget -q` | 0 — 1 passed, 1 xfailed |
| `.venv/bin/python -m pytest python/repark-parity/tests/cast/test_cast_cost_pin.py::test_cast_2500_standalone_under_regression_budget -q` | 0 — 1 passed in 413.66 s |
| `make check-docs-links` | 0 — 787 files, 5062 links clean |
| `make check-map-sync` | 0 — 244 maps clean (strict=off) |
| `make check-ledger-grammar` | 0 — 111 live ledgers clean (709 clauses, 1339 pinned clause ids) |
| `make verify` | 0 — fmt/clippy/panic-ban/file-size/conventions/docs/owner-ruling/parity-live dual-wire/ruff/rust tests all green |
| `git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' \| grep -P '^\+\s*(//\|#(?! noqa))'` | clean — no added comments |

## Cost

Step 1 only: tracked bench, nine-cell CSV, attribution, py-spy of the 2500-cast
over_aggregate `session.sql`, two pins (regression budget + strict-xfail linear
flip), draft DataFusion issue in the perf document. No engine edit.

## Disk

`df -h /` at start: 503 G free. No worktree, no `cargo` rebuild. py-spy 0.4.2
installed into the clone `.venv` for the profile only (not a lockfile change).
Scratch under `/tmp/cast-cost-scratch/` (not committed).

## Dual-wire

Unchanged — measurement + pins + maps + this ledger. `.github/` is closed.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-cast-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The nine-cell golden pin is red on the base tree (CSV missing) and green after the committed CSV; the live 2500-cast standalone budget pin runs measure_one_cell against 1.5× the measured median.
      artifacts: [python/repark-parity/tests/cast/test_cast_cost_pin.py, docs/perf/cast-cost-2026-09-12.csv]
    - id: AT-2
      status: ATTACKED
      evidence: Three plan shapes × three CAST counts, value and plan/execute split, EXPLAIN ANALYZE elapsed_compute; no show-only path.
      artifacts: [python/repark-parity/bench/cast/measure.py, python/repark-parity/bench/cast/shapes.py]
    - id: AT-3
      status: ATTACKED
      evidence: No AWS, no JVM, no secrets, no .github. EXPLAIN ANALYZE discards output so a 2500-column projection is not materialized in Python.
      artifacts: [python/repark-parity/bench/cast/measure.py]
    - id: AT-4
      status: ATTACKED
      evidence: One session per grid run; attribution uses a fresh session per max_passes probe; py-spy of over_aggregate 2500 sql() is a separate process.
      artifacts: [python/repark-parity/bench/cast/attribute.py, docs/perf/cast-cost-2026-09-12.md]
    - id: AT-5
      status: N/A
      justification: No dependency, lockfile, or workflow change; py-spy was venv-local for the profile and is untracked.
    - id: AT-6
      status: ATTACKED
      evidence: No product code changed. The diff is a harness, pins, a perf document, its CSV, maps and this ledger.
      artifacts: [docs/perf/cast-cost-2026-09-12.md]
    - id: AT-7
      status: ATTACKED
      evidence: Superlinearity is the log-log exponent from three sizes, not wall folklore; max_passes 0/1/3 isolates the optimizer; the flip pin is strict xfail on the superlinear plan phase.
      artifacts: [python/repark-parity/tests/cast/test_cast_cost_pin.py, docs/perf/cast-cost-2026-09-12.md]
    - id: AT-8
      status: N/A
      justification: No dependency or lockfile change.
    - id: AT-9
      status: ATTACKED
      evidence: D-2 is answered without a product change; the draft DataFusion issue lives in the perf document for the orchestrator to file; the current cost is pinned so a bump that fixes it flips the linear pin.
      artifacts: [docs/perf/cast-cost-2026-09-12.md, python/repark-parity/tests/cast/test_cast_cost_pin.py]
    - id: AT-10
      status: ATTACKED
      evidence: Every touched directory's map.md moves in the same commit; the document, CSV, harness and pins point at each other.
      artifacts: [docs/perf/map.md, python/repark-parity/bench/map.md, python/repark-parity/bench/cast/map.md, python/repark-parity/tests/map.md, python/repark-parity/tests/cast/map.md, task/ledgers/staging/map.md]
  complete: true
```
