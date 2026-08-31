# Charter ledger — W-0 · window-shape measurement (Track A opener; no product change)

**Date:** 2026-08-31 · **Branch:** `feat/w-0-window-bench` · **Base:**
`60225cc427673cbc2e4bf23e90db376e602773dd` · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Verify before done" and
[../../../docs/testing.md](../../../docs/testing.md) · **Path:** STANDARD ·
**risk_tier:** standard · **Measure-only.**

**Charter:** intake Track A row W-0 in
[../../roadmap/mid-term/roadmap-intake-2026-08-23.md](../../roadmap/mid-term/roadmap-intake-2026-08-23.md)
(read-only). **Retires:** moved to `completed/` in this unit's departure commit.

**Closed homes:** `crates/**`, engine behaviour, `[patch.crates-io]`, `.github/`,
`briefs/next-sequence.md`, ledger `completed/` / archive. A bench crash is a
measured outcome (registry row), never a silent workaround.

## PROPOSITION LEDGER — W-0 — 2026-08-31

The probe roster below is the finite domain C-002 and C-009 quantify over.
Spark 4.1.2 built-in aggregates used as
`fn OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)`.
Excluded: `grouping` / `grouping_id` (not window aggregates) and registry §9
DECLARED-absent sketches (`hll_*`, theta, KLL, bitmap, `count_min_sketch`).

**Unary over numeric `v`:** `any`, `any_value`, `approx_count_distinct`,
`array_agg`, `avg`, `bit_and`, `bit_or`, `bit_xor`, `bool_and`, `bool_or`,
`collect_list`, `collect_set`, `count`, `count_if`, `every`, `first`,
`first_value`, `kurtosis`, `last`, `last_value`, `max`, `mean`, `median`,
`min`, `mode`, `skewness`, `some`, `std`, `stddev`, `stddev_pop`,
`stddev_samp`, `sum`, `try_avg`, `try_sum`, `var_pop`, `var_samp`, `variance`.

**With extra arguments:** `approx_percentile(v, 0.5)`, `corr(v, v2)`,
`covar_pop(v, v2)`, `covar_samp(v, v2)`, `max_by(v, id)`, `min_by(v, id)`,
`percentile(v, 0.5)`, `percentile_approx(v, 0.5)`, `regr_avgx(v, v2)`,
`regr_avgy(v, v2)`, `regr_count(v, v2)`, `regr_intercept(v, v2)`,
`regr_r2(v, v2)`, `regr_slope(v, v2)`, `regr_sxx(v, v2)`, `regr_sxy(v, v2)`,
`regr_syy(v, v2)`.

Retractable class the intake names: `sum`, `count`, `avg`, `min`, `max`,
`stddev`, `var` (and the Spark aliases `mean`, `std`, `stddev_pop`,
`stddev_samp`, `var_pop`, `var_samp`, `variance`). Everything else that
plans is the non-retractable class.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | One CLI entry point lives under `python/repark-parity/bench/windows/` with seeded data generation (no wall clock in the seed) and a machine-profile record (cpu, cores, governor, ram). | Import the driver without the native module; assert the generator is deterministic and the profile keys exist. | OPEN | Home not yet created. |
| C-002 | Sliding-frame cells exist for every probe-roster name, classified retract vs not vs absent vs refuse. | Parametrized pin over the roster; each name has exactly one class. | OPEN | Roster enumerated above; live class map not yet measured. |
| C-003 | A constant-frame cell runs `UNBOUNDED PRECEDING` / `UNBOUNDED FOLLOWING` (or `OVER ()`) on a retractable aggregate. | Smoke asserts the cell SQL and that the result record carries the constant-frame label. | OPEN | DataFusion already has this path; W-0 only measures it. |
| C-004 | An unpartitioned `ORDER BY` cell exists whose full-scale default is 10_000_000 rows. Gate scale is smaller and does not assert wall clock. | Constant `FULL_UNPARTITIONED_ROWS == 10_000_000`; smoke runs the cell at gate scale. | OPEN | 1e7 wall clock is a MEASUREMENT, not a clause. |
| C-005 | A `lead` / `lag` cell reads an unsorted Iceberg table (memory catalog, no sort order) and times both functions. | Smoke writes the table, runs `lead`/`lag` `OVER (ORDER BY ts)`, records plan-shape tokens including whether a sort ran. | OPEN | Oracles run the same SQL on in-memory tables; the Iceberg scan is the RePark shape. |
| C-006 | A window cell runs with a session `memory_limit` below the working set and records one outcome class: `ok`, `spill`, `oom`, `error`, or `crash`. The driver never retries a different query to hide the class. | Smoke with a 1 MiB floor and a working set above it; the recorded class is one of the five; an injected raise becomes `error`, not a rewritten query. | OPEN | How DataFusion fails is the number W-3 will document; W-0 only records it. |
| C-007 | Two oracle adapters exist: DuckDB at the workspace pin `duckdb==1.5.5`, and PySpark `4.1.2`. A missing extra or JVM is skip-loud and named in the results, never a silent omit. | Adapter constructors; DuckDB version pin in the bench requirements; PySpark skip records `oracle=pyspark reason=...`. | OPEN | Root `pyproject.toml` already pins DuckDB 1.5.5; PySpark is the parity `record` extra. |
| C-008 | A dated results document under `task/` carries engine version, DuckDB version, PySpark version-or-skip, machine profile, raw cell numbers, and generated-dataset byte sizes. | The result model requires those fields; the filed markdown exists and names them. | OPEN | Numbers are one host's wall clock; ratios are the claim, as P-2. |
| C-009 | Every probe-roster name whose live class is **refuse** (plans as a window aggregate, then DataFusion sliding-accumulator `not_impl`) is a registry BACKLOG row `WIN-SLIDE-<name>` with repark / Spark / pin / rationale. Absent names are not W-0 rows. | Gate test: the refuse set equals the `WIN-SLIDE-` headings; each pin asserts the sliding query raises. | OPEN | Enumeration is the roster above; the refuse subset is measured, not guessed. |
| C-010 | This unit does not modify `crates/`. An engine crash or loud refuse is recorded as an outcome (and C-009 row when it is a sliding refuse), never patched around. | `git diff` against base has no `crates/` path; the crash/error path has a pin. | OPEN | Product change is out of charter. |
| C-011 | Generated datasets land under the caller scratch directory and are deleted after the measurement run; the results document records the sizes taken before delete. `--keep-scratch` is the only opt-out. | Smoke writes, records `dataset_bytes`, deletes; a second listing of the scratch is empty. | OPEN | 1e7 and over-limit data must not remain in the clone. |

VERDICT: OPEN — 11 clauses, 0 PROVEN, 0 REJECTED. The gate passes when every row is
PROVEN with its pin (`pins: w-0-window-bench/C-NNN`).

```yaml
KILLED_ASSUMPTIONS:
  - "DuckDB must be added to the workspace": REMOVED (root pyproject.toml already pins duckdb==1.5.5 in the dev group; the bench restates that pin locally)
  - "make py-test exercises the native window operator": REMOVED (make py-test is the parity harness, no native module; engine-free pins live there, engine smoke lives with the facade suite and the measurement run)
CLARIFYING_QUESTIONS: []
```

## 1. Out of scope

- W-1 fallback `WindowExpr` (re-scan vs segment tree).
- W-2 Iceberg sort-order provenance / `BoundedWindowAggExec`.
- W-3 spill-coverage matrix guide (W-0 only records the over-limit class).
- W-U upstream issues (file them later, carrying these numbers).
- TA kernel numerics, window frame R4 (`EXCLUDE`).
- Any change under `crates/`.

## 2. Sequence

1. This charter (this commit).
2. Bench + engine-free pins (`make py-test`).
3. Sliding-roster probe against the native module; freeze the refuse set; registry rows.
4. Full-scale cells (1e7 unpartitioned, Iceberg lead/lag, over `memory_limit`); delete scratch; file the dated report.
5. Flip clauses PROVEN; gates.

## 3. Self Logic Review (charter)

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-W0-CHARTER
  agent: Actor
  action: file the W-0 charter ledger and staging map
  charter_trace: C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
  preconditions:
    - intake Track A W-0 row is the scope contract: SATISFIED (roadmap-intake-2026-08-23.md §3)
    - no crates/ edit in this commit: SATISFIED (ledger + staging map only)
    - DuckDB pin already in the workspace: SATISFIED (pyproject.toml dev group duckdb==1.5.5)
  success_condition: staging ledger has eleven C-NNN rows all OPEN, staging/map.md lists it, check-ledger-grammar exits 0
  step_risks:
    - quantifying "every refusing aggregate" without a finite roster: HANDLED (roster enumerated in this ledger)
    - engine-free CI cannot pin Iceberg/memory_limit: HANDLED (C-001/C-007 engine-free; C-005/C-006 smoke against the native module at measurement time)
  contingencies:
    - a bench crash on the 1e7 or over-limit cell: EXECUTABLE (additive — record outcome class and registry row; do not patch the engine)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```
